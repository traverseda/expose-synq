"""Shell and process tickets. Unsandboxed unless marked, since bwrap may not be installed."""
import json
import os
import shutil
import time

import pytest
from SpiriSynq.remote_callables import RpcException

from conftest import stub
from synq_expose import client
from synq_expose.config import ExecConfig, SandboxConfig
from synq_expose.shell import Shell

needs_bwrap = pytest.mark.skipif(shutil.which("bwrap") is None, reason="bwrap not installed")
UNSANDBOXED = SandboxConfig(enabled=False)


@pytest.fixture
def sh(share):
    return share(allow_execute=True)


def spawn(s, command, **kwargs) -> client.Process:
    return client.Process(s.z, s.mirror(Shell, "/shell").spawn(command, **kwargs))


def output(p: client.Process, stream="stdout") -> bytes:
    """All of a process's output on ``stream``, until it closes it."""
    return b"".join(p.output(stream))


def read_until(p: client.Process, needle: bytes, timeout=10.0) -> bytes:
    """Stream stdout until ``needle`` shows up; everything up to then."""
    got = b""
    deadline = time.monotonic() + timeout
    for piece in p.output(timeout=timeout):
        got += piece
        if needle in got or time.monotonic() > deadline:
            return got
    return got


def test_shell_object_only_with_allow_execute(share):
    assert not share().exists("/shell")
    assert share(allow_execute=True).exists("/shell")


def test_shell_advertises_max_procs(share):
    s = share(allow_execute=True, exec=ExecConfig(sandbox=UNSANDBOXED, max_procs=2))
    assert s.mirror(Shell, "/shell").max_procs == 2


# ── Plain commands ──────────────────────────────────────────────────────────

def test_argv_command(sh):
    p = spawn(sh, ["echo", "hi"])
    assert p.ticket.startswith("node/tickets/@")
    assert output(p) == b"hi\n"
    assert p.wait(10) == 0


def test_string_command_uses_shell(sh):
    p = spawn(sh, "echo $((1 + 2)) && exit 3")
    assert output(p) == b"3\n"
    assert p.wait(10) == 3


@pytest.mark.xfail(reason="SpiriSynq doesn't escape ';' in RPC arguments (known upstream)", strict=True)
def test_semicolon_in_command(sh):
    assert output(spawn(sh, "echo a; echo b")) == b"a\nb\n"


def test_empty_command_is_refused(sh):
    with pytest.raises(RpcException):
        sh.mirror(Shell, "/shell").spawn([])


def test_stderr_is_separate(sh):
    p = spawn(sh, "echo out\necho err >&2")
    assert output(p, "stdout") == b"out\n"
    assert output(p, "stderr") == b"err\n"


def test_cwd_is_workspace(sh):
    assert output(spawn(sh, ["pwd"])).decode().strip() == str(sh.workspace)


def test_stdin(sh):
    p = spawn(sh, ["cat"], stdin=True)
    p.write(b"hello ")
    p.write(b"world\n")
    p.close_stdin()
    assert output(p) == b"hello world\n"


def test_stdin_resend_is_not_written_twice(sh):
    p = spawn(sh, ["cat"], stdin=True)
    client.call(sh.z, f"{p.ticket}/stdin/append?seq=0", payload=b"once\n")
    client.call(sh.z, f"{p.ticket}/stdin/append?seq=0", payload=b"once\n")
    with pytest.raises(client.TicketError, match="ahead"):
        client.call(sh.z, f"{p.ticket}/stdin/append?seq=5", payload=b"x")
    client.call(sh.z, f"{p.ticket}/stdin/close")
    assert output(p) == b"once\n"


def test_stdin_closed_by_default(sh):
    p = spawn(sh, ["cat"])
    assert p.wait(5) == 0
    with pytest.raises(client.TicketError, match="stdin"):
        p.write(b"x")


def test_read_by_offset(sh):
    p = spawn(sh, ["printf", "abcdef"])
    p.wait(10)
    assert p.read(start=2) == b"cdef"
    assert p.read(start=6) == b""
    assert json.loads(client.call(sh.z, f"{p.ticket}/status"))["stdout_size"] == 6


def test_read_says_eof_or_not_yet(sh):
    def attachment(t, start):
        sample = client._one(sh.z, f"{t}/stdout/read?start={start};wait=0", 10)
        return sample.attachment.to_string()

    quiet = spawn(sh, ["sleep", "30"])
    assert attachment(quiet.ticket, 0) == "at=0"
    done = spawn(sh, ["printf", "ab"])
    done.wait(10)
    output(done)        # until EOF has been seen
    assert attachment(done.ticket, 0) == "o=0"
    assert attachment(done.ticket, 2) == "end=2"


def test_read_waits_for_output(sh):
    p = spawn(sh, "sleep 0.3\necho late")
    start = time.monotonic()
    assert p.read(wait=5) == b"late\n"
    assert 0.2 < time.monotonic() - start < 3


def test_waiting_read_does_not_block_other_tickets(sh):
    slow = spawn(sh, "sleep 2\necho done")
    fast = spawn(sh, ["echo", "fast"])
    start = time.monotonic()
    poll = sh.z.get(f"{slow.ticket}/stdout/read?start=0;wait=5", consolidation=client.NO_CONSOLIDATION, timeout=10)
    assert output(fast) == b"fast\n"
    assert time.monotonic() - start < 1.5
    list(poll)


def test_stream_outlasts_a_quiet_process(sh):
    # Quieter than the client's timeout, with no retries: only keepalives keep it going.
    p = spawn(sh, "echo a\nsleep 2.5\necho b")
    assert b"".join(p.output(timeout=1.5, retries=0)) == b"a\nb\n"


def test_stream_from_an_offset(sh):
    p = spawn(sh, ["printf", "0123456789"])
    assert b"".join(p.output(start=4)) == b"456789"
    assert b"".join(p.output(start=2, end=5)) == b"234"


def test_large_output(sh):
    p = spawn(sh, "head -c 3000000 /dev/urandom | od -An -v | head -c 5000000")
    data = output(p)
    assert len(data) == 5_000_000
    assert p.wait(10) == 0


def test_old_output_is_dropped_past_the_cap(share):
    s = share(allow_execute=True, exec=ExecConfig(sandbox=UNSANDBOXED, output_cap=4096))
    p = spawn(s, "head -c 10000 /dev/zero\nprintf end")
    p.wait(10)
    with pytest.raises(client.TicketError, match="dropped"):
        p.read(start=0)
    assert p.read(start=10000) == b"end"
    assert b"".join(p.output(start=10003 - 4096)).endswith(b"\0end")


def test_kill(sh):
    p = spawn(sh, ["sleep", "30"])
    p.kill("SIGTERM")
    assert p.wait(5) == -15


def test_kill_reaches_the_process_group(sh):
    p = spawn(sh, "sleep 30 & echo $!\nwait")
    child = int(read_until(p, b"\n"))
    p.kill("SIGKILL")
    p.wait(5)
    assert eventually_gone(child)


def test_timeout_kills(sh):
    p = spawn(sh, ["sleep", "30"], timeout=0.2)
    assert p.wait(5) != 0


def test_server_timeout_caps_the_callers(share):
    s = share(allow_execute=True, exec=ExecConfig(sandbox=UNSANDBOXED, timeout=0.2))
    p = spawn(s, ["sleep", "30"], timeout=60)
    assert p.wait(5) != 0


def test_max_procs(share):
    s = share(allow_execute=True, exec=ExecConfig(sandbox=UNSANDBOXED, max_procs=1))
    p = spawn(s, ["sleep", "30"])
    with pytest.raises(RpcException, match="max"):
        spawn(s, ["true"])
    p.kill()
    p.wait(5)
    time.sleep(0.1)
    assert output(spawn(s, ["echo", "ok"])) == b"ok\n"


def test_output_survives_exit_until_closed(sh):
    p = spawn(sh, ["echo", "kept"])
    p.wait(10)
    assert p.read() == b"kept\n"
    p.close()
    p.close()   # repeatable
    with pytest.raises(client.TicketError):
        client.call(sh.z, f"{p.ticket}/stdout/read?start=0", timeout=0.5)


def test_close_ends_a_running_process(sh):
    p = spawn(sh, "echo $$\nexec sleep 30")
    pid = int(read_until(p, b"\n"))
    p.close()
    assert eventually_gone(pid)


def test_process_ends_when_its_ticket_idles_out(share):
    s = share(allow_execute=True, ttl=0.3)
    p = spawn(s, "echo $$\nexec sleep 30")
    pid = int(read_until(p, b"\n"))
    p._heartbeat.stop()
    time.sleep(0.5)
    s.tickets.sweep()
    assert eventually_gone(pid)


def test_heartbeat_keeps_a_process_alive(share):
    s = share(allow_execute=True, ttl=0.3)
    p = spawn(s, "echo $$\nexec sleep 30")
    pid = int(read_until(p, b"\n"))
    for _ in range(5):
        time.sleep(0.2)
        s.tickets.sweep()
    assert p.status()["running"]
    os.kill(pid, 0)
    p.close()


def test_env_is_minimal(share, monkeypatch):
    monkeypatch.setenv("EXPOSE_TEST_SECRET", "leak")
    monkeypatch.setenv("EXPOSE_TEST_PASSED", "ok")
    s = share(allow_execute=True, exec=ExecConfig(sandbox=UNSANDBOXED, pass_env=("EXPOSE_TEST_PASSED",)))
    env = output(spawn(s, ["env"])).decode()
    assert "EXPOSE_TEST_PASSED=ok" in env
    assert "EXPOSE_TEST_SECRET" not in env
    assert "PATH=" in env


# ── On a pty: the same ticket, interactively ────────────────────────────────

def test_pty_has_one_output(sh):
    p = spawn(sh, "echo out\necho err >&2", pty=True)
    assert p.pty
    assert output(p) == b"out\r\nerr\r\n"
    with pytest.raises(client.TicketError):
        client.call(sh.z, f"{p.ticket}/stderr/read?start=0", timeout=0.5)


def test_pty_is_a_terminal_of_the_size_asked_for(sh):
    p = spawn(sh, "test -t 0 && test -t 1 && echo $TERM && stty size", pty={"rows": 30, "cols": 100, "term": "dumb"})
    assert output(p) == b"dumb\r\n30 100\r\n"
    assert p.wait(10) == 0


def test_interactive_shell(sh):
    p = spawn(sh, ["sh"], pty=True)
    p.write(b"echo $((6 * 7))\n")
    assert b"42\r\n" in read_until(p, b"42\r\n")
    p.write(b"exit 5\n")
    assert p.wait(10) == 5


def test_resize(sh):
    p = spawn(sh, ["sh"], pty=True)
    p.resize(40, 120)
    p.write(b"stty size\n")
    assert b"40 120" in read_until(p, b"40 120")
    p.close()


def test_ctrl_c_interrupts(sh):
    p = spawn(sh, ["sleep", "30"], pty=True)
    time.sleep(0.2)
    p.write(b"\x03")
    assert p.wait(5) == -2


def test_pty_stdin_close_sends_eof(sh):
    p = spawn(sh, ["cat"], pty=True)
    p.write(b"hi\n")
    p.close_stdin()
    assert p.wait(5) == 0
    assert output(p).count(b"hi\r\n") == 2   # the terminal's echo, then cat's


def test_resize_needs_a_pty(sh):
    p = spawn(sh, ["sleep", "30"])
    with pytest.raises(client.TicketError):
        p.resize(10, 10)
    p.close()


def test_close_hangs_up_a_pty(sh):
    p = spawn(sh, "echo $$\nexec sleep 30", pty=True)
    pid = int(read_until(p, b"\n"))
    p.close()
    assert eventually_gone(pid)


def eventually_gone(pid: int, timeout=5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with open(f"/proc/{pid}/stat") as f:
                if f.read().split(") ", 1)[1].startswith("Z"):   # dead, not yet reaped
                    return True
        except (FileNotFoundError, ProcessLookupError):
            return True
        time.sleep(0.05)
    return False


# ── Sandbox ─────────────────────────────────────────────────────────────────

@needs_bwrap
@stub
def test_sandbox_workspace_read_only_without_write(share):
    s = share(allow_execute=True, sandbox=True)
    p = spawn(s, ["touch", "x"])
    assert p.wait(10) != 0
    assert not (s.workspace / "x").exists()


@needs_bwrap
@stub
def test_sandbox_hides_excluded(share):
    s = share(allow_execute=True, sandbox=True)
    (s.workspace / ".env").write_text("SECRET=1")
    assert b"SECRET" not in output(spawn(s, "cat .env 2>&1 || true"))


@needs_bwrap
@stub
def test_sandbox_has_no_network(share):
    s = share(allow_execute=True, sandbox=True)
    assert output(spawn(s, ["cat", "/sys/class/net/lo/operstate"])) != b""
    assert b"eth" not in output(spawn(s, ["ls", "/sys/class/net"]))


@needs_bwrap
@stub
def test_sandbox_cant_see_home(share):
    s = share(allow_execute=True, sandbox=True)
    p = spawn(s, "ls ~/.ssh")
    assert p.wait(10) != 0


@needs_bwrap
@stub
def test_sandboxed_pty(share):
    s = share(allow_execute=True, sandbox=True)
    p = spawn(s, ["sleep", "30"], pty=True)
    time.sleep(0.2)
    p.write(b"\x03")
    assert p.wait(5) == -2
