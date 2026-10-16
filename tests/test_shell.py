"""Shell and process tickets. Unsandboxed unless marked, since bwrap may not be installed."""
import shutil
import time

import pytest
from SpiriSynq.remote_callables import RpcException

from conftest import stub
from synq_expose import client
from synq_expose.config import ExecConfig, SandboxConfig
from synq_expose.shell import Shell

needs_bwrap = pytest.mark.skipif(shutil.which("bwrap") is None, reason="bwrap not installed")


@pytest.fixture
def sh(share):
    return share(allow_execute=True)


def spawn(s, command, **kwargs) -> str:
    return s.mirror(Shell, "/shell").spawn(command, **kwargs)


def wait_exit(s, t, timeout=10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = client.call_json(s.z, f"{t}/status")
        if not status["running"]:
            return status
        time.sleep(0.02)
    raise TimeoutError("process still running")


def output(s, t, stream="stdout") -> bytes:
    """All of a finished process's output on ``stream``."""
    wait_exit(s, t)
    return client.call(s.z, f"{t}/{stream}?offset=0")


def test_shell_object_only_with_allow_execute(share):
    assert not share().exists("/shell")
    assert share(allow_execute=True).exists("/shell")


def test_shell_advertises_max_procs(share):
    s = share(allow_execute=True, exec=ExecConfig(sandbox=SandboxConfig(enabled=False), max_procs=2))
    assert s.mirror(Shell, "/shell").max_procs == 2


@stub
def test_argv_command(sh):
    t = spawn(sh, ["echo", "hi"])
    assert t.startswith("node/tickets/@")
    assert output(sh, t) == b"hi\n"
    assert wait_exit(sh, t)["exit_code"] == 0


@stub
def test_string_command_uses_shell(sh):
    t = spawn(sh, "echo $((1 + 2)) && exit 3")
    assert output(sh, t) == b"3\n"
    assert wait_exit(sh, t)["exit_code"] == 3


@stub
def test_stderr_is_separate(sh):
    t = spawn(sh, "echo out; echo err >&2")
    assert output(sh, t, "stdout") == b"out\n"
    assert output(sh, t, "stderr") == b"err\n"


@stub
def test_cwd_is_workspace(sh):
    assert output(sh, spawn(sh, ["pwd"])).decode().strip() == str(sh.workspace)


@stub
def test_stdin(sh):
    t = spawn(sh, ["cat"], stdin=True)
    client.call(sh.z, f"{t}/stdin", payload=b"hello\n")
    client.call(sh.z, f"{t}/stdin", payload=b"")   # EOF
    assert output(sh, t) == b"hello\n"


@stub
def test_stdin_closed_by_default(sh):
    t = spawn(sh, ["cat"])
    assert wait_exit(sh, t, timeout=5)["exit_code"] == 0


@stub
def test_output_by_offset(sh):
    t = spawn(sh, ["printf", "abcdef"])
    wait_exit(sh, t)
    assert client.call(sh.z, f"{t}/stdout?offset=2") == b"cdef"
    assert client.call(sh.z, f"{t}/stdout?offset=6") == b""


@stub
def test_long_poll_waits_for_output(sh):
    t = spawn(sh, "sleep 0.3; echo late")
    start = time.monotonic()
    assert client.call(sh.z, f"{t}/stdout?offset=0;wait=5") == b"late\n"
    assert 0.2 < time.monotonic() - start < 3


@stub
def test_long_poll_does_not_block_other_tickets(sh):
    slow = spawn(sh, "sleep 2; echo done")
    fast = spawn(sh, ["echo", "fast"])
    start = time.monotonic()
    poll = sh.z.get(f"{slow}/stdout?offset=0;wait=5", consolidation=client.NO_CONSOLIDATION, timeout=10)
    assert output(sh, fast) == b"fast\n"
    assert time.monotonic() - start < 1.5
    list(poll)


@stub
def test_kill(sh):
    t = spawn(sh, ["sleep", "30"])
    client.call(sh.z, f"{t}/kill?signal=SIGTERM")
    assert wait_exit(sh, t, timeout=5)["exit_code"] == -15


@stub
def test_timeout_kills(sh):
    t = spawn(sh, ["sleep", "30"], timeout=0.2)
    assert wait_exit(sh, t, timeout=5)["exit_code"] != 0


@stub
def test_max_procs(share):
    s = share(allow_execute=True, exec=ExecConfig(sandbox=SandboxConfig(enabled=False), max_procs=1))
    spawn(s, ["sleep", "30"])
    with pytest.raises(RpcException, match="max"):
        spawn(s, ["true"])


@stub
def test_output_survives_exit_until_discarded(sh):
    t = spawn(sh, ["echo", "kept"])
    wait_exit(sh, t)
    assert client.call(sh.z, f"{t}/stdout?offset=0") == b"kept\n"
    client.call(sh.z, f"{t}/discard")
    with pytest.raises(client.TicketError):
        client.call(sh.z, f"{t}/stdout?offset=0", timeout=0.5)


@stub
def test_env_is_minimal(share, monkeypatch):
    monkeypatch.setenv("EXPOSE_TEST_SECRET", "leak")
    monkeypatch.setenv("EXPOSE_TEST_PASSED", "ok")
    s = share(allow_execute=True, exec=ExecConfig(sandbox=SandboxConfig(enabled=False), pass_env=("EXPOSE_TEST_PASSED",)))
    env = output(s, spawn(s, ["env"])).decode()
    assert "EXPOSE_TEST_PASSED=ok" in env
    assert "EXPOSE_TEST_SECRET" not in env


# ── Sandbox ─────────────────────────────────────────────────────────────────

@needs_bwrap
@stub
def test_sandbox_workspace_read_only_without_write(share):
    s = share(allow_execute=True, sandbox=True)
    t = spawn(s, ["touch", "x"])
    assert wait_exit(s, t)["exit_code"] != 0
    assert not (s.workspace / "x").exists()


@needs_bwrap
@stub
def test_sandbox_hides_excluded(share):
    s = share(allow_execute=True, sandbox=True)
    (s.workspace / ".env").write_text("SECRET=1")
    assert b"SECRET" not in output(s, spawn(s, "cat .env 2>&1 || true"))


@needs_bwrap
@stub
def test_sandbox_has_no_network(share):
    s = share(allow_execute=True, sandbox=True)
    assert output(s, spawn(s, ["cat", "/sys/class/net/lo/operstate"])) != b""
    assert b"eth" not in output(s, spawn(s, ["ls", "/sys/class/net"]))


@needs_bwrap
@stub
def test_sandbox_cant_see_home(share):
    s = share(allow_execute=True, sandbox=True)
    t = spawn(s, "ls ~/.ssh")
    assert wait_exit(s, t)["exit_code"] != 0
