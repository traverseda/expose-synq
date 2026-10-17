"""``expose-synq run``: a remote command attached to local fds, on pipes or a pty."""
import os
import pty
import select
import termios
import threading

import pytest
from typer.testing import CliRunner

from synq_expose.cli import app
from synq_expose.run import exit_status, run_command
from synq_expose.shell import Shell


@pytest.fixture
def sh(share):
    return share(allow_execute=True)


def run_piped(s, argv, stdin: bytes = b"", forward_stdin=True) -> tuple[int, bytes, bytes]:
    """Run with pipes for stdin, stdout and stderr: (status, stdout, stderr)."""
    in_r, in_w = os.pipe()
    out_r, out_w = os.pipe()
    err_r, err_w = os.pipe()
    os.write(in_w, stdin)
    os.close(in_w)
    try:
        status = run_command(s.z, s.mirror(Shell, "/shell"), argv, tty=False, forward_stdin=forward_stdin,
                             stdin=in_r, stdout=out_w, stderr=err_w)
    finally:
        for fd in (in_r, out_w, err_w):
            os.close(fd)
    with os.fdopen(out_r, "rb") as out, os.fdopen(err_r, "rb") as err:
        return status, out.read(), err.read()


def test_exit_status_maps_signals():
    assert exit_status(3) == 3
    assert exit_status(-2) == 130


def test_piped(sh):
    assert run_piped(sh, ["cat"], b"hello\n") == (0, b"hello\n", b"")


def test_piped_stderr_and_status(sh):
    status, out, err = run_piped(sh, ["sh", "-c", "echo out\necho err >&2\nexit 7"])
    assert (status, out, err) == (7, b"out\n", b"err\n")


def test_piped_without_stdin(sh):
    assert run_piped(sh, ["cat"], b"ignored", forward_stdin=False) == (0, b"", b"")


def test_interactive_on_a_pty(sh):
    """A local pty stands in for the user's terminal: we type on its master end."""
    master, slave = pty.openpty()
    termios.tcsetattr(slave, termios.TCSANOW, termios.tcgetattr(slave))
    before = termios.tcgetattr(slave)
    result = {}

    def run():
        result["status"] = run_command(sh.z, sh.mirror(Shell, "/shell"), ["sh"], tty=True,
                                       stdin=slave, stdout=slave, stderr=slave)
    t = threading.Thread(target=run)
    t.start()
    os.write(master, b"echo $((6 * 7))\n")
    seen = b""
    while b"42\r\n" not in seen.replace(b"$((6 * 7))", b""):
        assert select.select([master], [], [], 10)[0], f"no output; got {seen!r}"
        seen += os.read(master, 4096)
    os.write(master, b"exit 3\n")
    t.join(10)
    assert not t.is_alive()
    assert result["status"] == 3
    assert termios.tcgetattr(slave) == before    # the terminal is back out of raw mode
    os.close(master)
    os.close(slave)


def test_cli_run_help():
    result = CliRunner().invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    assert "--tty" in result.output
