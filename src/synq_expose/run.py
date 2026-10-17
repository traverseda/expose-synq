"""``expose-synq run``: a command on a share, attached to this terminal, as ssh does it.

With a terminal on both ends the command gets a pty the size of this one, and
this terminal goes raw: every keystroke goes to the remote, Ctrl-C included,
and a resize here resizes the remote's. Otherwise it runs on pipes: stdin is
forwarded until EOF, stdout and stderr come back separately, and Ctrl-C here
sends SIGINT (a second one gives up). Either way it's the same process
ticket and the same calls (``client.Process``).
"""
import os
import signal
import sys
import termios
import threading
import tty as ttys

import zenoh

from synq_expose import client

LOST = 255
"""Exit status when the share stops answering, as ssh uses for a lost connection."""


def exit_status(code: int) -> int:
    """A remote exit code as a shell reports it: 128 + N for signal N."""
    return 128 - code if code < 0 else code


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def run_command(z: zenoh.Session, shell, argv: list[str], *, tty: bool, forward_stdin: bool = True,
                stdin: int = 0, stdout: int = 1, stderr: int = 2) -> int:
    """Run ``argv`` through the ``Shell`` mirror ``shell`` until it exits. Returns the exit status.

    ``tty`` asks for a pty, sized like ``stdout`` if that's a terminal.
    """
    if tty:
        try:
            cols, rows = os.get_terminal_size(stdout)
        except OSError:
            cols, rows = 80, 24
        spawned = shell.spawn(argv, pty={"rows": rows, "cols": cols,
                                         "term": os.environ.get("TERM", "xterm-256color")})
    else:
        spawned = shell.spawn(argv, stdin=forward_stdin)
    p = client.Process(z, spawned)
    lost: list[BaseException] = []
    done = threading.Event()

    def pump_out(name: str, fd: int) -> None:
        try:
            for piece in p.output(name):
                _write_all(fd, piece)
        except client.TicketError as e:
            if not done.is_set():
                lost.append(e)

    def pump_in() -> None:
        try:
            while data := os.read(stdin, 65536):
                p.write(data)
            p.close_stdin()
        except (client.TicketError, OSError):
            pass  # the process is gone, or stdin is

    outputs = [threading.Thread(target=pump_out, args=("stdout", stdout), daemon=True)]
    if not tty:
        outputs.append(threading.Thread(target=pump_out, args=("stderr", stderr), daemon=True))

    raw = tty and os.isatty(stdin)
    saved = termios.tcgetattr(stdin) if raw else None
    restore_winch = _forward_resizes(p, stdout) if tty else None
    try:
        if raw:
            ttys.setraw(stdin, termios.TCSANOW)   # not the default TCSAFLUSH: keep what was typed ahead
        for t in outputs:
            t.start()
        if tty or forward_stdin:
            threading.Thread(target=pump_in, daemon=True, name="stdin").start()
        interrupted = False
        while any(t.is_alive() for t in outputs):
            try:
                for t in outputs:
                    t.join(0.2)
            except KeyboardInterrupt:
                if interrupted:
                    raise
                interrupted = True
                p.kill("SIGINT")
        if lost:
            raise lost[0]
        return exit_status(p.wait())
    finally:
        done.set()
        if saved is not None:
            termios.tcsetattr(stdin, termios.TCSAFLUSH, saved)
        if restore_winch is not None:
            restore_winch()
        try:
            p.close()
        except client.TicketError:
            pass


def _forward_resizes(p: client.Process, fd: int):
    """Resize the remote pty whenever this terminal is resized. Returns a function that stops it.

    Only the main thread can catch SIGWINCH; elsewhere (tests) this does nothing.
    """
    if threading.current_thread() is not threading.main_thread():
        return None
    resized = threading.Event()
    stopped = threading.Event()

    def forward():
        while resized.wait() and not stopped.is_set():
            resized.clear()
            try:
                cols, rows = os.get_terminal_size(fd)
                p.resize(rows, cols)
            except (OSError, client.TicketError):
                pass

    previous = signal.signal(signal.SIGWINCH, lambda signum, frame: resized.set())
    threading.Thread(target=forward, daemon=True, name="resize").start()

    def restore():
        stopped.set()
        resized.set()
        signal.signal(signal.SIGWINCH, previous)
    return restore


def default_tty(stdin=sys.stdin, stdout=sys.stdout) -> bool:
    return stdin.isatty() and stdout.isatty()
