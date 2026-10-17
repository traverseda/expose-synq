"""Process execution. A process is a ticket (tickets.py) over its streams.

Its output is served like a file that's still being written (``ByteStream``,
fs.py), with a pipe's semantics: a read waits for data, and a stream with no
``end`` runs until the process closes that stream. Its stdin takes appends
like a write ticket's. A process on a pseudo-terminal (``pty``) is the same
ticket with one output stream, since the terminal carries stdout and stderr
both, plus ``resize``. Ctrl-C is the byte ``\\x03`` on stdin: the terminal's
line discipline turns it into SIGINT, as it would for a local terminal.

A process lives as long as its ticket: when the ticket is closed or idles
out, the process group is sent SIGHUP (on a pty) or SIGTERM, then SIGKILL.
A client holding a process heartbeats it, as for any ticket.

Anything allowed to run here can read whatever the sandbox can see.
"""
import fcntl
import os
import pty as ptys
import signal
import struct
import subprocess
import tempfile
import termios
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import zenoh
from loguru import logger
from SpiriSynq.remote_callables import remote_method
from SpiriSynq.syncable_objects import SyncableObject

from synq_expose.fs import ByteStream, _int_param, _json, _range
from synq_expose.sandbox import wrap_command

BASE_ENV = ("PATH", "HOME", "LANG")
"""Passed to every command, with ``--pass-env`` on top. Everything else is dropped."""

KEEPALIVE = 1.0
"""Seconds a stream at the end of live output waits before sending an ``at=`` sample to say it's still there."""

READ_WAIT, MAX_READ_WAIT = 10.0, 30.0
"""How long ``read`` waits for output by default, and at most."""

KILL_GRACE = 2.0
"""Seconds between the polite signal and SIGKILL when a process is ended for its client."""

PUMP_CHUNK = 64 << 10

_running_lock = threading.Lock()
"""Guards ``Shell.running``. Not a field: SpiriSynq would try to sync it."""


@dataclass
class Shell(SyncableObject):
    max_procs: int = 4
    running: int = 0
    sandboxed: bool = True
    """Whether commands run in bubblewrap. False is --no-sandbox: they run as the share's user, unconfined."""

    @remote_method()
    def spawn(
        self,
        command: list[str] | str,
        stdin: bool = False,
        pty: dict | bool | None = None,
        timeout: float | None = None,
    ) -> dict:
        """Start ``command`` with cwd at the workspace root, sandboxed unless --no-sandbox.

        A ``str`` goes through the configured shell. ``stdin=True`` keeps stdin
        open for ``stdin/append``, otherwise it's /dev/null. ``pty`` runs it on
        a pseudo-terminal: ``True``, or ``{"rows", "cols", "term"}`` (24, 80,
        xterm-256color); stdin is always open then. ``timeout`` kills it
        after that many seconds, within the server's ``--exec-timeout``.

        Returns ``{"ticket", "ttl", "pty"}``. See ``Process``.
        """
        self._guard.check("spawn", command=command, pty=bool(pty))
        cfg = self._config.exec
        if isinstance(command, str):
            argv = [cfg.shell, "-c", command]
        else:
            argv = list(command)
        if not argv or not all(isinstance(a, str) and a for a in argv):
            raise ValueError("command must be a non-empty string or list of non-empty strings")
        term = None
        if pty:
            spec = pty if isinstance(pty, dict) else {}
            term = (int(spec.get("rows", 24)), int(spec.get("cols", 80)), str(spec.get("term", "xterm-256color")))
        limits = [t for t in (timeout, cfg.timeout) if t]
        env = {k: os.environ[k] for k in (*BASE_ENV, *cfg.pass_env) if k in os.environ}
        argv = wrap_command(argv, workspace=self._policy.root, writable=self._config.allow_write,
                            hidden=[], config=cfg)  # TODO: hidden: the excluded paths, for the sandbox

        with _running_lock:
            if self.running >= self.max_procs:
                raise RuntimeError(f"already running the max of {self.max_procs} processes")
            self.running += 1
        try:
            process = Process(argv, cwd=self._policy.root, env=env, stdin=stdin, pty=term,
                              timeout=min(limits) if limits else None, output_cap=cfg.output_cap,
                              zenoh_session=self._tickets.session, on_exit=self._exited)
        except BaseException:
            self._exited()
            raise
        ticket = self._tickets.mint("process", process.routes(), on_revoke=process.end)
        process.bind(ticket)
        return {"ticket": ticket.key, "ttl": self._tickets.ttl, "pty": term is not None}

    def _exited(self) -> None:
        with _running_lock:
            self.running -= 1


class OutputDropped(RuntimeError):
    """The output asked for is older than what's kept."""


class Output:
    """A process's output on one stream: the last ``cap`` bytes, in a ring in an unlinked temp file.

    Offsets count every byte the stream has produced, so they stay stable as
    older output is dropped. One writer (the pump); reads and writes take
    the lock, so a read never sees bytes overwritten under it.
    """

    def __init__(self, cap: int):
        self.cap = cap
        self._file = tempfile.TemporaryFile(prefix="expose-synq-output-")
        self._fd = self._file.fileno()
        self.size = 0
        """Bytes produced so far: the offset just past the newest."""
        self.eof = False
        self._closed = False
        self._changed = threading.Condition()

    @property
    def kept_from(self) -> int:
        return max(0, self.size - self.cap)

    def append(self, data: bytes) -> None:
        with self._changed:
            if self._closed:
                return
            skip = max(0, len(data) - self.cap)   # more than the ring holds: only its tail is kept
            pos = (self.size + skip) % self.cap
            kept = data[skip:]
            first = kept[:self.cap - pos]
            os.pwrite(self._fd, first, pos)
            if len(first) < len(kept):
                os.pwrite(self._fd, kept[len(first):], 0)
            self.size += len(data)
            self._changed.notify_all()

    def finish(self) -> None:
        """The process closed this stream."""
        with self._changed:
            self.eof = True
            self._changed.notify_all()

    def read(self, offset: int, n: int) -> bytes:
        """Up to ``n`` bytes from ``offset``, whatever's there now: b"" at the end, whether or not that's EOF."""
        with self._changed:
            if self._closed:
                raise ValueError("output is closed")
            if offset < self.kept_from:
                raise OutputDropped(f"output before offset {self.kept_from} was dropped (only {self.cap} bytes are kept)")
            n = max(0, min(n, self.size - offset))
            pos = offset % self.cap
            data = os.pread(self._fd, min(n, self.cap - pos), pos)
            if len(data) < n:
                data += os.pread(self._fd, n - len(data), 0)
            return data

    def wait(self, offset: int, timeout: float, stopped: Callable[[], bool] = lambda: False) -> bool:
        """Wait up to ``timeout`` for output past ``offset``, or EOF. Whether either came."""
        deadline = time.monotonic() + timeout
        with self._changed:
            while not (self.size > offset or self.eof or self._closed):
                left = deadline - time.monotonic()
                if left <= 0 or stopped():
                    return False
                self._changed.wait(min(left, 0.1))
            return True

    def close(self) -> None:
        with self._changed:
            if not self._closed:
                self._closed = True
                self._file.close()
                self._changed.notify_all()


class OutputStream(ByteStream):
    """``read`` and ``stream`` over an ``Output``, with a pipe's semantics.

    A stream with no ``end`` runs until EOF, sending ``at=<offset>`` every
    ``KEEPALIVE`` seconds while there's nothing new. ``read`` waits up to
    ``wait`` seconds for output past ``start``, then replies with what's
    there, as read(2) on a pipe does. Each reply carries ``o=<offset>``; if
    there's nothing, one empty reply says ``end=<offset>`` (EOF) or
    ``at=<offset>`` (nothing yet).
    """

    def __init__(self, output: Output, name: str, zenoh_session: zenoh.Session | None = None):
        super().__init__(zenoh_session)
        self.output = output
        self.name = name

    def routes(self) -> dict:
        return {"read": self.read, "stream": self.stream}

    def _chunks(self, start, end, chunk, readahead="auto", stopped=lambda: False) -> Iterator[tuple[int, bytes | None]]:
        size = chunk if callable(chunk) else lambda: chunk
        offset = start
        while (end is None or offset < end) and not stopped():
            if not self.output.wait(offset, KEEPALIVE, stopped):
                if not stopped():
                    yield offset, None
                continue
            n = size()
            data = self.output.read(offset, n if end is None else min(n, end - offset))
            if not data:
                return  # EOF
            yield offset, data
            offset += len(data)

    def read(self, query: zenoh.Query) -> None:
        self._check_open()
        params = query.parameters
        start, end, chunk, _ = _range(params)
        wait = min(READ_WAIT if params.get("wait") is None else float(params.get("wait")), MAX_READ_WAIT)
        self.output.wait(start, wait, lambda: self._closed)
        offset = start
        while end is None or offset < end:
            data = self.output.read(offset, chunk if end is None else min(chunk, end - offset))
            if not data:
                break
            query.reply(query.key_expr, data, encoding=zenoh.Encoding.ZENOH_BYTES, attachment=f"o={offset}")
            offset += len(data)
        if offset == start:
            kind = "end" if self.output.eof and offset >= self.output.size else "at"
            query.reply(query.key_expr, b"", encoding=zenoh.Encoding.ZENOH_BYTES, attachment=f"{kind}={offset}")


class Process:
    """Process ticket. Routes under the ticket key ``@t``::

        @t/stdout/read   {start, end?, chunk?, wait?}   → output, as a read ticket's read with pipe semantics
        @t/stdout/stream {id, start, end?, chunk?, max_rate?} → {}; output published to @t/stdout/data/{id}
        @t/stdout/stream {stop, id} / {rate, id, max_rate}
        @t/stderr/...                                   → same, for stderr. Not there with a pty.
        @t/stdin/append  {seq} + data                   → written to stdin, as a write ticket's append
        @t/stdin/close                                  → EOF. On a pty, sends the terminal's EOF character.
        @t/resize        {rows, cols}                   → pty only: set the window size (the process gets SIGWINCH)
        @t/status        {wait?}                        → {running, exit_code, pty, stdout_size, stderr_size}.
                                                          With wait, waits up to that many seconds for the exit.
        @t/kill          {signal?}                      → signal the process group (default SIGTERM)
        @t/close                                        → end the process, drop its output, retire the ticket

    Output samples are tagged as for a read ticket (``o=``, ``end=``, ``err=``),
    plus ``at=<offset>`` while a stream is waiting on a process that's quiet.
    Each stream keeps its last ``output_cap`` bytes; asking for older output
    fails with ``OutputDropped``. ``exit_code`` is negative for a signal, as
    in ``subprocess``. Output stays readable after the process exits, until the ticket closes.
    """

    def __init__(self, argv: list[str], *, cwd, env: dict[str, str], stdin: bool,
                 pty: tuple[int, int, str] | None, timeout: float | None, output_cap: int,
                 zenoh_session: zenoh.Session | None, on_exit: Callable[[], None]):
        self.pty = pty is not None
        self._on_exit = on_exit
        self._lock = threading.Lock()
        self._exited = threading.Condition()
        self._next_seq = 0
        self._ended = False
        self.ticket = None
        self.outputs = {"stdout": Output(output_cap)}
        if self.pty:
            rows, cols, term = pty
            master, slave = ptys.openpty()
            try:
                _set_winsize(master, rows, cols)
                self.proc = subprocess.Popen(
                    argv, cwd=cwd, env={**env, "TERM": term}, stdin=slave, stdout=slave, stderr=slave,
                    start_new_session=True, preexec_fn=_take_controlling_tty,
                )
            except BaseException:
                os.close(master)
                raise
            finally:
                os.close(slave)
            self._stdin_fd = master
            sources = {"stdout": master}
        else:
            self.outputs["stderr"] = Output(output_cap)
            self.proc = subprocess.Popen(
                argv, cwd=cwd, env=env, stdin=subprocess.PIPE if stdin else subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
            )
            self._stdin_fd = self.proc.stdin.fileno() if stdin else None
            sources = {"stdout": self.proc.stdout.fileno(), "stderr": self.proc.stderr.fileno()}
        self.streams = {name: OutputStream(out, name, zenoh_session) for name, out in self.outputs.items()}
        self._pumps = [threading.Thread(target=self._pump, args=(fd, self.outputs[name]), daemon=True,
                                        name=f"pump-{self.proc.pid}-{name}") for name, fd in sources.items()]
        for pump in self._pumps:
            pump.start()
        self._timer = None
        if timeout:
            self._timer = threading.Timer(timeout, self._terminate)
            self._timer.daemon = True
            self._timer.start()
        threading.Thread(target=self._reap, daemon=True, name=f"reap-{self.proc.pid}").start()

    def bind(self, ticket) -> None:
        self.ticket = ticket
        for name, stream in self.streams.items():
            stream.ticket = ticket
            stream.prefix = f"{ticket.key}/{name}"

    def routes(self) -> dict:
        routes = {f"{name}/{route}": handler for name, stream in self.streams.items()
                  for route, handler in stream.routes().items()}
        routes |= {"stdin/append": self.append, "stdin/close": self.close_stdin, "status": self.status,
                   "kill": self.kill, "close": self.close_route}
        if self.pty:
            routes["resize"] = self.resize
        return routes

    # ── Running ─────────────────────────────────────────────────────────

    def _pump(self, fd: int, output: Output) -> None:
        try:
            while True:
                try:
                    data = os.read(fd, PUMP_CHUNK)
                except OSError:  # EIO: every process on the pty has closed it
                    break
                if not data:
                    break
                output.append(data)
        finally:
            output.finish()

    def _reap(self) -> None:
        self.proc.wait()
        if self._timer is not None:
            self._timer.cancel()
        with self._exited:
            self._exited.notify_all()
        self._on_exit()

    def _signal(self, sig: int) -> None:
        try:
            os.killpg(self.proc.pid, sig)
        except ProcessLookupError:
            pass  # the whole group has gone

    def _terminate(self) -> None:
        """The polite signal, then SIGKILL if it's still running after ``KILL_GRACE``."""
        if self.proc.poll() is not None:
            return
        self._signal(signal.SIGHUP if self.pty else signal.SIGTERM)
        try:
            self.proc.wait(KILL_GRACE)
        except subprocess.TimeoutExpired:
            self._signal(signal.SIGKILL)

    def end(self) -> None:
        """End the process and free its output. Called when the ticket is closed or revoked."""
        with self._lock:
            if self._ended:
                return
            self._ended = True
        for stream in self.streams.values():
            stream.close()
        # Off the caller's thread: the registry's sweep shouldn't wait out KILL_GRACE.
        threading.Thread(target=self._cleanup, daemon=True, name=f"end-{self.proc.pid}").start()

    def _cleanup(self) -> None:
        self._terminate()
        for pump in self._pumps:
            pump.join(KILL_GRACE)
        for output in self.outputs.values():
            output.close()
        for f in (self.proc.stdin, self.proc.stdout, self.proc.stderr):
            if f is not None:
                try:
                    f.close()
                except OSError:
                    pass
        if self.pty:
            os.close(self._stdin_fd)

    # ── Routes ──────────────────────────────────────────────────────────

    def _check_open(self) -> None:
        if self._ended:
            raise ValueError("process ticket is closed")

    def append(self, query: zenoh.Query) -> None:
        seq = _int_param(query.parameters, "seq")
        if seq is None:
            raise ValueError("append needs seq")
        data = query.payload.to_bytes() if query.payload is not None else b""
        with self._lock:
            self._check_open()
            if self._stdin_fd is None:
                raise ValueError("stdin isn't open: spawn with stdin=True")
            if seq > self._next_seq:
                raise ValueError(f"append seq {seq} is ahead, expected {self._next_seq}")
            if seq == self._next_seq:
                view = memoryview(data)
                while view:  # blocks while the pipe is full, as a Unix writer does
                    view = view[os.write(self._stdin_fd, view):]
                self._next_seq += 1
            # seq below the next one: a resend of an append that already landed
        _json(query, {})

    def close_stdin(self, query: zenoh.Query) -> None:
        with self._lock:
            self._check_open()
            if self.pty:
                os.write(self._stdin_fd, termios.tcgetattr(self._stdin_fd)[6][termios.VEOF])
            elif self.proc.stdin is not None and not self.proc.stdin.closed:
                self.proc.stdin.close()
                self._stdin_fd = None
        _json(query, {})

    def resize(self, query: zenoh.Query) -> None:
        rows, cols = _int_param(query.parameters, "rows"), _int_param(query.parameters, "cols")
        if not rows or not cols:
            raise ValueError("resize needs rows and cols")
        self._check_open()
        _set_winsize(self._stdin_fd, rows, cols)
        _json(query, {})

    def status(self, query: zenoh.Query) -> None:
        wait = min(float(query.parameters.get("wait") or 0), MAX_READ_WAIT)
        if wait:
            with self._exited:
                self._exited.wait_for(lambda: self.proc.returncode is not None or self._ended, wait)
        code = self.proc.poll()
        _json(query, {
            "running": code is None,
            "exit_code": code,
            "pty": self.pty,
            **{f"{name}_size": out.size for name, out in self.outputs.items()},
        })

    def kill(self, query: zenoh.Query) -> None:
        name = query.parameters.get("signal") or "SIGTERM"
        sig = int(name) if name.isdigit() else signal.Signals[name if name.startswith("SIG") else f"SIG{name}"]
        self._check_open()
        if self.proc.poll() is None:
            self._signal(sig)
        _json(query, {})

    def close_route(self, query: zenoh.Query) -> None:
        self.end()
        if self.ticket is not None:
            self.ticket.retire()
        _json(query, {})


def _set_winsize(fd: int, rows: int, cols: int) -> None:
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


def _take_controlling_tty() -> None:
    """In the child, after setsid: make the pty (now fd 0) its controlling terminal,
    so the line discipline signals it (Ctrl-C → SIGINT, resize → SIGWINCH)."""
    fcntl.ioctl(0, termios.TIOCSCTTY, 0)
