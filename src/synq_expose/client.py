"""Client side of the ticket stream protocols (tickets.py), for agents, tests and benchmarks.

SpiriSynq mirrors call the RPCs (``open_read``, ``open_write``, ...). These
helpers then move the bytes over the returned ticket with raw zenoh queries.

Stream queries must use ``ConsolidationMode.NONE``: every reply to a read
carries the same key, and zenoh's default consolidation would keep only one.

Byte routes reply with raw bytes. Small structured replies (write acks,
``status``) are JSON.
"""
import collections
import json
import queue
import secrets
import statistics
import threading
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor

import zenoh

from synq_expose.fs import CHUNK_SIZE

NO_CONSOLIDATION = zenoh.ConsolidationMode.NONE


class TicketError(RuntimeError):
    pass


class NoReply(TicketError):
    """Nothing answered in time. The request may or may not have landed."""


def _replies(z: zenoh.Session, selector: str, timeout: float, payload: bytes | None = None) -> Iterator[zenoh.Sample]:
    for reply in z.get(selector, consolidation=NO_CONSOLIDATION, timeout=timeout, payload=payload):
        if reply.err:
            raise TicketError(reply.err.payload.to_string())
        yield reply.ok


def _one(z: zenoh.Session, selector: str, timeout: float, payload: bytes | None = None) -> zenoh.Sample:
    for sample in _replies(z, selector, timeout, payload):
        return sample
    raise NoReply(f"no reply from {selector.split('?')[0]}")


def _range_params(start: int, end: int | None, chunk: int | None, readahead: str | None = None) -> str:
    return (f"start={start}" + ("" if end is None else f";end={end}") + ("" if chunk is None else f";chunk={chunk}")
            + ("" if readahead is None else f";readahead={readahead}"))


def read_chunks(z: zenoh.Session, ticket: str, start: int = 0, end: int | None = None,
                chunk: int | None = None, timeout: float = 30.0, readahead: str = "auto") -> Iterator[bytes]:
    """The slow path: one query for [start, end), answered with byte replies.

    For single chunks and small reads. Use ``stream`` for anything big.
    """
    replied = False
    for sample in _replies(z, f"{ticket}/read?{_range_params(start, end, chunk, readahead)}", timeout):
        replied = True
        if data := sample.payload.to_bytes():
            yield data
    if not replied:
        # A read always replies at least once, even past EOF.
        raise NoReply(f"no reply from {ticket}")


def _request(z: zenoh.Session, selector: str, payload: bytes | None, timeout: float,
             retries: int, backoff: float = 0.05, max_backoff: float = 1.0, abandoned=lambda: False) -> bytes:
    """One request that's safe to resend: retried on NoReply, never on an error reply.

    Waits ``backoff`` before the first resend, doubling each time up to
    ``max_backoff``. Gives up early, before a resend, once ``abandoned()``.
    """
    for attempt in range(retries + 1):
        try:
            return _one(z, selector, timeout, payload).payload.to_bytes()
        except NoReply:
            if attempt == retries or abandoned():
                raise
            time.sleep(min(backoff * 2 ** attempt, max_backoff))
    raise AssertionError("unreachable")


HEARTBEAT = "auto"
"""Heartbeat a third of the way through the ticket's ttl, as its first heartbeat reports it."""


STREAM_BUFFER = 4 << 20
"""Default bytes a stream queues for its caller. A buffer always takes one
sample, however big, so a chunk bigger than it still gets through."""


class Heartbeat:
    """Keeps a ticket alive while it's held: ``<t>/heartbeat`` every ``interval`` seconds, on a thread.

    With ``interval="auto"``, the first heartbeat goes straight away and its
    reply's ttl sets the interval to a third of it, so any server's ttl works.
    A failed heartbeat is ignored and tried again (after a second, until the
    ttl is known); if the ticket really is gone, the next real request says
    so. ``interval=None`` sends none.
    """

    def __init__(self, z: zenoh.Session, ticket: str, interval: float | str | None = HEARTBEAT):
        self._stop = threading.Event()
        self._thread = None
        if interval is not None:
            self._thread = threading.Thread(target=self._run, args=(z, f"{ticket}/heartbeat", interval),
                                            name="heartbeat", daemon=True)
            self._thread.start()

    def _run(self, z: zenoh.Session, selector: str, interval: float | str) -> None:
        wait = 0.0 if interval == "auto" else interval
        while not self._stop.wait(wait):
            try:
                reply = json.loads(_one(z, selector, 10.0).payload.to_bytes())
                if interval == "auto":
                    interval = wait = reply["ttl"] / 3
            except Exception:
                if interval == "auto":
                    wait = 1.0

    def stop(self) -> None:
        self._stop.set()

    def __enter__(self) -> "Heartbeat":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()


def close(z: zenoh.Session, ticket: str, *, timeout: float = 10.0, retries: int = 5) -> None:
    """Close a ticket (``<t>/close``). Repeating it is harmless."""
    _request(z, f"{ticket}/close", None, timeout, retries)


def _attachment(sample: zenoh.Sample) -> tuple[str, str]:
    kind, _, value = (sample.attachment.to_string() if sample.attachment else "").partition("=")
    return kind, value


def stream(z: zenoh.Session, ticket: str, start: int = 0, end: int | None = None, chunk: int | None = None,
           *, timeout: float = 10.0, retries: int = 5, backoff: float = 0.05, buffer: int = STREAM_BUFFER,
           readahead: str = "auto", heartbeat: float | str | None = HEARTBEAT, max_rate: int = 0) -> Iterator[bytes]:
    """Stream [start, end) of a read ticket, published to ``<ticket>/data/<id>``. Yields bytes in order.

    ``max_rate`` caps the stream at that many bytes per second, paced by
    the server (0: no limit). Restarts keep the cap.

    Heartbeats the ticket (see ``Heartbeat``) until the generator
    finishes or is closed, so a long stream outlives the ticket's ttl, and
    one whose client is gone doesn't. It doesn't close the ticket: that's
    up to whoever opened it.

    Subscribes first, then asks for the stream, so nothing published is
    missed. Every sample carries its offset. A gap, or nothing arriving for
    ``timeout`` seconds, stops that stream and starts a new one from the last
    offset received: the same "resend until it lands" as writes, at the
    granularity of a stream. ``retries`` counts failures in a row without
    progress. An ``err`` sample (the file changed, say) raises straight away.

    ``ticket`` is the key the ``stream`` route is under: a read ticket, or a
    process's ``<t>/stdout``. On process output, a stream with no ``end``
    runs until the process closes the stream, and while it's quiet the
    server sends ``at=`` samples so it doesn't look lost. Heartbeating
    ``<t>/stdout`` does nothing; ``Process`` heartbeats the ticket itself.

    There's no backpressure from the caller. zenoh's backpressure is per
    link, not per stream: a stream pausing it would stall everything else on
    the session. So up to ``buffer`` bytes are queued for the caller, and
    the callback never waits: past a full buffer it drops samples, and once the
    caller has drained the buffer, the stream restarts from there.

    The stream's rate adapts, as TCP's does. After an overrun it restarts
    at ``RATE_BACKOFF`` of the caller's pace (the median over its recent
    samples, so one pause doesn't count); after a gap, at half the rate.
    With no loss, it's raised ``RATE_RAISE``-fold every ``RATE_RAISE_EVERY``
    seconds through ``<t>/stream?rate``, without restarting, up to
    ``max_rate`` or, without one, until it's lifted. A caller that keeps up
    never meets a limit.
    """
    with Heartbeat(z, ticket, heartbeat):
        yield from _stream(z, ticket, start, end, chunk, timeout, retries, backoff, buffer, readahead, max_rate)


OVERRUN = object()
"""Queued in place of the first sample a full inbox drops."""


class _Overrun(NoReply):
    pass


class _Gap(NoReply):
    pass


RATE_BACKOFF = 0.9
"""After an overrun, the stream restarts at this share of the rate the caller was consuming at."""
RATE_RAISE, RATE_RAISE_EVERY = 1.5, 0.5
"""Without loss, the rate goes up this much every this many seconds..."""
RATE_UNLIMITED = 8 << 30
"""...until past this, when it's lifted (back to the caller's own max_rate, if it set one)."""
MIN_RATE = 64 << 10


class _Inbox:
    """One stream attempt's samples, at most ``buffer`` bytes of them (and always at least one).

    Each attempt gets its own: a sample still in flight to an old
    attempt's subscriber must never land in the next one's queue.
    """

    def __init__(self, buffer: int):
        self.buffer = buffer
        self._samples: queue.SimpleQueue = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._queued = 0
        self._overrun = False

    def deliver(self, sample: zenoh.Sample) -> None:
        # Runs on zenoh's receive thread, which everything on the link
        # shares: never wait here. Past a full buffer, drop the sample; the
        # consumer re-streams from where it got to once it has drained.
        size = len(sample.payload)
        with self._lock:
            if self._overrun:
                return
            if self._queued and self._queued + size > self.buffer:
                self._overrun = True
                self._samples.put((OVERRUN, 0))   # wakes the consumer as soon as it gets here
                return
            self._queued += size
        self._samples.put((sample, size))

    def get(self, timeout: float):
        """The next sample, ``OVERRUN``, or None if nothing came for ``timeout``."""
        try:
            sample, size = self._samples.get(timeout=timeout)
        except queue.Empty:
            return None
        with self._lock:
            self._queued -= size
        return sample


def _stream(z, ticket, start, end, chunk, timeout, retries, backoff, buffer, readahead, max_rate) -> Iterator[bytes]:
    position = start
    failures = 0
    rate = max_rate     # what the stream is asked to stay under now; 0: no limit
    consumed = _Consumption()
    while end is None or position < end:
        stream_id = secrets.token_hex(8)
        inbox = _Inbox(buffer)
        subscriber = z.declare_subscriber(f"{ticket}/data/{stream_id}", inbox.deliver)
        progressed = finished = False
        try:
            _request(z, f"{ticket}/stream?id={stream_id};{_range_params(position, end, chunk, readahead)}"
                     + (f";max_rate={int(rate)}" if rate else ""), None,
                     timeout, retries, backoff)
            raised_at = time.monotonic()
            while True:
                consumed.asking()
                item = inbox.get(timeout)
                consumed.got()
                if item is OVERRUN:
                    raise _Overrun(f"stream of {ticket} overran the buffer at offset {position}")
                if item is None:
                    raise NoReply(f"stream of {ticket} went quiet at offset {position}")
                sample = item
                kind, value = _attachment(sample)
                if kind == "err":
                    raise TicketError(value)
                if kind == "at":
                    # Live output with nothing new: the stream is still there.
                    if int(value) > position:
                        raise _Gap(f"gap in stream of {ticket}: {position} to {value}")
                    continue
                if kind == "end":
                    if int(value) == position:
                        finished = True
                        return
                    raise NoReply(f"stream of {ticket} ended at {value}, expected {position}")
                if kind != "o":
                    raise TicketError(f"unexpected sample on {ticket}: {kind!r}")
                offset = int(value)
                if offset < position:
                    continue        # already have it
                if offset > position:
                    raise _Gap(f"gap in stream of {ticket}: {position} to {offset}")
                data = sample.payload.to_bytes()
                position += len(data)
                progressed = True
                consumed.size = len(data)
                yield data
                if rate and rate != max_rate and time.monotonic() - raised_at >= RATE_RAISE_EVERY:
                    # No loss for a while: ask for more, without restarting.
                    rate = _raised(rate, max_rate)
                    raised_at = time.monotonic()
                    z.get(f"{ticket}/stream?rate;id={stream_id};max_rate={int(rate)}", lambda reply: None, timeout=1)
        except _Overrun:
            # The caller fell behind, nothing failed: restart straight away,
            # a little under the rate it was taking samples at.
            rate = _lowered(rate, consumed.rate(), max_rate)
        except NoReply as e:
            if isinstance(e, _Gap):
                # Lost samples: halve the rate, or start from the caller's pace if there was none.
                rate = _lowered(rate, rate / 2 / RATE_BACKOFF if rate else consumed.rate(), max_rate)
            failures = 0 if progressed else failures + 1
            if failures > retries:
                raise
            time.sleep(min(backoff * 2 ** failures, 1.0))
        finally:
            subscriber.undeclare()
            if not finished:
                try:
                    z.get(f"{ticket}/stream?stop;id={stream_id}", lambda reply: None, timeout=1)
                except Exception:
                    pass


class _Consumption:
    """How fast the caller takes samples: the median over its last few, so one long pause doesn't count."""

    def __init__(self, window: int = 32):
        self._recent: collections.deque = collections.deque(maxlen=window)
        self._got_at: float | None = None
        self.size = 0

    def asking(self) -> None:
        """The caller wants the next sample: it's done with the last one."""
        if self._got_at is not None and self.size:
            self._recent.append(self.size / max(time.monotonic() - self._got_at, 1e-9))
        self._got_at, self.size = None, 0

    def got(self) -> None:
        self._got_at = time.monotonic()

    def rate(self) -> float | None:
        """Bytes/s, or None before there's anything to go on."""
        return statistics.median(self._recent) if self._recent else None


def _lowered(rate: float, measured: float | None, cap: int) -> float:
    """The rate to restart at after losing samples. Unchanged if there's nothing to go on."""
    if measured is None:
        return rate
    new = max(measured * RATE_BACKOFF, MIN_RATE)
    return min(new, cap) if cap else new


def _raised(rate: float, cap: int) -> float:
    new = rate * RATE_RAISE
    if cap:
        return min(new, cap)
    return 0 if new > RATE_UNLIMITED else new


WRITE_WORKERS = 32
"""Threads sending writes, shared by every ``RemoteFile`` in the process."""

_write_pool: ThreadPoolExecutor | None = None
_write_pool_lock = threading.Lock()


def _shared_write_pool() -> ThreadPoolExecutor:
    """Started on first use; its threads are reused by every file after."""
    global _write_pool
    with _write_pool_lock:
        if _write_pool is None:
            _write_pool = ThreadPoolExecutor(max_workers=WRITE_WORKERS, thread_name_prefix="write")
        return _write_pool


class RemoteFile:
    """A write ticket as a file. ``write`` doesn't wait for acks; ``append`` does.

    Each ``write`` is a task on a pool shared by every file in the process
    (``WRITE_WORKERS`` threads) that sends its chunk and resends it until
    acked. Only ``max_in_flight`` of a file's writes run at once; past that ``write``
    waits for a slot, which bounds memory. A chunk that runs out of retries,
    or gets an error reply, fails the file: the next call raises it.

    ``flush`` and ``close`` wait for every write first, so returning from
    either means all of them landed. There's no cursor on the wire; ``offset``
    is always explicit. While it's open it heartbeats the ticket (see
    ``Heartbeat``), so it stays open however long it sits idle.
    """

    def __init__(self, z: zenoh.Session, ticket: str, size: int = 0, *, max_in_flight: int = 8,
                 retries: int = 5, timeout: float = 10.0, backoff: float = 0.05,
                 heartbeat: float | str | None = HEARTBEAT):
        self.z = z
        self.ticket = ticket
        self.size_at_open = size
        self.retries = retries
        self.timeout = timeout
        self.backoff = backoff
        self._slots = threading.BoundedSemaphore(max_in_flight)
        self._pool = _shared_write_pool()
        self._pending: set[Future] = set()
        self._lock = threading.Lock()
        self._error: BaseException | None = None
        self._seq = 0
        self._closed = False
        self._heartbeat = Heartbeat(z, ticket, heartbeat)
        self._abandoned = threading.Event()

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise self._error

    def _send(self, route: str, params: str, data: bytes | None = None) -> bytes:
        return _request(self.z, f"{self.ticket}/{route}{'?' + params if params else ''}", data,
                        self.timeout, self.retries, self.backoff, abandoned=self._abandoned.is_set)

    def write(self, offset: int, data: bytes) -> None:
        """Queue ``data`` for ``offset``. Returns once it's in flight, not once it's acked."""
        self._raise_if_failed()
        data = bytes(data)
        self._slots.acquire()
        self._raise_if_failed()

        def task():
            try:
                self._send("write", f"offset={offset}", data)
            except BaseException as e:
                with self._lock:
                    self._error = self._error or e
            finally:
                self._slots.release()

        future = self._pool.submit(task)
        with self._lock:
            self._pending.add(future)
        future.add_done_callback(self._forget)

    def _forget(self, future: Future) -> None:
        with self._lock:
            self._pending.discard(future)

    def append(self, data: bytes) -> None:
        """Append ``data`` and wait for the ack, so appends land in order."""
        self._raise_if_failed()
        self._send("append", f"seq={self._seq}", bytes(data))
        self._seq += 1

    def wait(self) -> None:
        """Block until every queued write is acked, or raise the first failure."""
        while True:
            with self._lock:
                pending = list(self._pending)
            if not pending:
                break
            for future in pending:
                future.result()
        self._raise_if_failed()

    def truncate(self, size: int) -> None:
        """Wait for every write, then cut the file to ``size`` bytes (or extend it with zeros).

        Waiting first keeps it in order, as for one Unix writer: a write
        still in flight can't land after it and lengthen the file again.
        """
        self.wait()
        self._send("truncate", f"size={size}")

    def flush(self) -> None:
        """Wait for every write, then fsync."""
        self.wait()
        self._send("flush", "")

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.wait()
            self._send("close", "")
        finally:
            self._closed = True
            self._abandon()

    def _abandon(self) -> None:
        """Stop sending, without waiting or closing the ticket. Unless something else holds it, it times out.

        Writes still queued are cancelled; ones already sending stop before their next resend.
        """
        self._abandoned.set()
        self._heartbeat.stop()
        with self._lock:
            pending = list(self._pending)
        for future in pending:
            future.cancel()

    def __del__(self) -> None:
        # Dropped without close: stop heartbeating so the ticket can time out.
        heartbeat = getattr(self, "_heartbeat", None)
        if heartbeat is not None:
            heartbeat.stop()

    def __enter__(self) -> "RemoteFile":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def write_all(z: zenoh.Session, ticket: str, data: bytes | Iterable[bytes], *, offset: int = 0,
              chunk_size: int = CHUNK_SIZE, close: bool = True, **kwargs) -> int:
    """Write ``data`` from ``offset`` with as many chunks in flight as allowed. Returns the bytes written."""
    if isinstance(data, (bytes, bytearray, memoryview)):
        view = memoryview(data)
        data = (view[i:i + chunk_size] for i in range(0, len(view), chunk_size))
    f = RemoteFile(z, ticket, **kwargs)
    written = 0
    try:
        for chunk in data:
            f.write(offset + written, chunk)
            written += len(chunk)
        f.close() if close else f.wait()
    except BaseException:
        f._abandon()
        raise
    return written


def call(z: zenoh.Session, selector: str, payload: bytes | None = None, timeout: float = 30.0) -> bytes:
    """One request on a ticket route, e.g. ``call(z, f"{t}/stdout?offset=0;wait=5")``. Returns the raw reply."""
    return _one(z, selector, timeout, payload).payload.to_bytes()


def call_json(z: zenoh.Session, selector: str, payload: bytes | None = None, timeout: float = 30.0):
    return json.loads(call(z, selector, payload, timeout))


class Process:
    """A process ticket, from ``Shell.spawn``: its output, stdin and status. Heartbeats the ticket while open.

    The same calls work for a plain command and an interactive one on a pty.
    ``close`` ends the process; a ``Process`` dropped without it stops
    heartbeating, and the server ends the process once the ticket idles out.
    """

    def __init__(self, z: zenoh.Session, spawned: dict | str, *, timeout: float = 10.0, retries: int = 5,
                 heartbeat: float | str | None = HEARTBEAT):
        self.z = z
        self.ticket = spawned["ticket"] if isinstance(spawned, dict) else spawned
        self.pty = spawned.get("pty", False) if isinstance(spawned, dict) else False
        self.timeout = timeout
        self.retries = retries
        self._seq = 0
        self._stdin_lock = threading.Lock()
        self._heartbeat = Heartbeat(z, self.ticket, heartbeat)

    def _send(self, route: str, data: bytes | None = None) -> bytes:
        return _request(self.z, f"{self.ticket}/{route}", data, self.timeout, self.retries)

    def output(self, name: str = "stdout", start: int = 0, end: int | None = None, **kwargs) -> Iterator[bytes]:
        """Stream ``name`` (stdout or stderr) from ``start``, until the process closes it or ``end``."""
        return stream(self.z, f"{self.ticket}/{name}", start, end, heartbeat=None, **kwargs)

    def read(self, name: str = "stdout", start: int = 0, wait: float = 0) -> bytes:
        """What's in ``name`` from ``start`` now, waiting up to ``wait`` seconds for something, like read(2)."""
        selector = f"{self.ticket}/{name}/read?start={start};wait={wait}"
        return b"".join(s.payload.to_bytes() for s in _replies(self.z, selector, wait + self.timeout))

    def write(self, data: bytes) -> None:
        """Write to stdin, in order, waiting for the ack. On a pty this is typing: ``b"\\x03"`` is Ctrl-C."""
        with self._stdin_lock:
            self._send(f"stdin/append?seq={self._seq}", bytes(data))
            self._seq += 1

    def close_stdin(self) -> None:
        self._send("stdin/close")

    def resize(self, rows: int, cols: int) -> None:
        self._send(f"resize?rows={rows};cols={cols}")

    def status(self, wait: float = 0) -> dict:
        return json.loads(_request(self.z, f"{self.ticket}/status" + (f"?wait={wait}" if wait else ""), None,
                                   self.timeout + wait, self.retries))

    def wait(self, timeout: float | None = None) -> int:
        """Block until the process exits; its exit code (negative for a signal)."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            left = 25.0 if deadline is None else min(25.0, deadline - time.monotonic())
            if left <= 0:
                raise TimeoutError("process still running")
            status = self.status(wait=left)
            if not status["running"]:
                return status["exit_code"]

    def kill(self, sig: str = "SIGTERM") -> None:
        self._send(f"kill?signal={sig}")

    def close(self) -> None:
        """End the process (if it's still running) and drop its output. Repeating it is harmless."""
        try:
            self._send("close")
        finally:
            self._heartbeat.stop()

    def __del__(self) -> None:
        heartbeat = getattr(self, "_heartbeat", None)
        if heartbeat is not None:
            heartbeat.stop()

    def __enter__(self) -> "Process":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
