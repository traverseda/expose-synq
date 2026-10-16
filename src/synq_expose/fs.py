"""Filesystem access, split into read and write objects.

The RPCs here only carry small values. File contents always move over
tickets (tickets.py): ``open_read`` / ``open_write`` return one.
Every method takes a workspace-relative ``path`` first.
"""
import hashlib
import json
import os
import queue
import re
import shutil
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from stat import S_IMODE, S_ISDIR, S_ISLNK, S_ISREG

import zenoh
from loguru import logger
from SpiriSynq.remote_callables import remote_method
from SpiriSynq.syncable_objects import SyncableObject

from synq_expose.policy import PathPolicy, PolicyError

CHUNK_SIZE = 1 << 20


def file_digest(path: Path, algorithm: str) -> str:
    with path.open("rb") as f:
        return f"{algorithm}:{hashlib.file_digest(f, algorithm).hexdigest()}"



@dataclass
class FsRead(SyncableObject):
    @remote_method()
    def list_dir(self, path: str = ".") -> Iterator[dict]:
        """Stream the entries in ``path``, by name: ``stat`` plus ``name``. Excluded paths are omitted."""
        self._guard.check("list_dir", path=path)
        for p, st in _entries(self._policy, self._policy.resolve(path)):
            yield {"name": p.name, **_stat(st)}

    @remote_method()
    def stat(self, path: str) -> dict:
        """lstat: ``{"type", "size", "mtime", "mode"}``. ``type`` is file, dir, symlink or other; ``mode`` the permission bits."""
        self._guard.check("stat", path=path)
        return _stat(self._policy.resolve(path, nofollow=True).lstat())

    @remote_method()
    def hash(self, path: str, algorithm: str = "sha256") -> str:
        """``"<algorithm>:<hexdigest>"``, so a client can skip re-reading unchanged files."""
        self._guard.check("hash", path=path)
        return file_digest(self._policy.resolve(path), algorithm)

    @remote_method()
    def glob(self, path: str, pattern: str) -> Iterator[str]:
        """Stream the workspace-relative paths under ``path`` that match ``pattern``.

        Depth first, each directory's entries by name, a directory before
        what's in it.

        ``pattern`` is relative to ``path``. ``*`` matches within a segment and
        ``**`` any number of segments, as in ``pathlib``; dotfiles match like
        any other name. Directories match too. Symlinks aren't followed, and
        excluded paths are skipped.
        """
        self._guard.check("glob", path=path, pattern=pattern)
        top = self._policy.resolve(path)
        depth = None if "**" in pattern else pattern.count("/") + 1
        for p, _ in _walk(self._policy, top, depth):
            if PurePosixPath(p.relative_to(top).as_posix()).full_match(pattern):
                yield p.relative_to(self._policy.root).as_posix()

    @remote_method()
    def grep(self, path: str, pattern: str) -> Iterator[dict]:
        """Stream ``{path, line, text}`` for each line matching the regex ``pattern``.

        ``path`` is a file or a directory to search under. Like grep: lines
        are numbered from 1, files with a NUL byte in their first 8 KiB are
        binary and skipped, symlinks aren't followed. Excluded paths are
        skipped. ``text`` is the line without its newline, decoded as UTF-8
        with undecodable bytes replaced.
        """
        self._guard.check("grep", path=path, pattern=pattern)
        regex = re.compile(pattern.encode())
        top = self._policy.resolve(path)
        found = [(top, top.stat())] if top.is_file() else _walk(self._policy, top)
        for p, st in found:
            if not S_ISREG(st.st_mode):
                continue
            try:
                f = p.open("rb")
            except OSError:
                continue
            with f:
                if b"\0" in f.read(8192):
                    continue
                f.seek(0)
                rel = p.relative_to(self._policy.root).as_posix()
                for n, line in enumerate(f, 1):
                    if regex.search(line):
                        yield {"path": rel, "line": n, "text": line.rstrip(b"\n").decode(errors="replace")}

    @remote_method()
    def open_read(self, path: str) -> dict:
        """Mint a read ticket for ``path``. Returns ``{"ticket", "size", "mtime"}``. See ``FileReadStream``.

        Costs one stat, whatever the file's size. For a digest, call ``hash``.
        """
        self._guard.check("open_read", path=path)
        target = self._policy.resolve(path)
        if not target.is_file():
            raise FileNotFoundError(path)
        st = target.stat()
        stream = FileReadStream(target, st, self._tickets.session)
        ticket = self._tickets.mint("read", stream.routes(), on_revoke=stream.close)
        stream.ticket = ticket
        return {"ticket": ticket.key, "size": st.st_size, "mtime": st.st_mtime, "ttl": self._tickets.ttl}


@dataclass
class FsWrite(SyncableObject):
    @remote_method()
    def open_write(self, path: str, truncate: bool, exclusive: bool = False) -> dict:
        """Open ``path`` for writing, creating it if missing, and mint a ticket for it.

        ``truncate`` empties an existing file first; there's no default, since
        either one can wreck a file. ``exclusive`` (O_EXCL) fails if ``path``
        already exists, so a temp file can't land on someone else's.
        Returns ``{"ticket", "size", "ttl"}``, the size after opening. See
        ``FileWriteStream``.
        """
        self._guard.check("open_write", path=path, truncate=truncate, exclusive=exclusive)
        target = self._policy.resolve(path, write=True)
        stream = FileWriteStream(target, truncate, exclusive)
        ticket = self._tickets.mint("write", stream.routes(), on_revoke=stream.close)
        stream.ticket = ticket
        return {"ticket": ticket.key, "size": stream.size_at_open, "ttl": self._tickets.ttl}

    @remote_method()
    def edit_file(self, path: str, old: str, new: str) -> None:
        """Replace exactly one occurrence of ``old`` with ``new``, as ``sed -i`` does.

        The edited file is written next to the original with its mode, then
        renamed over it. Readers with the file open keep the old contents
        (and get ``FileChangedError`` on their next read); writers with it
        open are writing to the old file.
        """
        self._guard.check("edit_file", path=path)
        target = self._policy.resolve(path, write=True)
        old_b = old.encode()
        if not old_b:
            raise ValueError("old must not be empty")
        data = target.read_bytes()
        if (count := data.count(old_b)) != 1:
            raise ValueError(f"{path}: old matches {count} times, not exactly once")
        fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as f:
                os.fchmod(f.fileno(), S_IMODE(target.stat().st_mode))
                f.write(data.replace(old_b, new.encode(), 1))
            os.replace(tmp, target)
        except BaseException:
            os.unlink(tmp)
            raise

    @remote_method()
    def chmod(self, path: str, mode: int) -> None:
        """chmod: set the permission bits (at most ``0o7777``). Symlinks can't be chmodded, as on Linux."""
        self._guard.check("chmod", path=path, mode=mode)
        if not 0 <= mode <= 0o7777:
            raise ValueError("mode must be permission bits, 0o7777 at most")
        os.chmod(self._policy.resolve(path, write=True), mode)

    @remote_method()
    def rename(self, path: str, to: str) -> None:
        """rename(2): move ``path`` to ``to``, replacing a file there (or an empty directory, for a directory).

        Atomic, so writing a temp file and renaming it over the original is
        the safe rewrite, as on Unix. Across mounts on the server it fails
        with EXDEV, as rename(2) does: there's no copy-and-delete fallback. A
        symlink is moved, not followed. A directory holding read-only paths
        can't be moved, since that would change their paths.
        """
        self._guard.check("rename", path=path, to=to)
        source = self._policy.resolve(path, write=True, nofollow=True)
        dest = self._policy.resolve(to, write=True, nofollow=True, is_dir=source.is_dir() and not source.is_symlink())
        if self._policy.root in (source, dest):
            raise PolicyError("can't rename the workspace root")
        if S_ISDIR(source.lstat().st_mode):
            self._check_tree_writable(source)
        os.rename(source, dest)

    def _check_tree_writable(self, top: Path) -> None:
        for p, st in _entries_below(top):
            if not self._policy.is_writable(p, S_ISDIR(st.st_mode)):
                raise PolicyError(f"{p.relative_to(self._policy.root).as_posix()!r} is read-only")

    @remote_method()
    def mkdir(self, path: str, parents: bool = False) -> None:
        """mkdir, or ``mkdir -p`` with ``parents``: create missing parents, and don't fail if it exists."""
        self._guard.check("mkdir", path=path, parents=parents)
        self._policy.resolve(path, write=True, is_dir=True).mkdir(parents=parents, exist_ok=parents)

    @remote_method()
    def delete(self, path: str, recursive: bool = False) -> None:
        """unlink a file or symlink, rmdir an empty directory, or with ``recursive`` remove a directory tree (``rm -r``).

        A tree that holds excluded or read-only paths is refused before
        anything is removed: the caller can't see the excluded ones, so it
        can't know what it would destroy.
        """
        self._guard.check("delete", path=path, recursive=recursive)
        target = self._policy.resolve(path, write=True, nofollow=True)
        if target == self._policy.root:
            raise PolicyError("can't delete the workspace root")
        if not S_ISDIR(target.lstat().st_mode):
            target.unlink()
        elif not recursive:
            target.rmdir()
        else:
            if not all(self._policy.is_visible(p, S_ISDIR(st.st_mode)) for p, st in _entries_below(target)):
                raise PolicyError(f"{path!r} holds paths that aren't exposed")
            self._check_tree_writable(target)
            shutil.rmtree(target)


def _type(mode: int) -> str:
    if S_ISREG(mode):
        return "file"
    if S_ISDIR(mode):
        return "dir"
    return "symlink" if S_ISLNK(mode) else "other"


def _stat(st: os.stat_result) -> dict:
    return {"type": _type(st.st_mode), "size": st.st_size, "mtime": st.st_mtime, "mode": S_IMODE(st.st_mode)}


def _entries(policy: PathPolicy, directory: Path) -> list[tuple[Path, os.stat_result]]:
    """The exposed entries of ``directory``, by name, with their lstat."""
    with os.scandir(directory) as it:
        found = sorted(((Path(e.path), e.stat(follow_symlinks=False)) for e in it), key=lambda e: e[0].name)
    return [(p, st) for p, st in found if policy.is_visible(p, S_ISDIR(st.st_mode))]


def _walk(policy: PathPolicy, top: Path, depth: int | None = None) -> Iterator[tuple[Path, os.stat_result]]:
    """Exposed paths under ``top``, depth first, at most ``depth`` levels down. Doesn't follow
    symlinks or enter excluded directories, and skips directories it can't read, like find."""
    for p, st in _entries(policy, top):
        yield p, st
        if S_ISDIR(st.st_mode) and depth != 1:
            try:
                yield from _walk(policy, p, None if depth is None else depth - 1)
            except PermissionError:
                pass


def _entries_below(top: Path) -> Iterator[tuple[Path, os.stat_result]]:
    """Every path under ``top``, excluded ones included, without following symlinks."""
    for dirpath, dirnames, filenames in os.walk(top):
        for name in dirnames + filenames:
            p = Path(dirpath, name)
            yield p, p.lstat()


def _int_param(params, name: str, default: int | None = None) -> int | None:
    value = params.get(name)
    if value is None:
        return default
    n = int(value)
    if n < 0:
        raise ValueError(f"{name} must be >= 0")
    return n


def _json(query: zenoh.Query, value) -> None:
    query.reply(query.key_expr, json.dumps(value), encoding=zenoh.Encoding.APPLICATION_JSON)


MIN_CHUNK, MAX_CHUNK = 4 << 10, 4 << 20
"""Chunk sizes a read or stream may ask for. Past 1 MiB, bigger chunks don't send any
faster, and a stream holds up to ``READ_AHEAD + 1`` of them in memory."""
STREAM_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")


def _range(params) -> tuple[int, int | None, int, str]:
    """``start``, ``end`` (exclusive; None: to the current end of the file), ``chunk`` (clamped) and ``readahead``."""
    start = _int_param(params, "start", 0)
    end = _int_param(params, "end")
    if end is not None and end < start:
        raise ValueError("end must be >= start")
    chunk = min(max(_int_param(params, "chunk", CHUNK_SIZE), MIN_CHUNK), MAX_CHUNK)
    readahead = params.get("readahead") or "auto"
    if readahead not in READAHEAD_MODES:
        raise ValueError(f"readahead must be one of {', '.join(READAHEAD_MODES)}")
    return start, end, chunk, readahead


READ_AHEAD = 2
"""Chunks read ahead of the one being sent. Measured best at 2; deeper is slower."""

READ_AHEAD_AFTER = 4
"""With ``readahead=auto``, chunks sent directly before read-ahead starts.

Decided as the read goes rather than from the file's size, so short reads
never pay for the thread, and a file that grows while being read still
gets read-ahead once it's long enough."""

READAHEAD_MODES = ("auto", "on", "off")


def read_ahead(source: Iterator, depth: int) -> Iterator:
    """Run ``source`` on its own thread, up to ``depth`` items ahead of the consumer.

    Exceptions from ``source`` are raised in the consumer. If the consumer
    stops early, the thread stops at its next item, and the consumer waits
    for it, so whatever ``source`` reads from can be closed straight after.
    """
    items: queue.Queue = queue.Queue(maxsize=depth)
    done = object()
    abandoned = threading.Event()

    def produce():
        try:
            for item in source:
                while not abandoned.is_set():
                    try:
                        items.put((item, None), timeout=0.1)
                        break
                    except queue.Full:
                        pass
                if abandoned.is_set():
                    return
            items.put((done, None))
        except BaseException as e:
            items.put((done, e))

    producer = threading.Thread(target=produce, name="read-ahead", daemon=True)
    producer.start()
    try:
        while True:
            item, error = items.get()
            if item is done:
                if error is not None:
                    raise error
                return
            yield item
    finally:
        abandoned.set()
        producer.join()


class _Pacer:
    """Keeps a stream under ``rate`` bytes/s (0: no limit). The rate can change while it runs.

    Call it after sending ``n`` bytes; it waits until the stream is back
    under the rate, averaged since the rate was last set, so a slow send
    isn't made up for by a burst later. Waits in short slices so a stop is
    still seen promptly.
    """

    def __init__(self, rate: int, stopped):
        self._stopped = stopped
        self._lock = threading.Lock()
        self.set(rate)

    def set(self, rate: int) -> None:
        with self._lock:
            self.rate = rate
            self._since = time.monotonic()
            self._sent = 0

    def chunk(self, requested: int) -> int:
        """At most a second's worth, so a slow stream still sends often enough not to look like it went quiet."""
        rate = self.rate
        return max(min(requested, rate), MIN_CHUNK) if rate else requested

    def __call__(self, n: int) -> None:
        while not self._stopped():
            with self._lock:
                if not self.rate:
                    return
                self._sent += n
                n = 0
                wait = self._since + self._sent / self.rate - time.monotonic()
            if wait <= 0:
                return
            time.sleep(min(wait, 0.1))


class FileReadStream:
    """Read ticket: an open file. Routes under the ticket key::

        get <t>/read?start=S[;end=E][;chunk=C][;readahead=R]  → raw byte replies, C bytes each, then the
                                                          query finishes. Always at least one reply
                                                          (empty past EOF). The slow path: for
                                                          single chunks and small reads.
        get <t>/stream?id=I;start=S[;end=E][;chunk=C][;readahead=R][;max_rate=B]  → {} once queued. Publishes
                                                          [S, E) to <t>/data/I, then an end marker,
                                                          at most B bytes/s on average (0 or absent: no limit).
        get <t>/stream?stop;id=I                        → {} and stops stream I
        get <t>/stream?rate;id=I;max_rate=B             → {} and sets stream I's max_rate while it runs
        get <t>/close                                   → {}. Stops every stream; later reads are refused.
        get <t>/heartbeat                               → {}. Keeps the ticket alive (see below).

    A ticket lives until ``close``, or until it goes ``ttl`` seconds (from
    ``open_read``) without a request, when the registry revokes it. A
    running stream doesn't count: a client that wants its stream to outlast
    the ttl heartbeats, so a client that vanishes takes its streams with it.

    ``end`` is exclusive. Left out, a read or stream goes to the end of the file
    as it is then, like ``cat``. To follow a growing file (``tail -f``),
    stream again from where the last one ended; a ticket can stream as often
    and for as long as its client heartbeats. ``chunk`` is clamped to [MIN_CHUNK, MAX_CHUNK].
    ``readahead`` is auto (the default, see ``READ_AHEAD_AFTER``), on or off.

    Stream samples say what they are in their attachment: ``o=<offset>`` on
    data, ``end=<offset>`` on the empty final sample, ``err=<message>`` if the
    stream failed. There's no backpressure from subscribers (zenoh's is per
    link, and would let one slow stream stall the session); publishing blocks
    only when the link itself is full. A client subscribes to ``<t>/data/I``
    before asking for stream I, picking I itself, and re-streams from its
    last offset if it sees a gap, goes quiet or falls behind.

    The file may grow, as with ``tail -f``. If it's replaced (another inode
    at the path) or truncated (smaller than the ticket has seen it), a read
    or stream fails with ``FileChangedError``; that's checked before and
    after reading. Call ``open_read`` again for the new file. Bytes rewritten
    in place aren't detected, as with any Unix reader: to confirm a read,
    hash what you received and compare with ``hash``.
    """

    def __init__(self, path: Path, st: os.stat_result, zenoh_session: zenoh.Session | None = None):
        self.path = path
        self.inode = st.st_ino
        self._largest = st.st_size
        """The largest size seen. Below it, the file was truncated."""
        self.ticket = None
        self._session = zenoh_session
        self._lock = threading.Lock()
        self._stopped: set[str] = set()
        self._pacers: dict[str, _Pacer] = {}
        self._closed = False

    def routes(self) -> dict:
        return {"read": self.read, "stream": self.stream, "close": self.close_route}

    def _check_open(self) -> None:
        if self._closed:
            raise ValueError("file is closed")

    def _check(self, st: os.stat_result) -> None:
        if st.st_ino != self.inode:
            raise FileChangedError(f"{self.path.name} was replaced since open_read")
        with self._lock:
            if st.st_size < self._largest:
                raise FileChangedError(f"{self.path.name} was truncated from {self._largest} to {st.st_size} bytes")
            self._largest = st.st_size

    def _chunks(self, start: int, end: int | None, chunk: int | Callable[[], int], readahead: str = "auto",
                stopped=lambda: False) -> Iterator[tuple[int, bytes]]:
        """``(offset, bytes)`` pairs over [start, end), checking the file before and after.

        ``chunk`` is a size, or a function giving the size for each read.

        Read ahead on a second thread, ``READ_AHEAD`` chunks deep. ``pread``
        releases the GIL and sending to zenoh mostly does too, so the next
        chunk is read while the last one is being sent. Profiled: reading
        is ~60% of a serial serve, sending ~40%. See ``read_ahead``.
        """
        # O_NOFOLLOW: never read through a symlink swapped in since open_read.
        fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            self._check(os.fstat(fd))

            size = chunk if callable(chunk) else lambda: chunk

            def chunks():
                offset = start
                while (end is None or offset < end) and not stopped():
                    n = size()
                    data = os.pread(fd, n if end is None else min(n, end - offset), offset)
                    if not data:
                        break
                    yield offset, data
                    offset += len(data)

            source = chunks()
            if readahead == "auto":
                # A thread costs more than a short read takes to send, so
                # only start one once the read has gone on long enough.
                for _ in range(READ_AHEAD_AFTER):
                    item = next(source, None)
                    if item is None:
                        break
                    yield item
                else:
                    yield from read_ahead(source, READ_AHEAD)
            elif readahead == "on":
                yield from read_ahead(source, READ_AHEAD)
            else:
                yield from source
            self._check(os.stat(self.path, follow_symlinks=False))
        finally:
            os.close(fd)

    def read(self, query: zenoh.Query) -> None:
        self._check_open()
        replied = False
        for _, data in self._chunks(*_range(query.parameters)):
            query.reply(query.key_expr, data, encoding=zenoh.Encoding.ZENOH_BYTES)
            replied = True
        if not replied:
            query.reply(query.key_expr, b"", encoding=zenoh.Encoding.ZENOH_BYTES)

    def stream(self, query: zenoh.Query) -> None:
        params = query.parameters
        stream_id = params.get("id") or ""
        if not STREAM_ID.fullmatch(stream_id):
            raise ValueError("stream needs an id of 1-64 letters, digits, '-' or '_'")
        if "stop" in params:
            with self._lock:
                self._stopped.add(stream_id)
            _json(query, {})
            return
        if "rate" in params:
            with self._lock:
                pacer = self._pacers.get(stream_id)
            if pacer is not None:
                pacer.set(_int_param(params, "max_rate", 0))
            _json(query, {})
            return
        self._check_open()
        if self._session is None or self.ticket is None:
            raise RuntimeError("streaming isn't available on this ticket")
        start, end, chunk, readahead = _range(params)
        pacer = _Pacer(_int_param(params, "max_rate", 0), lambda: self._closed or stream_id in self._stopped)
        with self._lock:
            self._pacers[stream_id] = pacer
        publisher = self._session.declare_publisher(
            f"{self.ticket.key}/data/{stream_id}",
            congestion_control=zenoh.CongestionControl.BLOCK,
            reliability=zenoh.Reliability.RELIABLE,
        )
        # The query is answered once the stream is queued; the stream runs
        # on its own thread, so it isn't bound by the query or a ticket worker.
        threading.Thread(target=self._publish, args=(publisher, stream_id, start, end, chunk, readahead, pacer),
                         name=f"stream-{stream_id}", daemon=True).start()
        _json(query, {})

    def _publish(self, publisher, stream_id: str, start: int, end: int | None, chunk: int, readahead: str,
                 pace: _Pacer) -> None:
        stopped = lambda: self._closed or stream_id in self._stopped
        offset = start
        try:
            for offset, data in self._chunks(start, end, lambda: pace.chunk(chunk), readahead, stopped):
                publisher.put(data, encoding=zenoh.Encoding.ZENOH_BYTES, attachment=f"o={offset}")
                offset += len(data)
                pace(len(data))
            if not stopped():
                publisher.put(b"", attachment=f"end={offset}")
        except Exception as e:
            logger.warning(f"stream {stream_id} of {self.path.name} failed: {e}")
            try:
                publisher.put(b"", attachment=f"err={type(e).__name__}: {e}")
            except Exception:
                pass
        finally:
            publisher.undeclare()
            with self._lock:
                self._stopped.discard(stream_id)
                self._pacers.pop(stream_id, None)

    def close_route(self, query: zenoh.Query) -> None:
        self.close()
        if self.ticket is not None:
            self.ticket.retire()
        _json(query, {})

    def close(self) -> None:
        """Stop every running stream, at its next chunk. Also called when the ticket is revoked."""
        self._closed = True


class FileChangedError(RuntimeError):
    pass


class FileWriteStream:
    """Write ticket: an open file. Routes under the ticket key::

        get <t>/write?offset=N  + payload   → pwrite at N. {}
        get <t>/append?seq=K    + payload   → write at the end of the file. {}
        get <t>/truncate?size=N             → ftruncate to N bytes, shrinking or extending with zeros. {}
        get <t>/flush                       → fsync. {}
        get <t>/close                       → close the file. {}
        get <t>/heartbeat                   → {}. Keeps an idle ticket alive.

    There's no temp file and no rename: writes land in the file as they
    arrive, as with any Unix writer.

    ``write`` is positional, so resending one is harmless and order doesn't
    matter. A client can keep many in flight and resend any that go unacked.

    ``append`` uses ``O_APPEND``, so each one lands at the current end
    without tearing. Like Unix appends, order between them is only what the
    client imposes: wait for each ack before sending the next. ``seq``
    counts a ticket's appends from 0, so a resend whose first try landed (the
    ack was lost) is acked again without being written twice. A ``seq``
    further ahead than the next one is refused.

    Repeating ``flush`` or ``close`` is harmless. After ``close`` the ticket
    answers repeats for ``tickets.RETIRE_GRACE`` seconds, then is revoked.
    A ticket that goes ``ttl`` seconds without a request (heartbeats count)
    is revoked by the registry, which closes the file.
    """

    def __init__(self, path: Path, truncate: bool, exclusive: bool = False):
        flags = os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC
        create = os.O_CREAT | (os.O_EXCL if exclusive else 0) | (os.O_TRUNC if truncate else 0)
        self._fd = os.open(path, flags | create, 0o666)
        # Two descriptors: on Linux pwrite ignores its offset on an O_APPEND one.
        self._append_fd = os.open(path, flags | os.O_APPEND)
        self.size_at_open = os.fstat(self._fd).st_size
        self.ticket = None
        self._lock = threading.Lock()
        self._next_seq = 0
        self._closed = False

    def routes(self) -> dict:
        return {"write": self.write, "append": self.append, "truncate": self.truncate, "flush": self.flush,
                "close": self.close_route}

    def _payload(self, query: zenoh.Query) -> bytes:
        return query.payload.to_bytes() if query.payload is not None else b""

    def _check_open(self) -> None:
        if self._closed:
            raise ValueError("file is closed")

    def write(self, query: zenoh.Query) -> None:
        offset = _int_param(query.parameters, "offset")
        if offset is None:
            raise ValueError("write needs offset")
        data = self._payload(query)
        with self._lock:
            self._check_open()
            os.pwrite(self._fd, data, offset)
        _json(query, {})

    def append(self, query: zenoh.Query) -> None:
        seq = _int_param(query.parameters, "seq")
        if seq is None:
            raise ValueError("append needs seq")
        data = self._payload(query)
        with self._lock:
            self._check_open()
            if seq > self._next_seq:
                raise ValueError(f"append seq {seq} is ahead, expected {self._next_seq}")
            if seq == self._next_seq:
                os.write(self._append_fd, data)
                self._next_seq += 1
            # seq below the next one: a resend of an append that already landed
        _json(query, {})

    def truncate(self, query: zenoh.Query) -> None:
        size = _int_param(query.parameters, "size")
        if size is None:
            raise ValueError("truncate needs size")
        with self._lock:
            self._check_open()
            os.ftruncate(self._fd, size)
        _json(query, {})

    def flush(self, query: zenoh.Query) -> None:
        with self._lock:
            self._check_open()
            os.fsync(self._fd)
        _json(query, {})

    def close_route(self, query: zenoh.Query) -> None:
        self.close()
        if self.ticket is not None:
            self.ticket.retire()
        _json(query, {})

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                os.close(self._fd)
                os.close(self._append_fd)
