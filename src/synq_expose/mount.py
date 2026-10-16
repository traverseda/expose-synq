"""``expose-synq mount``: a share as a local FUSE filesystem.

Each FUSE call is the RPC of the same name (``getattr`` is ``stat``,
``unlink``/``rmdir`` are ``delete``), and file handles are tickets: a read
ticket for reading, a write ticket for writing, both for ``O_RDWR``. A
remote error's errno (``[Errno N]`` in its message) goes to the kernel as
is; one without becomes EIO.

Writes return once they're in flight. ``flush`` (every ``close(2)``) and
``fsync`` wait for them, so an error shows up there at the latest, as on
NFS. Before ``getattr`` or a read, this mount's own writes to that path are
waited for, so it sees what it wrote, as on Unix.

There are no RPCs for symlinks, hard links, ownership or timestamps, so
``readlink``, ``symlink``, ``link``, ``chown`` and ``utimens`` aren't
implemented (ENOSYS).
"""
import errno
import functools
import os
import re
import stat
import threading
from dataclasses import dataclass, field
from itertools import count

import mfusepy
from SpiriSynq.remote_callables import RpcException

from synq_expose import client
from synq_expose.client import RemoteFile, TicketError

_ERRNO = re.compile(r"\[Errno (\d+)\]")
_TYPES = {"file": stat.S_IFREG, "dir": stat.S_IFDIR, "symlink": stat.S_IFLNK, "other": stat.S_IFREG}


def _os_error(e: Exception) -> OSError:
    m = _ERRNO.search(str(e))
    return mfusepy.FuseOSError(int(m.group(1)) if m else errno.EIO)


def remote(method):
    """Turn remote errors into the errno they carry."""
    @functools.wraps(method)            # mfusepy reads create's signature
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except (RpcException, TicketError) as e:
            raise _os_error(e) from e
    return wrapper


@dataclass
class Handle:
    path: str
    reader: str | None = None           # read ticket
    heartbeat: client.Heartbeat | None = None
    writer: RemoteFile | None = None
    append: bool = False


@dataclass
class SynqFS(mfusepy.Operations):
    z: object                           # the client's zenoh session, for tickets
    fs_read: object
    fs_write: object | None             # None on a read-only share
    use_ns = True
    _handles: dict[int, Handle] = field(default_factory=dict)
    _next_fh: count = field(default_factory=lambda: count(1))
    _lock: threading.Lock = field(default_factory=threading.Lock)

    # -- helpers ---------------------------------------------------------

    def _writers(self, path: str) -> list[RemoteFile]:
        with self._lock:
            return [h.writer for h in self._handles.values() if h.writer is not None and h.path == path]

    def _settle(self, path: str) -> None:
        """Wait for this mount's writes to ``path``, so what comes next sees them."""
        for writer in self._writers(path):
            writer.wait()

    def _open_reader(self, h: Handle) -> None:
        h.reader = self.fs_read.open_read(h.path)["ticket"]
        h.heartbeat = client.Heartbeat(self.z, h.reader)

    def _close_reader(self, h: Handle) -> None:
        if h.reader is not None:
            h.heartbeat.stop()
            try:
                client.close(self.z, h.reader)
            except TicketError:
                pass                    # it times out instead
            h.reader = None

    def _add(self, h: Handle) -> int:
        with self._lock:
            fh = next(self._next_fh)
            self._handles[fh] = h
        return fh

    def _handle(self, fh: int) -> Handle:
        with self._lock:
            return self._handles[fh]

    # -- metadata --------------------------------------------------------

    @remote
    def getattr(self, path, fh=None):
        path = self._handle(fh).path if path is None else _rel(path)
        self._settle(path)
        st = self.fs_read.stat(path)
        mtime = int(st["mtime"] * 1e9)
        return {
            "st_mode": _TYPES[st["type"]] | st["mode"],
            "st_size": st["size"],
            "st_blocks": (st["size"] + 511) // 512,
            "st_nlink": 1,
            "st_uid": os.getuid(),
            "st_gid": os.getgid(),
            "st_mtime": mtime, "st_atime": mtime, "st_ctime": mtime,
        }

    @remote
    def readdir(self, path, fh):
        return [".", "..", *((e["name"], {"st_mode": _TYPES[e["type"]]}, 0)
                             for e in self.fs_read.list_dir(_rel(path)))]

    @remote
    def chmod(self, path, mode):
        self.fs_write.chmod(_rel(path), stat.S_IMODE(mode))

    @remote
    def mkdir(self, path, mode):
        path = _rel(path)
        self.fs_write.mkdir(path)
        self.fs_write.chmod(path, stat.S_IMODE(mode))

    @remote
    def rmdir(self, path):
        self.fs_write.delete(_rel(path))

    @remote
    def unlink(self, path):
        self.fs_write.delete(_rel(path))

    @remote
    def rename(self, old, new):
        old, new = _rel(old), _rel(new)
        self.fs_write.rename(old, new)
        with self._lock:
            for h in self._handles.values():
                if h.path == old or h.path.startswith(old + "/"):
                    h.path = new + h.path[len(old):]

    @remote
    def truncate(self, path, length, fh=None):
        # mfusepy doesn't pass ftruncate's handle, so this is always by path.
        path = _rel(path)
        self._settle(path)
        if writers := self._writers(path):
            writers[0].truncate(length)
        else:
            opened = self.fs_write.open_write(path, truncate=False)
            with RemoteFile(self.z, opened["ticket"]) as writer:
                writer.truncate(length)
        # Our own read tickets would take this for someone else's truncate.
        with self._lock:
            readers = [h for h in self._handles.values() if h.path == path and h.reader is not None]
        for h in readers:
            self._close_reader(h)
            self._open_reader(h)

    # -- files -----------------------------------------------------------

    @remote
    def open(self, path, flags):
        return self._open(_rel(path), flags)

    @remote
    def create(self, path, mode, flags):
        path = _rel(path)
        opened = self.fs_write.open_write(path, truncate=bool(flags & os.O_TRUNC), exclusive=bool(flags & os.O_EXCL))
        try:
            self.fs_write.chmod(path, stat.S_IMODE(mode))
        except BaseException:
            client.close(self.z, opened["ticket"])
            raise
        return self._open(path, flags, opened)

    def _open(self, path: str, flags: int, opened: dict | None = None) -> int:
        """``opened``: a write ticket from ``create``, kept if this handle writes."""
        access = flags & os.O_ACCMODE
        h = Handle(path, append=bool(flags & os.O_APPEND))
        if opened is not None:
            h.writer = RemoteFile(self.z, opened["ticket"], opened["size"])
        try:
            if access != os.O_RDONLY and h.writer is None:
                opened = self.fs_write.open_write(path, truncate=bool(flags & os.O_TRUNC))
                h.writer = RemoteFile(self.z, opened["ticket"], opened["size"])
            elif access == os.O_RDONLY and h.writer is not None:
                h.writer.close()        # open(O_CREAT|O_RDONLY): created, not for writing
                h.writer = None
            if access != os.O_WRONLY:
                self._settle(path)
                self._open_reader(h)
        except BaseException:
            self._release(h)
            raise
        return self._add(h)

    @remote
    def read(self, path, size, offset, fh):
        h = self._handle(fh)
        if h.reader is None:
            raise mfusepy.FuseOSError(errno.EBADF)
        self._settle(h.path)
        return b"".join(client.read_chunks(self.z, h.reader, offset, offset + size))

    @remote
    def write(self, path, data, offset, fh):
        h = self._handle(fh)
        if h.writer is None:
            raise mfusepy.FuseOSError(errno.EBADF)
        if h.append:
            h.writer.append(data)       # O_APPEND on the server: the kernel's offset may be stale
        else:
            h.writer.write(offset, data)
        return len(data)

    @remote
    def flush(self, path, fh):
        if (writer := self._handle(fh).writer) is not None:
            writer.wait()

    @remote
    def fsync(self, path, datasync, fh):
        if (writer := self._handle(fh).writer) is not None:
            writer.flush()

    @remote
    def release(self, path, fh):
        with self._lock:
            h = self._handles.pop(fh)
        self._release(h)

    def _release(self, h: Handle) -> None:
        self._close_reader(h)
        if h.writer is not None:
            try:
                h.writer.close()
            except TicketError:
                h.writer._abandon()


def _rel(path: str) -> str:
    """FUSE's ``/a/b`` as a workspace path."""
    return path.lstrip("/") or "."


class _FUSE(mfusepy.FUSE):
    def rename_fuse_3(self, old: bytes, new: bytes, flags: int) -> int:
        # mfusepy drops renameat2's flags, which would make RENAME_NOREPLACE
        # replace. rename RPC has none of them, so refuse, as a filesystem
        # without them does; mv -n then checks for itself.
        if flags:
            return -errno.EINVAL
        return super().rename_fuse_3(old, new, flags)


def mount(operations: SynqFS, mountpoint: str, *, read_only: bool, fsname: str, debug: bool = False) -> None:
    """Serve ``operations`` on ``mountpoint`` until it's unmounted or interrupted."""
    # hard_remove: unlink what's asked, rather than renaming an open file to
    # .fuse_hidden*; the server's open descriptors keep it readable, as on Unix.
    _FUSE(operations, mountpoint, foreground=True, ro=read_only, fsname=fsname, subtype="expose-synq",
          hard_remove=True, debug=debug)
