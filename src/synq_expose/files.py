"""``expose-synq file``: one small command per FsRead / FsWrite method, plus ``pull`` and ``push`` for tickets.

A remote path is ``TOPIC:PATH``, as scp writes ``host:path``. Without a
``:`` the topic comes from ``EXPOSE_SYNQ_TOPIC``, so an agent's shell can
set it once; a leading ``:`` (``:a:b``) means that default too, for a path
holding a colon. Each command does what its method does and nothing more.
"""
import os
import stat
import time
from collections.abc import Iterable, Iterator
from typing import BinaryIO

from synq_expose import client
from synq_expose.client import RemoteFile, TicketError
from synq_expose.config import TOPIC_ENV
from synq_expose.fs import CHUNK_SIZE

_TYPE_BITS = {"file": stat.S_IFREG, "dir": stat.S_IFDIR, "symlink": stat.S_IFLNK, "other": 0}


class NoTopic(ValueError):
    pass


def split(arg: str, default: str | None = None) -> tuple[str, str]:
    """``TOPIC:PATH`` as ``(topic, path)``. A missing or empty topic is ``default``; an empty path is ``.``.

    One starting ``./`` is all path, as in scp.
    """
    topic, colon, path = arg.partition(":")
    if not colon or arg.startswith("./"):
        topic, path = "", arg
    topic = topic.rstrip("/") or default
    if not topic:
        raise NoTopic(f"{arg!r} has no topic: write TOPIC:PATH or set {TOPIC_ENV}")
    return topic, path or "."


def local_chunks(f: BinaryIO, size: int = CHUNK_SIZE) -> Iterator[bytes]:
    while chunk := f.read(size):
        yield chunk


def pull(z, fs_read, path: str, out, start: int = 0, end: int | None = None) -> None:
    """Stream [start, end) of ``path`` to ``out``: a binary file, or a function opening one.

    ``out`` is only opened once the remote file is, so a missing one doesn't
    leave an empty local file behind.
    """
    ticket = fs_read.open_read(path)["ticket"]
    try:
        f = out() if callable(out) else out
        try:
            for chunk in client.stream(z, ticket, start, end):
                f.write(chunk)
        finally:
            f.close() if callable(out) else f.flush()
    finally:
        try:
            client.close(z, ticket)
        except TicketError:
            pass                        # it times out instead


def push(z, fs_write, path: str, data: Iterable[bytes], *, append: bool = False, exclusive: bool = False) -> int:
    """Write ``data`` to ``path``: truncating it first, or with ``append`` appending as O_APPEND does.

    Returns the bytes written.
    """
    opened = fs_write.open_write(path, truncate=not append, exclusive=exclusive)
    if not append:
        return client.write_all(z, opened["ticket"], data)
    written = 0
    with RemoteFile(z, opened["ticket"], opened["size"]) as f:
        for chunk in data:
            f.append(chunk)
            written += len(chunk)
    return written


def _mtime(mtime: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(mtime))


def ls_line(entry: dict) -> str:
    """``-rw-r--r--  1234 2026-10-17 12:00 name``, as ``ls -l`` without links and owners."""
    mode = stat.filemode(_TYPE_BITS[entry["type"]] | entry["mode"])
    if entry["type"] == "other":
        mode = "?" + mode[1:]
    return f"{mode} {entry['size']:>10} {_mtime(entry['mtime'])} {entry['name']}"


def stat_lines(st: dict) -> list[str]:
    return [f"type: {st['type']}", f"size: {st['size']}", f"mode: {st['mode']:04o}",
            f"mtime: {time.strftime('%Y-%m-%dT%H:%M:%S%z', time.localtime(st['mtime']))}"]


def grep_line(match: dict) -> str:
    return f"{match['path']}:{match['line']}:{match['text']}"


def parse_mode(mode: str) -> int:
    """An octal mode as chmod takes it: ``644``, ``0755``, ``0o600``."""
    try:
        return int(mode.removeprefix("0o"), 8)
    except ValueError:
        raise ValueError(f"{mode!r} isn't an octal mode") from None


def read_text(path: str, stdin: BinaryIO) -> str:
    """A file's text, or stdin's for ``-``."""
    if path == "-":
        return stdin.read().decode()
    with open(os.path.expanduser(path), "rb") as f:
        return f.read().decode()
