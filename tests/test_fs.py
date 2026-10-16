"""Filesystem objects and file tickets, called over the test network like a remote agent would."""
import hashlib
import json
import os
import random
import threading
import time

import pytest
from SpiriSynq.remote_callables import RpcException

from synq_expose import client
from synq_expose.client import TicketError
from synq_expose.fs import CHUNK_SIZE, FsRead, FsWrite
from synq_expose.testing import wait_for
from synq_expose.workspace import Workspace


def current_umask() -> int:
    umask = os.umask(0)
    os.umask(umask)
    return umask


def read_bytes(z, ticket, **kwargs) -> bytes:
    """Collect a small read in tests. Real clients consume ``read_chunks`` as it streams."""
    return b"".join(client.read_chunks(z, ticket, **kwargs))


def sha256(data: bytes) -> str:
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


@pytest.fixture
def ro(share):
    s = share()
    (s.workspace / "src").mkdir()
    (s.workspace / "src" / "main.py").write_text("print('hi')\n")
    (s.workspace / ".env").write_text("SECRET=1\n")
    return s


@pytest.fixture
def rw(share):
    s = share(allow_write=True, readonly_paths=(".git/**",))
    (s.workspace / "src").mkdir()
    (s.workspace / ".git").mkdir()
    (s.workspace / "existing.txt").write_bytes(b"old\n")
    return s


def files(workspace) -> set[str]:
    return {str(p.relative_to(workspace)) for p in workspace.rglob("*")}


# ── Capabilities ────────────────────────────────────────────────────────────

def test_read_only_share_has_no_write_object(ro):
    assert ro.exists("/fs/read")
    assert not ro.exists("/fs/write")
    assert not ro.exists("/shell")


def test_workspace_advertises_grants(rw):
    ws = rw.mirror(Workspace)
    assert ws.root_name == "ws"
    assert ws.can_write and not ws.can_execute
    assert rw.exists("/fs/write")


# ── RPCs ────────────────────────────────────────────────────────────────────

def test_hash(ro):
    fs = ro.mirror(FsRead, "/fs/read")
    assert fs.hash("src/main.py") == sha256(b"print('hi')\n")


def test_open_read_returns_stat_and_hidden_ticket(ro):
    fs = ro.mirror(FsRead, "/fs/read")
    handle = fs.open_read("src/main.py")
    assert handle["size"] == 12
    assert handle["mtime"] == (ro.workspace / "src" / "main.py").stat().st_mtime
    assert set(handle) == {"ticket", "size", "mtime", "ttl"}
    assert handle["ticket"].startswith("node/tickets/@")
    assert len(ro.tickets) == 1
    assert list(ro.z.get("node/tickets/**", timeout=0.5)) == []


@pytest.mark.parametrize("path", [
    "../outside",
    "src/../.env",
    ".env",          # default excludes
    "missing.txt",
    "src",           # a directory
])
def test_open_read_refuses(ro, path):
    fs = ro.mirror(FsRead, "/fs/read")
    with pytest.raises(RpcException):
        fs.open_read(path)
    assert len(ro.tickets) == 0


def open_write(s, path, truncate=True):
    handle = s.mirror(FsWrite, "/fs/write").open_write(path, truncate=truncate)
    return handle["ticket"]


def test_open_write_mints_a_ticket(rw):
    handle = rw.mirror(FsWrite, "/fs/write").open_write("new.txt", truncate=True)
    assert handle["ticket"].startswith("node/tickets/@")
    assert handle["size"] == 0
    assert (rw.workspace / "new.txt").exists()   # opened, as with O_CREAT
    assert len(rw.tickets) == 1


def test_open_write_without_truncate_keeps_contents(rw):
    assert rw.mirror(FsWrite, "/fs/write").open_write("existing.txt", truncate=False)["size"] == 4
    assert (rw.workspace / "existing.txt").read_bytes() == b"old\n"


def test_open_write_truncates(rw):
    assert rw.mirror(FsWrite, "/fs/write").open_write("existing.txt", truncate=True)["size"] == 0
    assert (rw.workspace / "existing.txt").read_bytes() == b""


@pytest.mark.parametrize("path", [".git/config", "../outside", ".env"])
def test_open_write_refuses(rw, path):
    fs = rw.mirror(FsWrite, "/fs/write")
    with pytest.raises(RpcException):
        fs.open_write(path, truncate=True)
    assert len(rw.tickets) == 0


def test_open_write_into_missing_dir_fails(rw):
    with pytest.raises(RpcException, match="No such file"):
        rw.mirror(FsWrite, "/fs/write").open_write("nope/new.txt", truncate=True)
    assert len(rw.tickets) == 0


def test_audit_log(share, tmp_path):
    log = tmp_path / "audit.jsonl"
    s = share(audit_log=log)
    (s.workspace / "a.txt").write_text("a")
    s.mirror(FsRead, "/fs/read").hash("a.txt")
    records = [json.loads(line) for line in log.read_text().splitlines()]
    assert [(r["op"], r["path"]) for r in records] == [("hash", "a.txt")]


# ── Read tickets ────────────────────────────────────────────────────────────

def test_read_small_file(ro):
    handle = ro.mirror(FsRead, "/fs/read").open_read("src/main.py")
    assert read_bytes(ro.z, handle["ticket"]) == b"print('hi')\n"


def test_read_spans_chunks(ro):
    data = bytes(range(256)) * (3 * CHUNK_SIZE // 256) + b"tail"
    (ro.workspace / "big.bin").write_bytes(data)
    handle = ro.mirror(FsRead, "/fs/read").open_read("big.bin")
    chunks = list(client.read_chunks(ro.z, handle["ticket"]))
    assert [len(c) for c in chunks] == [CHUNK_SIZE] * 3 + [4]
    assert b"".join(chunks) == data
    assert ro.mirror(FsRead, "/fs/read").hash("big.bin") == sha256(data)


def test_read_offset_and_length(ro):
    (ro.workspace / "abc.txt").write_bytes(b"abcdefghij")
    t = ro.mirror(FsRead, "/fs/read").open_read("abc.txt")["ticket"]
    assert read_bytes(ro.z, t, start=3) == b"defghij"
    assert read_bytes(ro.z, t, start=3, end=5) == b"de"
    assert read_bytes(ro.z, t, start=10) == b""


def test_read_refuses_replaced_file(ro):
    path = ro.workspace / "src" / "main.py"
    t = ro.mirror(FsRead, "/fs/read").open_read("src/main.py")["ticket"]
    (ro.workspace / "new.py").write_text("print('new')\n")
    os.replace(ro.workspace / "new.py", path)   # what editors do on save
    with pytest.raises(TicketError, match="FileChangedError.*replaced"):
        read_bytes(ro.z, t)


def test_read_refuses_truncated_file(ro):
    path = ro.workspace / "src" / "main.py"
    t = ro.mirror(FsRead, "/fs/read").open_read("src/main.py")["ticket"]
    os.truncate(path, 3)
    with pytest.raises(TicketError, match="FileChangedError.*truncated"):
        read_bytes(ro.z, t)


def test_read_follows_growth(ro):
    path = ro.workspace / "log.txt"
    path.write_bytes(b"one\n")
    t = ro.mirror(FsRead, "/fs/read").open_read("log.txt")["ticket"]
    assert read_bytes(ro.z, t) == b"one\n"
    with path.open("ab") as f:
        f.write(b"two\n")
    assert read_bytes(ro.z, t, start=4) == b"two\n"


def test_read_refuses_symlink_swapped_in(ro, tmp_path):
    (tmp_path / "secret").write_text("outside")
    t = ro.mirror(FsRead, "/fs/read").open_read("src/main.py")["ticket"]
    (ro.workspace / "src" / "main.py").unlink()
    (ro.workspace / "src" / "main.py").symlink_to(tmp_path / "secret")
    with pytest.raises(TicketError):
        read_bytes(ro.z, t)


@pytest.mark.parametrize("params", ["start=-1", "start=x", "start=5;end=2"])
def test_read_rejects_bad_params(ro, params):
    t = ro.mirror(FsRead, "/fs/read").open_read("src/main.py")["ticket"]
    with pytest.raises(TicketError, match="ValueError"):
        client.call(ro.z, f"{t}/read?{params}")


def test_read_ticket_is_reusable(ro):
    t = ro.mirror(FsRead, "/fs/read").open_read("src/main.py")["ticket"]
    assert read_bytes(ro.z, t) == read_bytes(ro.z, t)


def test_stream_in_order(ro):
    data = os.urandom(5 * 4096 + 123)
    (ro.workspace / "s.bin").write_bytes(data)
    handle = ro.mirror(FsRead, "/fs/read").open_read("s.bin")
    assert b"".join(client.stream(ro.z, handle["ticket"], chunk=4096)) == data


def test_stream_range(ro):
    data = os.urandom(10_000)
    (ro.workspace / "s.bin").write_bytes(data)
    handle = ro.mirror(FsRead, "/fs/read").open_read("s.bin")
    assert b"".join(client.stream(ro.z, handle["ticket"], start=777, chunk=4096)) == data[777:]
    assert b"".join(client.stream(ro.z, handle["ticket"], start=777, end=5000)) == data[777:5000]


def test_stream_empty_file(ro):
    (ro.workspace / "e.bin").write_bytes(b"")
    handle = ro.mirror(FsRead, "/fs/read").open_read("e.bin")
    assert list(client.stream(ro.z, handle["ticket"])) == []


def test_stream_raises_when_file_is_replaced(ro):
    path = ro.workspace / "s.bin"
    path.write_bytes(os.urandom(10_000))
    handle = ro.mirror(FsRead, "/fs/read").open_read("s.bin")
    path.unlink()
    path.write_bytes(os.urandom(20_000))
    with pytest.raises(TicketError, match="replaced"):
        list(client.stream(ro.z, handle["ticket"]))


def test_stream_raises_when_file_is_truncated(ro):
    path = ro.workspace / "s.bin"
    path.write_bytes(os.urandom(10_000))
    handle = ro.mirror(FsRead, "/fs/read").open_read("s.bin")
    os.truncate(path, 5000)
    with pytest.raises(TicketError, match="truncated"):
        list(client.stream(ro.z, handle["ticket"]))


def test_stream_follows_a_growing_file(share):
    # tail -f: stream to the end, wait, stream again from there. The ticket
    # outlives its ttl because the client heartbeats between streams.
    s = share(ttl=0.3)
    path = s.workspace / "log.bin"
    path.write_bytes(b"")
    t = s.mirror(FsRead, "/fs/read").open_read("log.bin")["ticket"]
    written, got = b"", b""
    with client.Heartbeat(s.z, t, 0.05):
        for _ in range(5):
            line = os.urandom(5000)
            with path.open("ab") as f:
                f.write(line)
            written += line
            got += b"".join(client.stream(s.z, t, start=len(got), chunk=4096, heartbeat=None))
            idle(s, 0.2)
    assert got == written


def test_read_ticket_expires(share):
    s = share(ttl=0.1)
    (s.workspace / "a.txt").write_text("a")
    t = s.mirror(FsRead, "/fs/read").open_read("a.txt")["ticket"]
    time.sleep(0.2)
    s.tickets.sweep()
    with pytest.raises(TicketError, match="no reply"):
        read_bytes(s.z, t, timeout=0.5)


def idle(share, seconds: float) -> None:
    """Let ``seconds`` pass, sweeping the share's tickets as its main loop would."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        time.sleep(0.05)
        share.tickets.sweep()


def test_heartbeat_keeps_read_ticket_alive(share):
    s = share(ttl=0.3)
    (s.workspace / "a.txt").write_text("a")
    handle = s.mirror(FsRead, "/fs/read").open_read("a.txt")
    assert handle["ttl"] == 0.3
    with client.Heartbeat(s.z, handle["ticket"], 0.05):
        idle(s, 1.0)
        assert read_bytes(s.z, handle["ticket"]) == b"a"


def test_read_close(ro):
    t = ro.mirror(FsRead, "/fs/read").open_read("src/main.py")["ticket"]
    client.close(ro.z, t)
    client.close(ro.z, t)
    with pytest.raises(TicketError, match="closed"):
        read_bytes(ro.z, t)
    with pytest.raises(TicketError, match="closed"):
        list(client.stream(ro.z, t, retries=0))


def stream_threads(stream_id: str) -> list:
    return [t for t in threading.enumerate() if t.name == f"stream-{stream_id}"]


def test_stream_stops_when_ticket_expires(share):
    # A stream doesn't keep its ticket alive: with no heartbeats, its client
    # is gone as far as the server knows, and the stream stops with the ticket.
    s = share(ttl=0.3)
    with (s.workspace / "big.bin").open("wb") as f:
        f.truncate(64 << 20)   # sparse: long to stream, cheap to make
    t = s.mirror(FsRead, "/fs/read").open_read("big.bin")["ticket"]
    received = []
    subscriber = s.z.declare_subscriber(f"{t}/data/x", lambda sample: received.append(len(sample.payload)))
    try:
        client.call(s.z, f"{t}/stream?id=x;chunk=65536;max_rate={1 << 20}")   # ~60 s to finish
        assert stream_threads("x")
        idle(s, 0.5)
        assert len(s.tickets) == 0
        assert wait_until(lambda: not stream_threads("x"), timeout=1)
    finally:
        subscriber.undeclare()
    assert sum(received) < 64 << 20


def wait_until(condition, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


def test_stream_heartbeats_while_running(share):
    s = share(ttl=0.3)
    data = os.urandom(64 * 4096)
    (s.workspace / "s.bin").write_bytes(data)
    t = s.mirror(FsRead, "/fs/read").open_read("s.bin")["ticket"]
    got = []
    for piece in client.stream(s.z, t, chunk=4096, buffer=4096, heartbeat=0.05):
        got.append(piece)
        if len(got) % 16 == 0:
            idle(s, 0.25)   # a slow consumer: the stream outlives the ttl
    assert b"".join(got) == data


def test_stream_max_rate(ro):
    data = os.urandom(128 << 10)
    (ro.workspace / "s.bin").write_bytes(data)
    t = ro.mirror(FsRead, "/fs/read").open_read("s.bin")["ticket"]
    start = time.monotonic()
    assert b"".join(client.stream(ro.z, t, chunk=8192, max_rate=256 << 10)) == data
    assert 0.4 < time.monotonic() - start < 2   # 128 KiB at 256 KiB/s


def test_stream_max_rate_shrinks_chunks(ro):
    (ro.workspace / "s.bin").write_bytes(os.urandom(32 << 10))
    t = ro.mirror(FsRead, "/fs/read").open_read("s.bin")["ticket"]
    sizes = [len(p) for p in client.stream(ro.z, t, max_rate=64 << 10)]
    assert sum(sizes) == 32 << 10 and max(sizes) <= 64 << 10


def test_paced_stream_stops_on_close(ro):
    (ro.workspace / "s.bin").write_bytes(os.urandom(1 << 20))
    t = ro.mirror(FsRead, "/fs/read").open_read("s.bin")["ticket"]
    subscriber = ro.z.declare_subscriber(f"{t}/data/slow", lambda s: None)
    try:
        client.call(ro.z, f"{t}/stream?id=slow;chunk=4096;max_rate=4096")
        assert stream_threads("slow")
        client.close(ro.z, t)
        assert wait_until(lambda: not stream_threads("slow"), timeout=1)
    finally:
        subscriber.undeclare()


def test_stream_rate_changes_while_running(ro):
    (ro.workspace / "s.bin").write_bytes(os.urandom(256 << 10))
    t = ro.mirror(FsRead, "/fs/read").open_read("s.bin")["ticket"]
    ended = threading.Event()
    subscriber = ro.z.declare_subscriber(
        f"{t}/data/x", lambda sample: client._attachment(sample)[0] == "end" and ended.set())
    try:
        client.call(ro.z, f"{t}/stream?id=x;chunk=4096;max_rate=16384")   # 16 s at this rate
        time.sleep(0.2)
        client.call(ro.z, f"{t}/stream?rate;id=x;max_rate=0")
        assert ended.wait(2)
    finally:
        subscriber.undeclare()


def counting_streams(monkeypatch) -> list:
    requests = []
    real = client._request

    def counting(z, selector, *args, **kwargs):
        if "/stream?" in selector and "stop" not in selector:
            requests.append(selector)
        return real(z, selector, *args, **kwargs)
    monkeypatch.setattr(client, "_request", counting)
    return requests


def test_slow_consumer_settles_at_its_own_rate(ro, monkeypatch):
    # Without rate control a 16 KiB buffer would overrun ~64 times on 1 MiB;
    # after the first, the stream runs a little under the consumer's pace.
    data = os.urandom(1 << 20)
    (ro.workspace / "s.bin").write_bytes(data)
    t = ro.mirror(FsRead, "/fs/read").open_read("s.bin")["ticket"]
    requests = counting_streams(monkeypatch)
    got = []
    for piece in client.stream(ro.z, t, chunk=4096, buffer=16 << 10):
        got.append(piece)
        time.sleep(0.002)   # ~2 MB/s
    assert b"".join(got) == data
    assert len(requests) <= 10
    assert any("max_rate=" in r for r in requests[1:])


def test_stream_max_rate_must_be_a_number(ro):
    t = ro.mirror(FsRead, "/fs/read").open_read("src/main.py")["ticket"]
    with pytest.raises(TicketError, match="ValueError"):
        client.call(ro.z, f"{t}/stream?id=x;max_rate=-1")


def test_paused_stream_holds_up_nothing_else(ro):
    data = os.urandom(4 << 20)
    (ro.workspace / "s.bin").write_bytes(data)
    t = ro.mirror(FsRead, "/fs/read").open_read("s.bin")["ticket"]
    pieces = client.stream(ro.z, t, chunk=4096, buffer=4 * 4096)
    got = [next(pieces)]
    time.sleep(0.3)   # paused: the buffer fills and overflows
    start = time.monotonic()
    assert read_bytes(ro.z, t, end=1, timeout=2) == data[:1]
    assert time.monotonic() - start < 0.5
    got.extend(pieces)   # and the stream still finishes, re-streamed past the overflow,
    assert b"".join(got) == data
    assert time.monotonic() - start < 3   # without the pause leaving it throttled


def test_abandoned_stream_returns_promptly(ro):
    (ro.workspace / "s.bin").write_bytes(os.urandom(4 << 20))
    t = ro.mirror(FsRead, "/fs/read").open_read("s.bin")["ticket"]
    pieces = client.stream(ro.z, t, chunk=4096, buffer=4096)
    next(pieces)
    time.sleep(0.2)   # the buffer fills and zenoh's callback waits on it
    start = time.monotonic()
    pieces.close()
    assert time.monotonic() - start < 2
    # and the session still receives: nothing is left stuck on the full buffer
    assert read_bytes(ro.z, t, end=1, timeout=2)


# ── Write tickets ───────────────────────────────────────────────────────────

def test_write_creates_file(rw):
    t = open_write(rw, "src/new.py")
    assert client.write_all(rw.z, t, b"x = 1\n") == 6
    assert (rw.workspace / "src" / "new.py").read_bytes() == b"x = 1\n"
    assert (rw.workspace / "src" / "new.py").stat().st_mode & 0o777 == 0o666 & ~current_umask()


def test_write_keeps_mode(rw):
    (rw.workspace / "existing.txt").chmod(0o750)
    client.write_all(rw.z, open_write(rw, "existing.txt"), b"new\n")
    assert (rw.workspace / "existing.txt").stat().st_mode & 0o777 == 0o750


def test_write_spans_chunks(rw):
    data = os.urandom(3 * CHUNK_SIZE + 17)
    client.write_all(rw.z, open_write(rw, "big.bin"), data)
    assert (rw.workspace / "big.bin").read_bytes() == data


def test_write_lands_without_close(rw):
    # No temp file: writes are in the file as soon as they're acked.
    t = open_write(rw, "a.txt")
    client.write_all(rw.z, t, b"abc", close=False)
    assert (rw.workspace / "a.txt").read_bytes() == b"abc"


def test_write_out_of_order_and_gaps(rw):
    t = open_write(rw, "ooo.txt")
    client.call(rw.z, f"{t}/write?offset=3", payload=b"def")
    client.call(rw.z, f"{t}/write?offset=0", payload=b"abc")
    client.call(rw.z, f"{t}/write?offset=8", payload=b"z")
    assert (rw.workspace / "ooo.txt").read_bytes() == b"abcdef\0\0z"


def test_write_into_existing_without_truncate(rw):
    t = open_write(rw, "existing.txt", truncate=False)
    client.write_all(rw.z, t, b"O")
    assert (rw.workspace / "existing.txt").read_bytes() == b"Old\n"


def test_append(rw):
    with client.RemoteFile(rw.z, open_write(rw, "existing.txt", truncate=False)) as f:
        f.append(b"one\n")
        f.append(b"two\n")
    assert (rw.workspace / "existing.txt").read_bytes() == b"old\none\ntwo\n"


def test_append_resend_is_not_written_twice(rw):
    t = open_write(rw, "log.txt")
    client.call(rw.z, f"{t}/append?seq=0", payload=b"a")
    client.call(rw.z, f"{t}/append?seq=0", payload=b"a")   # resend: ack lost
    client.call(rw.z, f"{t}/append?seq=1", payload=b"b")
    assert (rw.workspace / "log.txt").read_bytes() == b"ab"


def test_append_ahead_is_refused(rw):
    t = open_write(rw, "log.txt")
    with pytest.raises(TicketError, match="ahead"):
        client.call(rw.z, f"{t}/append?seq=1", payload=b"b")
    assert (rw.workspace / "log.txt").read_bytes() == b""


def test_append_lands_at_end_after_writes(rw):
    t = open_write(rw, "mix.txt")
    client.call(rw.z, f"{t}/write?offset=0", payload=b"head")
    client.call(rw.z, f"{t}/append?seq=0", payload=b"tail")
    assert (rw.workspace / "mix.txt").read_bytes() == b"headtail"


def test_truncate_shrinks_and_extends(rw):
    t = open_write(rw, "existing.txt", truncate=False)
    client.call(rw.z, f"{t}/truncate?size=2")
    assert (rw.workspace / "existing.txt").read_bytes() == b"ol"
    client.call(rw.z, f"{t}/truncate?size=2")   # repeating it is harmless
    client.call(rw.z, f"{t}/truncate?size=5")
    assert (rw.workspace / "existing.txt").read_bytes() == b"ol\0\0\0"


def test_truncate_lands_after_writes_in_flight(rw):
    with client.RemoteFile(rw.z, open_write(rw, "a.bin")) as f:
        for offset in range(0, 64 << 10, 4096):
            f.write(offset, os.urandom(4096))
        f.truncate(10)
    assert (rw.workspace / "a.bin").stat().st_size == 10


@pytest.mark.parametrize("params", ["", "size=-1", "size=x"])
def test_truncate_rejects_bad_size(rw, params):
    t = open_write(rw, "a.txt")
    with pytest.raises(TicketError, match="ValueError"):
        client.call(rw.z, f"{t}/truncate?{params}")


def test_truncate_after_close_is_refused(rw):
    t = open_write(rw, "a.txt")
    client.call(rw.z, f"{t}/close")
    with pytest.raises(TicketError, match="closed"):
        client.call(rw.z, f"{t}/truncate?size=0")


def test_flush(rw):
    with client.RemoteFile(rw.z, open_write(rw, "a.txt")) as f:
        f.write(0, b"abc")
        f.flush()
        assert (rw.workspace / "a.txt").read_bytes() == b"abc"


def test_close_is_repeatable_and_final(rw):
    t = open_write(rw, "a.txt")
    client.call(rw.z, f"{t}/close")
    client.call(rw.z, f"{t}/close")
    with pytest.raises(TicketError, match="closed"):
        client.call(rw.z, f"{t}/write?offset=0", payload=b"b")


def test_closed_ticket_retires(share):
    s = share(allow_write=True)
    t = open_write(s, "a.txt")
    client.call(s.z, f"{t}/close")
    s.tickets._tickets[t].retire_at = 0   # past its grace period
    s.tickets.sweep()
    assert len(s.tickets) == 0


def test_heartbeat_follows_the_servers_ttl(share):
    # No interval given: the first heartbeat's reply sets it, from the server's ttl.
    s = share(allow_write=True, ttl=0.3)
    with client.RemoteFile(s.z, open_write(s, "a.txt")) as f:
        idle(s, 1.0)
        f.write(0, b"late")
    assert (s.workspace / "a.txt").read_bytes() == b"late"


def test_stream_buffer_is_bytes(ro):
    # A chunk bigger than the buffer still gets through, one at a time.
    data = os.urandom(64 << 10)
    (ro.workspace / "s.bin").write_bytes(data)
    t = ro.mirror(FsRead, "/fs/read").open_read("s.bin")["ticket"]
    assert b"".join(client.stream(ro.z, t, chunk=16 << 10, buffer=1)) == data


def test_remote_file_heartbeats_while_open(share):
    s = share(allow_write=True, ttl=0.3)
    with client.RemoteFile(s.z, open_write(s, "a.txt"), heartbeat=0.05) as f:
        idle(s, 1.0)
        f.write(0, b"late")
    assert (s.workspace / "a.txt").read_bytes() == b"late"


def test_abandoned_write_keeps_what_landed(share):
    s = share(allow_write=True, ttl=0.1)
    t = open_write(s, "a.txt")
    client.call(s.z, f"{t}/write?offset=0", payload=b"partial")
    time.sleep(0.2)
    s.tickets.sweep()
    assert len(s.tickets) == 0
    assert (s.workspace / "a.txt").read_bytes() == b"partial"


def test_error_reply_fails_the_file(rw):
    f = client.RemoteFile(rw.z, open_write(rw, "a.txt"))
    client.call(rw.z, f"{f.ticket}/close")
    f.write(0, b"x")
    with pytest.raises(TicketError, match="closed"):
        f.wait()


# ── Faults ──────────────────────────────────────────────────────────────────

class Swallowed:
    """A query whose replies go nowhere: the handler ran, the ack is lost."""

    def __init__(self, query):
        self._query = query

    def __getattr__(self, name):
        return getattr(self._query, name)

    def reply(self, *args, **kwargs):
        pass

    def reply_err(self, *args, **kwargs):
        pass


def flaky(handler, rng, *, drop=0.2, lose_ack=0.2, duplicate=0.2, delay=0.02):
    """Wrap a ticket route: drop requests, run them but lose the ack, run them twice, delay them."""
    def wrapped(query):
        time.sleep(rng.random() * delay)
        roll = rng.random()
        if roll < drop:
            return
        if roll < drop + lose_ack:
            handler(Swallowed(query))
            return
        if roll < drop + lose_ack + duplicate:
            handler(Swallowed(query))
        handler(query)
    return wrapped


@pytest.fixture
def flaky_file(rw):
    from synq_expose.fs import FileWriteStream

    def make(path, seed):
        rng = random.Random(seed)
        stream = FileWriteStream(rw.workspace / path, truncate=True)
        routes = {name: flaky(h, rng) for name, h in stream.routes().items()}
        ticket = rw.tickets.mint("write", routes, on_revoke=stream.close)
        stream.ticket = ticket
        return client.RemoteFile(rw.z, ticket.key, timeout=0.1, retries=30, backoff=0.001)
    return make


@pytest.mark.parametrize("seed", range(5))
def test_writes_survive_faults(rw, flaky_file, seed):
    data = os.urandom(64 * 1024)
    with flaky_file("w.bin", seed) as f:
        for offset in range(0, len(data), 4096):
            f.write(offset, data[offset:offset + 4096])
    assert (rw.workspace / "w.bin").read_bytes() == data


@pytest.mark.parametrize("seed", range(5))
def test_appends_survive_faults(rw, flaky_file, seed):
    lines = [f"line {i}\n".encode() for i in range(30)]
    with flaky_file("log.txt", seed) as f:
        for line in lines:
            f.append(line)
    assert (rw.workspace / "log.txt").read_bytes() == b"".join(lines)


class FlakyPublisher:
    """A stream's publisher that drops, duplicates, reorders and delays samples, the end marker included."""

    def __init__(self, publisher, rng, *, drop=0.1, duplicate=0.1, reorder=0.1, delay=0.002):
        self._publisher = publisher
        self._rng = rng
        self._held = None
        self.odds = drop, duplicate, reorder, delay

    def put(self, *args, **kwargs):
        drop, duplicate, reorder, delay = self.odds
        time.sleep(self._rng.random() * delay)
        roll = self._rng.random()
        if roll < drop:
            return
        if roll < drop + reorder and self._held is None:
            self._held = (args, kwargs)   # sent after the next one
            return
        self._publisher.put(*args, **kwargs)
        if roll < drop + reorder + duplicate:
            self._publisher.put(*args, **kwargs)
        if self._held is not None:
            held, self._held = self._held, None
            self._publisher.put(*held[0], **held[1])

    def undeclare(self):
        self._publisher.undeclare()


@pytest.fixture
def flaky_reader(ro):
    from synq_expose.fs import FileReadStream

    def make(path, seed):
        rng = random.Random(seed)
        target = ro.workspace / path
        stream = FileReadStream(target, target.stat(), ro.tickets.session)
        publish = stream._publish
        stream._publish = lambda publisher, *a: publish(FlakyPublisher(publisher, rng), *a)
        routes = {name: flaky(h, rng) for name, h in stream.routes().items()}
        ticket = ro.tickets.mint("read", routes, on_revoke=stream.close)
        stream.ticket = ticket
        wait_for(ro.z, f"{ticket.key}/stream")
        return ticket.key
    return make


@pytest.mark.parametrize("seed", range(5))
def test_streams_survive_faults(ro, flaky_reader, seed):
    data = os.urandom(64 * 1024)
    (ro.workspace / "s.bin").write_bytes(data)
    t = flaky_reader("s.bin", seed)
    got = client.stream(ro.z, t, chunk=4096, timeout=0.2, retries=30, backoff=0.001, heartbeat=None)
    assert b"".join(got) == data


@pytest.mark.parametrize("seed", range(3))
def test_stream_ranges_survive_faults(ro, flaky_reader, seed):
    data = os.urandom(40_000)
    (ro.workspace / "s.bin").write_bytes(data)
    t = flaky_reader("s.bin", seed)
    got = client.stream(ro.z, t, start=1234, end=30_000, chunk=4096, timeout=0.2, retries=30, backoff=0.001,
                        heartbeat=None)
    assert b"".join(got) == data[1234:30_000]


def test_stream_gives_up_when_nothing_answers(ro):
    with pytest.raises(client.NoReply):
        list(client.stream(ro.z, "node/tickets/@nothing-here", timeout=0.1, retries=2, backoff=0.001,
                           heartbeat=None))


def test_files_share_the_write_pool(rw):
    for i in range(20):
        client.write_all(rw.z, open_write(rw, f"f{i}.txt"), os.urandom(64 << 10), chunk_size=4096)
    writers = [t for t in threading.enumerate() if t.name.startswith("write")]
    assert 0 < len(writers) <= client.WRITE_WORKERS


def test_abandoned_file_frees_its_workers(rw):
    # Nothing answers, so each write would resend for ~5 s. Abandoned, it stops at its next resend.
    f = client.RemoteFile(rw.z, "node/tickets/@nothing-here", timeout=0.05, retries=100, backoff=0.001,
                          heartbeat=None)
    for offset in range(4):
        f.write(offset, b"x")
    f._abandon()
    assert wait_until(lambda: not f._pending, timeout=1)


def test_write_gives_up_when_nothing_answers(rw):
    f = client.RemoteFile(rw.z, "node/tickets/@nothing-here", timeout=0.1, retries=2)
    f.write(0, b"x")
    with pytest.raises(client.NoReply):
        f.wait()


# ── Directory operations ────────────────────────────────────────────────────

def test_list_dir_hides_excluded(ro):
    entries = list(ro.mirror(FsRead, "/fs/read").list_dir("."))
    assert [e["name"] for e in entries] == ["src"]
    assert entries[0]["type"] == "dir"
    assert list(ro.mirror(FsRead, "/fs/read").list_dir("src")) == [{
        "name": "main.py", **ro.mirror(FsRead, "/fs/read").stat("src/main.py")}]


def test_list_dir_hides_excluded_directories(share):
    s = share(exclude=("secrets/",))
    (s.workspace / "secrets").mkdir()
    (s.workspace / "secrets" / "k").write_text("x")
    fs = s.mirror(FsRead, "/fs/read")
    assert list(fs.list_dir(".")) == []
    with pytest.raises(RpcException, match="not exposed"):
        list(fs.list_dir("secrets"))


def test_list_dir_refuses_a_file(ro):
    with pytest.raises(RpcException):
        list(ro.mirror(FsRead, "/fs/read").list_dir("src/main.py"))


def test_stat(ro):
    (ro.workspace / "src" / "main.py").chmod(0o640)
    st = ro.mirror(FsRead, "/fs/read").stat("src/main.py")
    assert st == {"type": "file", "size": 12, "mode": 0o640,
                  "mtime": (ro.workspace / "src" / "main.py").stat().st_mtime}
    assert ro.mirror(FsRead, "/fs/read").stat("src")["type"] == "dir"


def test_stat_is_lstat(ro):
    (ro.workspace / "link").symlink_to("/etc/passwd")
    assert ro.mirror(FsRead, "/fs/read").stat("link")["type"] == "symlink"
    with pytest.raises(RpcException, match="symlink"):
        ro.mirror(FsRead, "/fs/read").open_read("link")


def test_stat_missing(ro):
    with pytest.raises(RpcException):
        ro.mirror(FsRead, "/fs/read").stat("nope")


def test_glob_skips_excluded(ro):
    (ro.workspace / "src" / "x.pem").write_text("key")
    assert list(ro.mirror(FsRead, "/fs/read").glob(".", "**/*")) == ["src", "src/main.py"]


def test_glob_is_relative_to_path(ro):
    (ro.workspace / "src" / "pkg").mkdir()
    (ro.workspace / "src" / "pkg" / "a.py").write_text("")
    (ro.workspace / "top.py").write_text("")
    fs = ro.mirror(FsRead, "/fs/read")
    assert list(fs.glob("src", "*.py")) == ["src/main.py"]
    assert list(fs.glob("src", "**/*.py")) == ["src/main.py", "src/pkg/a.py"]
    assert list(fs.glob(".", "*.py")) == ["top.py"]


def test_glob_doesnt_follow_symlinks(ro, tmp_path_factory):
    outside = tmp_path_factory.mktemp("outside")
    (outside / "o.py").write_text("")
    (ro.workspace / "out").symlink_to(outside)
    assert list(ro.mirror(FsRead, "/fs/read").glob(".", "**/*.py")) == ["src/main.py"]


def test_grep(ro):
    matches = list(ro.mirror(FsRead, "/fs/read").grep(".", "print"))
    assert matches == [{"path": "src/main.py", "line": 1, "text": "print('hi')"}]


def test_grep_skips_excluded(ro):
    assert list(ro.mirror(FsRead, "/fs/read").grep(".", "SECRET")) == []


def test_grep_regex_lines_and_binary(ro):
    (ro.workspace / "src" / "a.txt").write_bytes(b"one\ntwo\nthree\xff\n")
    (ro.workspace / "src" / "b.bin").write_bytes(b"two\0")
    matches = list(ro.mirror(FsRead, "/fs/read").grep("src", r"^t"))
    assert matches == [{"path": "src/a.txt", "line": 2, "text": "two"},
                       {"path": "src/a.txt", "line": 3, "text": "three�"}]


def test_grep_one_file(ro):
    assert [m["line"] for m in ro.mirror(FsRead, "/fs/read").grep("src/main.py", "hi")] == [1]


def test_grep_bad_regex(ro):
    with pytest.raises(RpcException):
        list(ro.mirror(FsRead, "/fs/read").grep(".", "("))


def test_edit_file_replaces_exactly_one(rw):
    (rw.workspace / "e.txt").write_text("a b a")
    fs = rw.mirror(FsWrite, "/fs/write")
    with pytest.raises(RpcException, match="2 times"):
        fs.edit_file("e.txt", "a", "c")
    with pytest.raises(RpcException, match="0 times"):
        fs.edit_file("e.txt", "z", "c")
    fs.edit_file("e.txt", "b", "c")
    assert (rw.workspace / "e.txt").read_text() == "a c a"
    assert files(rw.workspace) == {"src", ".git", "existing.txt", "e.txt"}   # no temp file left


def test_edit_file_renames_over_and_keeps_mode(rw):
    path = rw.workspace / "e.txt"
    path.write_text("hello")
    path.chmod(0o751)
    t = rw.mirror(FsRead, "/fs/read").open_read("e.txt")["ticket"]
    before = path.stat().st_ino
    rw.mirror(FsWrite, "/fs/write").edit_file("e.txt", "hello", "bye")
    assert path.stat().st_ino != before and path.stat().st_mode & 0o777 == 0o751
    with pytest.raises(TicketError, match="replaced"):   # as for a reader of a file sed -i rewrote
        read_bytes(rw.z, t)


def test_edit_file_refuses_readonly(rw):
    (rw.workspace / ".git" / "HEAD").write_text("ref")
    with pytest.raises(RpcException, match="read-only"):
        rw.mirror(FsWrite, "/fs/write").edit_file(".git/HEAD", "ref", "x")


def test_chmod(rw):
    fs = rw.mirror(FsWrite, "/fs/write")
    fs.chmod("existing.txt", 0o700)
    assert (rw.workspace / "existing.txt").stat().st_mode & 0o7777 == 0o700
    with pytest.raises(RpcException, match="permission bits"):
        fs.chmod("existing.txt", 0o10000)
    with pytest.raises(RpcException, match="read-only"):
        fs.chmod(".git", 0o777)


def test_open_write_exclusive(rw):
    fs = rw.mirror(FsWrite, "/fs/write")
    with pytest.raises(RpcException, match="exists"):
        fs.open_write("existing.txt", truncate=False, exclusive=True)
    assert (rw.workspace / "existing.txt").read_bytes() == b"old\n"
    client.write_all(rw.z, fs.open_write("fresh.txt", truncate=False, exclusive=True)["ticket"], b"x")
    assert (rw.workspace / "fresh.txt").read_bytes() == b"x"


def test_mkdir_and_delete(rw):
    fs = rw.mirror(FsWrite, "/fs/write")
    fs.mkdir("a/b", parents=True)
    fs.mkdir("a/b", parents=True)          # mkdir -p: already there is fine
    assert (rw.workspace / "a" / "b").is_dir()
    with pytest.raises(RpcException):
        fs.mkdir("a/b")                    # plain mkdir: it exists
    with pytest.raises(RpcException):
        fs.mkdir("x/y")                    # plain mkdir: no parent
    with pytest.raises(RpcException):
        fs.delete("a")                     # not empty
    fs.delete("a", recursive=True)
    assert not (rw.workspace / "a").exists()


def test_delete_file_and_symlink(rw, tmp_path_factory):
    outside = tmp_path_factory.mktemp("outside") / "keep"
    outside.write_text("x")
    (rw.workspace / "link").symlink_to(outside)
    fs = rw.mirror(FsWrite, "/fs/write")
    fs.delete("existing.txt")
    fs.delete("link")                      # the link goes, not what it points to
    assert files(rw.workspace) == {"src", ".git"}
    assert outside.read_text() == "x"


def test_delete_recursive_doesnt_follow_symlinks(rw, tmp_path_factory):
    outside = tmp_path_factory.mktemp("outside")
    (outside / "keep").write_text("x")
    (rw.workspace / "src" / "link").symlink_to(outside)
    rw.mirror(FsWrite, "/fs/write").delete("src", recursive=True)
    assert not (rw.workspace / "src").exists()
    assert (outside / "keep").read_text() == "x"


def test_delete_refuses_readonly(rw):
    fs = rw.mirror(FsWrite, "/fs/write")
    with pytest.raises(RpcException, match="read-only"):
        fs.delete(".git", recursive=True)
    assert (rw.workspace / ".git").is_dir()


def test_delete_refuses_trees_holding_hidden_or_readonly_paths(rw):
    (rw.workspace / "src" / ".env").write_text("SECRET=1")
    fs = rw.mirror(FsWrite, "/fs/write")
    with pytest.raises(RpcException, match="aren't exposed"):
        fs.delete("src", recursive=True)
    assert (rw.workspace / "src" / ".env").exists()


def test_delete_refuses_the_root(rw):
    with pytest.raises(RpcException, match="root"):
        rw.mirror(FsWrite, "/fs/write").delete(".", recursive=True)


@pytest.mark.parametrize("readahead", ["auto", "on", "off"])
def test_readahead_modes_read_the_same(ro, monkeypatch, readahead):
    from synq_expose import fs as fs_module

    monkeypatch.setattr(fs_module, "MIN_CHUNK", 1024)
    data = os.urandom(20 * 1024 + 7)
    (ro.workspace / "m.bin").write_bytes(data)
    t = ro.mirror(FsRead, "/fs/read").open_read("m.bin")["ticket"]
    assert read_bytes(ro.z, t, chunk=1024, readahead=readahead) == data
    assert b"".join(client.stream(ro.z, t, chunk=1024, readahead=readahead)) == data


def test_readahead_rejects_unknown_mode(ro):
    t = ro.mirror(FsRead, "/fs/read").open_read("src/main.py")["ticket"]
    with pytest.raises(TicketError, match="readahead"):
        read_bytes(ro.z, t, readahead="sometimes")


def test_auto_readahead_starts_after_a_few_chunks(ro, monkeypatch):
    from synq_expose import fs as fs_module
    from synq_expose.fs import FileReadStream

    started = []
    real = fs_module.read_ahead
    monkeypatch.setattr(fs_module, "read_ahead", lambda src, depth: started.append(True) or real(src, depth))
    monkeypatch.setattr(fs_module, "MIN_CHUNK", 1024)
    path = ro.workspace / "g.bin"
    path.write_bytes(os.urandom(3 * 1024))   # short: no read-ahead
    stream = FileReadStream(path, path.stat())
    assert len(list(stream._chunks(0, None, 1024))) == 3 and started == []
    path.write_bytes(os.urandom(10 * 1024))  # longer: read-ahead after READ_AHEAD_AFTER chunks
    stream = FileReadStream(path, path.stat())
    assert len(list(stream._chunks(0, None, 1024))) == 10 and started == [True]


def test_read_ahead_stops_and_waits_when_abandoned():
    from synq_expose.fs import read_ahead

    produced = []

    def source():
        for i in range(1000):
            produced.append(i)
            yield i

    items = read_ahead(source(), 2)
    assert [next(items), next(items)] == [0, 1]
    items.close()
    # The producer has finished: nothing more is produced after close returns.
    count = len(produced)
    time.sleep(0.05)
    assert len(produced) == count < 10


def test_read_ahead_passes_errors_through():
    from synq_expose.fs import read_ahead

    def source():
        yield 1
        raise OSError("disk on fire")

    items = read_ahead(source(), 2)
    assert next(items) == 1
    with pytest.raises(OSError, match="disk on fire"):
        next(items)


def test_rename_replaces_the_destination(rw):
    fs = rw.mirror(FsWrite, "/fs/write")
    t = rw.mirror(FsRead, "/fs/read").open_read("existing.txt")["ticket"]
    client.write_all(rw.z, fs.open_write("src/.existing.tmp", truncate=False, exclusive=True)["ticket"], b"new\n")
    fs.rename("src/.existing.tmp", "existing.txt")   # the safe rewrite: new file, then rename over
    assert (rw.workspace / "existing.txt").read_bytes() == b"new\n"
    assert files(rw.workspace) == {"src", ".git", "existing.txt"}
    with pytest.raises(TicketError, match="replaced"):
        read_bytes(rw.z, t)


def test_rename_moves_directories_and_symlinks(rw):
    (rw.workspace / "src" / "a.py").write_text("x")
    (rw.workspace / "link").symlink_to("/etc")
    fs = rw.mirror(FsWrite, "/fs/write")
    fs.rename("src", "lib")
    fs.rename("link", "lib/link")
    assert files(rw.workspace) == {"lib", "lib/a.py", "lib/link", ".git", "existing.txt"}
    assert (rw.workspace / "lib" / "link").readlink().as_posix() == "/etc"


@pytest.mark.parametrize("path, to, error", [
    ("existing.txt", ".git/x", "read-only"),
    (".git", "git", "read-only"),
    ("existing.txt", ".env", "not exposed"),
    ("existing.txt", ".", "root"),
    ("existing.txt", "../out", "invalid"),
    ("missing", "x", "No such file"),
])
def test_rename_refuses(rw, path, to, error):
    with pytest.raises(RpcException, match=error):
        rw.mirror(FsWrite, "/fs/write").rename(path, to)


def test_rename_refuses_a_tree_holding_readonly_paths(share):
    s = share(allow_write=True, readonly_paths=("*.lock",))
    (s.workspace / "a").mkdir()
    (s.workspace / "a" / "uv.lock").write_text("")
    with pytest.raises(RpcException, match="read-only"):
        s.mirror(FsWrite, "/fs/write").rename("a", "b")
    assert (s.workspace / "a" / "uv.lock").exists()



@pytest.mark.parametrize("name", [
    "a*b?c$d#e=f&g h",
    pytest.param("a;b", marks=pytest.mark.xfail(
        strict=True, reason="SpiriSynq passes RPC arguments as selector parameters without escaping ';'")),
])
def test_names_with_odd_characters(rw, name):
    path = f"src/{name}"
    fs = rw.mirror(FsWrite, "/fs/write")
    client.write_all(rw.z, fs.open_write(path, truncate=True)["ticket"], b"x")
    assert list(rw.mirror(FsRead, "/fs/read").glob("src", "*")) == [path]
    assert rw.mirror(FsRead, "/fs/read").stat(path)["size"] == 1
