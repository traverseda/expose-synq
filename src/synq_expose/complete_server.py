"""The tab-completion server, ``expose-synq _complete-server``: one warm process that answers Tabs.

It holds a zenoh session, keeps the list of shares current by asking every
``SCAN_EVERY`` seconds (and writes it to the cache for the fallback), and
lists a share's directories with its FsRead's ``list_dir``. A Tab gets an
answer within ``DEADLINE`` seconds: a listing that isn't back by then is
left to finish for the next Tab, and listings are kept ``LISTING_TTL``
seconds. It only ever looks, so a share's audit log shows the ``list_dir``
calls of your Tabs like any other.

Requests are one line, tab-separated: protocol, token, shell, the
``$EXPOSE_SYNQ_TOPIC`` of the shell, bash's current word, and the line up
to the cursor. The reply is what completion.answer returns. Over the Unix
socket, which only its owner can open, the token is ignored; over TCP it
must match the one in ``complete.tcp``. Requests are answered one at a
time.

One server runs per runtime directory (a lock on ``complete.lock``, which
holds its pid while it runs), and it exits after
``idle`` seconds without a request, on SIGTERM, or after a request once
its zenoh config or its own code has changed, so the next Tab starts one
that's up to date.
"""
import fcntl
import hmac
import os
import secrets
import select
import signal
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from pathlib import Path

from loguru import logger

from synq_expose import completion

IDLE = 600.0
DEADLINE = 0.3
SCAN_EVERY = 5.0
FIRST_SCAN = 1.5
"""Seconds the first scan listens: a new session needs a moment to learn what's out there."""
LISTING_TTL = 5.0
RPC_TIMEOUT = 5.0

SUFFIX = "/fs/read/sr_metadata/FsRead"
"""What follows a share's topic in the key of its FsRead metadata (workspace.FS_READ, then SpiriSynq's)."""


def scan(z, window: float) -> set[str]:
    """Every share's topic that answers within ``window`` seconds, asking again as the session learns routes."""
    import zenoh
    found: set[str] = set()
    deadline = time.monotonic() + window
    while (remaining := deadline - time.monotonic()) > 0:
        for reply in z.get(f"**{SUFFIX}", consolidation=zenoh.ConsolidationMode.NONE, timeout=remaining):
            if reply.ok:
                found.add(str(reply.ok.key_expr).removesuffix(SUFFIX))
        time.sleep(0.1)
    return found


class LiveSource:
    """What the completion callbacks complete from, inside the server."""

    def __init__(self, session, cache: Path):
        self.session = session
        self.z = session.zenoh_session
        self.cache = cache
        self.default: str | None = None
        self.deadline = 0.0
        self._topics = set(completion.read_topics(cache))
        self._listings: dict[tuple[str, str], tuple[float, object]] = {}
        self._mirrors: dict[str, object] = {}
        self._mirrors_lock = threading.Lock()
        self._pool = ThreadPoolExecutor(4, thread_name_prefix="list_dir")
        self._stop = threading.Event()
        self._scanner = threading.Thread(target=self._scan_loop, name="scan", daemon=True)
        self._scanner.start()

    def topics(self) -> list[str]:
        return sorted(self._topics)

    def default_topic(self) -> str | None:
        return self.default

    def paths(self, topic: str, partial: str) -> list[str]:
        """``partial`` completed against its directory's listing: ``src/a`` → ``src/app.py``, ``src/apps/``.

        Dotfiles only when ``partial``'s last part starts with a dot, as shells do.
        """
        name = partial.rpartition("/")[2]
        prefix = partial[:len(partial) - len(name)]
        directory = prefix.rstrip("/") or ("/" if prefix else ".")
        return [prefix + e["name"] + ("/" if e["type"] == "dir" else "")
                for e in self.listing(topic, directory)
                if e["name"].startswith(name) and (name.startswith(".") or not e["name"].startswith("."))]

    def listing(self, topic: str, directory: str) -> list[dict]:
        """The entries of ``directory`` on ``topic``, or none if they aren't back by the request's deadline."""
        key = (topic, directory)
        now = time.monotonic()
        cached = self._listings.get(key)
        if cached is None or cached[0] < now:
            cached = (now + LISTING_TTL, self._pool.submit(self._list_dir, topic, directory))
            self._listings[key] = cached
        try:
            return cached[1].result(timeout=max(0.0, self.deadline - time.monotonic()))
        except FutureTimeout:
            return []
        except Exception as e:      # no share there, a remote error: nothing to offer
            logger.debug(f"list_dir {topic}:{directory}: {e!r}")
            return []

    def _list_dir(self, topic: str, directory: str) -> list[dict]:
        from synq_expose.fs import FsRead
        from synq_expose.workspace import FS_READ
        with self._mirrors_lock:
            mirror = self._mirrors.get(topic)
            if mirror is None:
                with self.session.as_default():     # a worker thread doesn't inherit the context's session
                    mirror = self._mirrors[topic] = FsRead.from_topic(f"{topic}/{FS_READ}")
        return list(mirror.list_dir.timeout(RPC_TIMEOUT)(directory))

    def _scan_loop(self) -> None:
        window = FIRST_SCAN
        while not self._stop.is_set():
            try:
                found = scan(self.z, window)
            except Exception as e:
                logger.warning(f"scanning for shares: {e!r}")
            else:
                if found != self._topics or not self.cache.exists():
                    self._topics = found
                    completion.write_topics(self.cache, found)
            window = 0.5
            self._stop.wait(SCAN_EVERY)

    def close(self) -> None:
        self._stop.set()
        self._pool.shutdown(wait=False, cancel_futures=True)
        for mirror in self._mirrors.values():
            mirror.close()


def mtimes(paths) -> list[float | None]:
    out = []
    for path in paths:
        try:
            out.append(path.stat().st_mtime)
        except FileNotFoundError:
            out.append(None)
    return out


def serve(zenoh_config, watch: list[Path] = (), idle: float = IDLE, run: Path | None = None) -> None:
    """Answer Tabs until idle. Returns at once if another server holds this runtime directory."""
    run = run or completion.runtime_dir()
    run.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = open(run / "complete.lock", "a+")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return
    lock.truncate(0)
    lock.write(f"{os.getpid()}\n")
    lock.flush()

    # Imported once the lock is ours: SpiriSynq opens a session on import (#243).
    import typer
    from SpiriSynq.session import current_session
    from synq_expose.cli import app
    from synq_expose.config import reopen_session

    watch = [*watch, Path(completion.__file__), Path(__file__)]
    before = mtimes(watch)
    session = current_session.get()
    reopen_session(session, zenoh_config)
    live = completion.source = LiveSource(session, completion.cache_path())
    cli = typer.main.get_command(app)
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(0))

    sock_path, tcp_path = run / "complete.sock", run / "complete.tcp"
    listeners = []
    unix = socket.socket(socket.AF_UNIX)
    try:
        sock_path.unlink(missing_ok=True)
        unix.bind(str(sock_path))
        os.chmod(sock_path, 0o600)
        unix.listen()
        listeners.append(unix)
    except OSError as e:            # a path too long for a Unix socket: bash's TCP still works
        logger.warning(f"no Unix socket at {sock_path}: {e}")
        unix.close()
    tcp = socket.socket()
    tcp.bind(("127.0.0.1", 0))
    tcp.listen()
    listeners.append(tcp)
    token = secrets.token_hex(16)
    tmp = run / f".complete.tcp.{os.getpid()}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"{tcp.getsockname()[1]} {token}\n")
    os.replace(tmp, tcp_path)

    try:
        last = time.monotonic()
        while (left := last + idle - time.monotonic()) > 0:
            ready, _, _ = select.select(listeners, [], [], left)
            for listener in ready:
                conn, _ = listener.accept()
                with conn:
                    handle(conn, cli, live, token if listener is tcp else None)
                last = time.monotonic()
            if ready and mtimes(watch) != before:
                logger.info("config or code changed; exiting")
                break
    finally:
        for listener in listeners:
            listener.close()
        if unix in listeners:
            sock_path.unlink(missing_ok=True)
        tcp_path.unlink(missing_ok=True)
        lock.truncate(0)            # a pid in the lock file is a running server
        live.close()
        session.close()


def handle(conn: socket.socket, cli, live: LiveSource, token: str | None) -> None:
    """Answer one request. A bad one is dropped, so the shell gets nothing."""
    conn.settimeout(1.0)
    data = b""
    try:
        while not data.endswith(b"\n") and len(data) < 65536:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
        protocol, their_token, shell, default, cur, line = data.decode().rstrip("\n").split("\t", 5)
    except (OSError, UnicodeDecodeError, ValueError):
        return
    if protocol != completion.PROTOCOL or (token is not None and not hmac.compare_digest(their_token, token)):
        return
    live.default = default or None
    live.deadline = time.monotonic() + DEADLINE
    try:
        reply = completion.answer(cli, shell, line, cur)
    except Exception as e:
        logger.exception(f"completing {line!r}: {e!r}")
        return
    try:
        conn.sendall((reply + "\n").encode() if reply else b"")
    except OSError:
        pass
