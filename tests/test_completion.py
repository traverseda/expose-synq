import fcntl
import json
import os
import pty
import re
import select
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import termios
import time
from pathlib import Path

import pytest
import typer

from synq_expose import completion
from synq_expose.cli import app
from synq_expose.complete_server import SUFFIX, LiveSource
from synq_expose.workspace import FS_READ

TOPICS = ["host/proj", "host/other", "box/a:b"]
CLI = typer.main.get_command(app)


@pytest.fixture(autouse=True)
def started(monkeypatch):
    """In this process, the fallback never starts a real server; it's recorded instead."""
    calls = []
    monkeypatch.setattr(completion, "start_server", lambda *a: calls.append(a))
    monkeypatch.delenv("EXPOSE_SYNQ_TOPIC", raising=False)
    return calls


@pytest.fixture
def cache(tmp_path, monkeypatch):
    """The topics cache the fallback reads, under a private XDG_CACHE_HOME."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    path = completion.cache_path()
    completion.write_topics(path, TOPICS)
    return path


@pytest.fixture
def short_dir():
    """A directory with a path short enough for a Unix socket (107 bytes), which tmp_path may not be."""
    path = Path(tempfile.mkdtemp(prefix="esq", dir="/tmp"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


def test_suffix_follows_the_share_layout():
    assert SUFFIX.startswith(f"/{FS_READ}/sr_metadata/")


# ── Callbacks ───────────────────────────────────────────────────────────

def test_topic_completes_from_the_cache(cache):
    assert completion.complete_topic("host/") == ["host/other", "host/proj"]
    assert completion.complete_topic("box/a:") == ["box/a:b"]


def test_remote_without_the_server_completes_the_topic_part_only(cache):
    assert completion.complete_remote("ho") == ["host/other:", "host/proj:"]
    assert completion.complete_remote("") == ["host/other:", "host/proj:"]   # box/a:b can't be TOPIC:PATH
    assert completion.complete_remote("host/proj:") == []
    assert completion.complete_remote("./x") == []


class FakeSource:
    def __init__(self, default=None):
        self.default = default
        self.asked = []

    def topics(self):
        return ["host/proj"]

    def default_topic(self):
        return self.default

    def paths(self, topic, partial):
        self.asked.append((topic, partial))
        return [p for p in ("src/", "setup.py", "./x") if p.startswith(partial) and p.startswith(".") == partial.startswith(".")]


@pytest.mark.parametrize("default, incomplete, expected, asked", [
    (None, "host/proj:s", ["host/proj:src/", "host/proj:setup.py"], [("host/proj", "s")]),
    (None, "host/proj/:", ["host/proj/:src/", "host/proj/:setup.py"], [("host/proj", "")]),
    (None, ":s", [], []),                                                      # no topic at all
    ("dflt", ":s", [":src/", ":setup.py"], [("dflt", "s")]),
    ("dflt", "./", ["./x"], [("dflt", "./")]),
    ("dflt", "s", ["src/", "setup.py"], [("dflt", "s")]),
    ("dflt", "h", ["host/proj:"], [("dflt", "h")]),
])
def test_remote_splits_as_files_split(monkeypatch, default, incomplete, expected, asked):
    fake = FakeSource(default)
    monkeypatch.setattr(completion, "source", fake)
    assert completion.complete_remote(incomplete) == expected
    assert fake.asked == asked


# ── Shell classes ───────────────────────────────────────────────────────

def bash(line: str, cur: str) -> list[str]:
    """What the bash class answers for ``line``, whose last word bash split as ``cur``."""
    return completion.answer(CLI, "bash", line, cur).splitlines()


def zsh(line: str) -> str:
    return completion.answer(CLI, "zsh", line)


def test_bash_completes_commands_and_topics(cache, started):
    assert bash("expose-synq mo", "mo") == ["mount"]
    assert bash("expose-synq mount ho", "ho") == ["host/other", "host/proj"]
    assert bash("expose-synq run --topic host/p", "host/p") == ["host/proj"]
    assert bash("expose-synq file push local ho", "ho") == ["host/other:", "host/proj:"]
    assert not started                  # the server itself answers these


def test_bash_answers_relative_to_its_word_after_a_colon(monkeypatch):
    monkeypatch.setattr(completion, "source", FakeSource())
    # bash splits "host/proj:s" into "host/proj" ":" "s", and replaces only the "s".
    assert bash("expose-synq file ls host/proj:s", "s") == ["src/", "setup.py"]
    assert bash("expose-synq file ls host/proj:", ":") == [":src/", ":setup.py"]


def test_bash_fallback_reads_the_environment_and_starts_the_server(cache, monkeypatch, started):
    monkeypatch.setenv("COMP_LINE", "expose-synq mount ho")
    monkeypatch.setenv("COMP_CUR", "ho")
    assert completion.BashComplete(CLI, {}, "expose-synq", completion.COMPLETE_VAR).complete().splitlines() == \
        ["host/other", "host/proj"]
    assert started


def test_zsh_adds_no_space_after_a_topic_or_a_directory(monkeypatch):
    monkeypatch.setattr(completion, "source", FakeSource())
    assert zsh("expose-synq file ls ho") == "compadd -p '' -S '' -- host/proj:"
    assert zsh("expose-synq file ls host/proj:s") == \
        "compadd -p host/proj: -S '' -- src/; compadd -p host/proj: -- setup.py"
    assert zsh("expose-synq mount host/p").startswith("_arguments")


def test_zsh_offers_no_local_files_for_a_remote_path(monkeypatch):
    monkeypatch.setattr(completion, "source", FakeSource())
    assert zsh("expose-synq file ls host/proj:nothing") == ""
    assert zsh("expose-synq file push nothing") == "_files"           # a local path still gets them


def test_completion_prints_each_script():
    for shell in completion.Shell:
        assert "_EXPOSE_SYNQ_COMPLETE" in completion.script(shell)


# ── Starting the server ─────────────────────────────────────────────────

def test_start_server_starts_one(short_dir, monkeypatch):
    monkeypatch.undo()                  # the real start_server
    spawned = []
    monkeypatch.setattr(completion.subprocess, "Popen", lambda argv, **kwargs: spawned.append(argv))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(short_dir))
    completion.start_server()
    completion.start_server()
    assert spawned == [[sys.executable, "-m", "synq_expose", "_complete-server"]]
    assert (short_dir / "expose-synq").stat().st_mode & 0o777 == 0o700


def test_start_server_leaves_a_running_one_alone(short_dir, monkeypatch):
    monkeypatch.undo()
    monkeypatch.setattr(completion.subprocess, "Popen", lambda argv, **kwargs: pytest.fail("started another"))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(short_dir))
    run = short_dir / "expose-synq"
    run.mkdir()
    with socket.socket(socket.AF_UNIX) as s:
        s.bind(str(run / "complete.sock"))
        s.listen()
        completion.start_server()


# ── The server's source, against a real share ───────────────────────────

def files(workspace: Path) -> None:
    (workspace / "src" / "apps").mkdir(parents=True)
    for name in ("src/app.py", "src/.hidden", "README.md"):
        (workspace / name).touch()


def eventually(fn, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not (result := fn()) and time.monotonic() < deadline:
        time.sleep(0.05)
    return result


def test_live_source_lists_topics_and_paths(network, share, tmp_path):
    s = share()
    files(s.workspace)
    live = LiveSource(network.client, tmp_path / "topics")
    try:
        assert eventually(lambda: s.topic in live.topics())
        assert s.topic in completion.read_topics(tmp_path / "topics")
        live.deadline = time.monotonic() + 10
        assert live.paths(s.topic, "") == ["README.md", "src/"]
        assert live.paths(s.topic, "src/") == ["src/app.py", "src/apps/"]
        assert live.paths(s.topic, "src/.") == ["src/.hidden"]
        assert live.paths(s.topic, "nowhere/") == []
        assert live.paths("no/such/share", "") == []
    finally:
        live.close()


def test_live_source_answers_by_the_deadline(network, share, tmp_path):
    s = share()
    files(s.workspace)
    live = LiveSource(network.client, tmp_path / "topics")
    try:
        live.deadline = time.monotonic()          # no time at all: nothing yet, but it's on its way
        assert live.paths(s.topic, "src/") == []
        live.deadline = time.monotonic() + 10
        assert live.paths(s.topic, "src/") == ["src/app.py", "src/apps/"]
    finally:
        live.close()


# ── The server process ──────────────────────────────────────────────────

class Server:
    def __init__(self, env: dict, run: Path, process: subprocess.Popen):
        self.env, self.run, self.process = env, run, process

    def request(self, shell: str, line: str, cur: str = "", default: str = "", *, tcp: bool = False,
                token: str | None = None, protocol: str = completion.PROTOCOL) -> str:
        if tcp:
            port, real = (self.run / "complete.tcp").read_text().split()
            conn = socket.create_connection(("127.0.0.1", int(port)))
            token = real if token is None else token
        else:
            conn = socket.socket(socket.AF_UNIX)
            conn.connect(str(self.run / "complete.sock"))
        with conn:
            conn.sendall(f"{protocol}\t{token or ''}\t{shell}\t{default}\t{cur}\t{line}\n".encode())
            conn.settimeout(5)
            data = b""
            while chunk := conn.recv(4096):
                data += chunk
        return data.decode()


def start(env: dict, *args: str) -> subprocess.Popen:
    return subprocess.Popen([sys.executable, "-m", "synq_expose", "_complete-server", *args], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@pytest.fixture
def server_env(network, tmp_path, short_dir):
    config = tmp_path / "config" / "expose-synq" / "zenoh.json5"
    config.parent.mkdir(parents=True)
    config.write_text(json.dumps({"connect": {"endpoints": [network.endpoint]}}))
    return {**os.environ, "XDG_RUNTIME_DIR": str(short_dir), "XDG_CONFIG_HOME": str(tmp_path / "config"),
            "XDG_CACHE_HOME": str(tmp_path / "cache")}


@pytest.fixture
def server(server_env, short_dir):
    process = start(server_env, "--idle", "60")
    run = short_dir / "expose-synq"
    assert eventually(lambda: (run / "complete.tcp").exists()), "the server didn't start"
    yield Server(server_env, run, process)
    process.terminate()
    process.wait(timeout=10)


def test_server_answers_both_sockets(server, share):
    s = share()
    files(s.workspace)
    assert server.request("zsh", "expose-synq mo").startswith("_arguments '*: :((\"mount\"")
    assert eventually(lambda: server.request("bash", f"expose-synq mount {s.topic[:3]}", s.topic[:3], tcp=True)
                      == f"{s.topic}\n")
    line = f"expose-synq file ls {s.topic}:src/a"
    assert eventually(lambda: server.request("bash", line, "src/a", tcp=True)) == "src/app.py\nsrc/apps/\n"
    assert server.request("zsh", line) == \
        f"compadd -p {s.topic}:src/ -S '' -- apps/; compadd -p {s.topic}:src/ -- app.py\n"
    assert server.request("bash", "expose-synq file ls sr", "sr", default=s.topic, tcp=True) == "src/\n"


def test_server_drops_a_bad_request(server):
    assert server.request("bash", "expose-synq mo", "mo", tcp=True) == "mount\n"
    assert server.request("bash", "expose-synq mo", "mo", tcp=True, token="wrong") == ""
    assert server.request("bash", "expose-synq mo", "mo", protocol="0") == ""
    assert server.request("fish", "expose-synq mo") == ""
    assert server.request("bash", "expose-synq mo", "mo", tcp=True) == "mount\n"   # still up


def test_one_server_per_runtime_dir(server):
    assert (server.run / "complete.lock").read_text() == f"{server.process.pid}\n"
    second = start(server.env)
    assert second.wait(timeout=10) == 0
    assert server.process.poll() is None
    assert (server.run / "complete.lock").read_text() == f"{server.process.pid}\n"


def test_server_exits_when_idle_and_cleans_up(server_env, short_dir):
    process = start(server_env, "--idle", "1")
    assert process.wait(timeout=20) == 0
    assert sorted(p.name for p in (short_dir / "expose-synq").iterdir()) == ["complete.lock"]
    assert (short_dir / "expose-synq" / "complete.lock").read_text() == ""


def test_server_exits_after_its_config_changes(server):
    assert server.request("bash", "expose-synq mo", "mo", tcp=True) == "mount\n"
    config = Path(server.env["XDG_CONFIG_HOME"]) / "expose-synq" / "zenoh.json5"
    os.utime(config, (time.time() + 10, time.time() + 10))
    assert server.request("bash", "expose-synq mo", "mo", tcp=True) == "mount\n"   # answered, then gone
    assert server.process.wait(timeout=10) == 0


# ── Real shells, typing Tab on a pty ────────────────────────────────────

def type_into(argv: list[str], env: dict, keys: str) -> str:
    """Start an interactive shell on a pty, type ``keys`` (pausing after each Tab), and return the screen."""
    fd, tty = pty.openpty()
    fcntl.ioctl(tty, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 500, 0, 0))   # wide: lines don't wrap
    shell = subprocess.Popen(argv, env=env, stdin=tty, stdout=tty, stderr=tty, start_new_session=True)
    os.close(tty)
    out = b""

    def pump(seconds):
        nonlocal out
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            if select.select([fd], [], [], 0.05)[0]:
                try:
                    out += os.read(fd, 4096)
                except OSError:
                    return

    pump(1.0)
    for chunk in re.split(r"(?<=\t)", keys):
        os.write(fd, chunk.encode())
        pump(1.5 if chunk.endswith("\t") else 0.2)
    os.write(fd, b"\x15exit\n")
    pump(0.3)
    shell.wait(timeout=5)
    os.close(fd)
    screen = re.sub(r"\x1b\[[0-9;?]*[a-zA-Z]|\x1b[=>]", "", out.decode(errors="replace"))
    while re.search(r"[^\x08]\x08", screen):                  # apply backspaces
        screen = re.sub(r"[^\x08]\x08", "", screen)
    return screen


def rc_head(prompt: str, bin_dir: Path) -> str:
    """The start of an rc file: ``bin_dir`` first on PATH (the system rc files run before it and may set PATH)."""
    return f"PS1='{prompt} '\nPATH={bin_dir}:$PATH\n"


def run_shell(shell: str, tmp_path: Path, env: dict, bin_dir: Path, keys: str) -> str:
    env = {**env, "HOME": str(tmp_path), "TERM": "xterm", "ZDOTDIR": str(tmp_path)}
    if shell == "bash":
        rc = tmp_path / "bashrc"
        rc.write_text(rc_head("$", bin_dir) + completion.script("bash"))
        return type_into(["bash", "--noprofile", "--rcfile", str(rc), "-i"], env, keys)
    fpath = tmp_path / "zfunc"
    fpath.mkdir()
    (fpath / "_expose-synq").write_text(completion.script("zsh"))     # autoloaded, as installed
    (tmp_path / ".zshrc").write_text(rc_head("%", bin_dir) + f"fpath=({fpath} $fpath)\n"
                                     "autoload -U compinit; compinit -u -D\n")
    return type_into(["zsh", "-i"], env, keys)


def no_python(tmp_path: Path) -> Path:
    """A bin directory whose expose-synq fails: what completes must have come from the server."""
    bin_dir = tmp_path / "nopython"
    bin_dir.mkdir()
    (bin_dir / "expose-synq").write_text("#!/bin/sh\nexit 1\n")
    (bin_dir / "expose-synq").chmod(0o755)
    return bin_dir


@pytest.mark.parametrize("shell", ["bash", "zsh"])
def test_shell_completes_paths_from_the_server(shell, server, share, tmp_path):
    if not shutil.which(shell):
        pytest.skip(f"no {shell}")
    s = share()
    files(s.workspace)
    assert eventually(lambda: s.topic in server.request("bash", "expose-synq mount ", tcp=True))
    assert eventually(lambda: server.request("bash", f"expose-synq file ls {s.topic}:src/a", "a", tcp=True))
    screen = run_shell(shell, tmp_path, server.env, no_python(tmp_path),
                       f"expose-synq file ls {s.topic[:-2]}\tsr\tapp.\t")
    assert f"expose-synq file ls {s.topic}:src/app.py" in screen


@pytest.fixture
def reap(tmp_path):
    """Stop any server the fallback started under ``tmp_path``'s cache."""
    yield
    for lock in tmp_path.glob("cache/expose-synq/complete.lock"):
        if pid := lock.read_text().strip():
            try:
                os.kill(int(pid), signal.SIGTERM)
            except ProcessLookupError:
                pass


@pytest.mark.parametrize("shell", ["bash", "zsh"])
def test_shell_falls_back_without_the_server(shell, tmp_path, cache, reap):
    if not shutil.which(shell):
        pytest.skip(f"no {shell}")
    env = {"PATH": os.environ["PATH"], "XDG_CACHE_HOME": str(cache.parent.parent)}
    screen = run_shell(shell, tmp_path, env, Path(sys.executable).parent, "expose-synq file ls host/p\tsrc")
    assert "expose-synq file ls host/proj:src" in screen
