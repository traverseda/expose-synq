import os
import pty
import re
import select
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
import typer

from synq_expose import completion
from synq_expose.cli import app
from synq_expose.config import build_zenoh_config
from synq_expose.workspace import FS_READ

TOPICS = ["host/proj", "host/other", "box/a:b"]


@pytest.fixture
def cache(tmp_path, monkeypatch):
    """A fresh topics cache under a private XDG_CACHE_HOME."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    path = completion.cache_path()
    path.parent.mkdir(parents=True)
    path.write_text("".join(f"{t}\n" for t in TOPICS))
    return path


@pytest.fixture
def spawned(monkeypatch):
    calls = []
    monkeypatch.setattr(completion.subprocess, "Popen", lambda argv, **kwargs: calls.append(argv))
    return calls


def test_suffix_follows_the_share_layout():
    assert completion.SUFFIX.startswith(f"/{FS_READ}/sr_metadata/")


def test_topic_completes_from_the_cache(cache, spawned):
    assert completion.complete_topic("host/") == ["host/proj", "host/other"]
    assert completion.complete_topic("box/a:") == ["box/a:b"]
    assert not spawned


def test_remote_completes_the_topic_part_only(cache, spawned):
    assert completion.complete_remote("ho") == ["host/proj:", "host/other:"]
    assert completion.complete_remote("") == ["host/proj:", "host/other:"]   # box/a:b can't be TOPIC:PATH
    assert completion.complete_remote("host/proj:") == []
    assert completion.complete_remote("./x") == []
    assert not spawned


def test_stale_cache_starts_one_refresh(cache, spawned):
    old = time.time() - completion.MAX_AGE - 1
    os.utime(cache, (old, old))
    assert completion.topics() == TOPICS                       # answers from the stale cache right away
    assert completion.topics() == TOPICS
    assert len(spawned) == 1
    assert spawned[0][-3:] == ["_refresh-topics", "--timeout", str(completion.REFRESH_WINDOW)]


def test_missing_cache_starts_a_refresh(tmp_path, monkeypatch, spawned):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "none"))
    assert completion.topics() == []
    assert len(spawned) == 1


def test_refresh_lists_the_shares(network, share, tmp_path):
    s = share()
    path = tmp_path / "topics"
    assert completion.refresh(build_zenoh_config(connect=[network.endpoint], environ={}), path, window=2.0)
    assert s.topic in path.read_text().splitlines()


def test_refresh_skips_while_another_runs(tmp_path):
    import fcntl
    path = tmp_path / "topics"
    with open(tmp_path / "topics.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        assert not completion.refresh(build_zenoh_config(environ={}), path, window=0.1)
    assert not path.exists()


# ── Shell classes ───────────────────────────────────────────────────────

def bash(monkeypatch, line: str, cur: str) -> list[str]:
    """What the bash class answers for ``line``, whose last word bash split as ``cur``."""
    monkeypatch.setenv("COMP_LINE", line)
    monkeypatch.setenv("COMP_CUR", cur)
    shell = completion.BashComplete(typer.main.get_command(app), {}, "expose-synq", "_EXPOSE_SYNQ_COMPLETE")
    return shell.complete().splitlines()


def zsh(monkeypatch, line: str) -> str:
    monkeypatch.setenv("_TYPER_COMPLETE_ARGS", line)
    return completion.ZshComplete(typer.main.get_command(app), {}, "expose-synq", "_EXPOSE_SYNQ_COMPLETE").complete()


def test_bash_completes_commands_and_topics(cache, monkeypatch):
    assert bash(monkeypatch, "expose-synq mo", "mo") == ["mount"]
    assert bash(monkeypatch, "expose-synq mount ho", "ho") == ["host/proj", "host/other"]
    assert bash(monkeypatch, "expose-synq run --topic host/p", "host/p") == ["host/proj"]
    assert bash(monkeypatch, "expose-synq file push local ho", "ho") == ["host/proj:", "host/other:"]


def test_bash_answers_relative_to_its_word_after_a_colon(cache, monkeypatch):
    # bash splits "box/a:" into "box/a" ":", and replaces only the ":".
    assert bash(monkeypatch, "expose-synq mount box/a:", ":") == [":b"]
    assert bash(monkeypatch, "expose-synq mount box/a:b", "b") == ["b"]


def test_zsh_adds_no_space_after_a_topic_prefix(cache, monkeypatch):
    assert zsh(monkeypatch, "expose-synq file ls host/p") == "compadd -S '' -- host/proj:"
    assert zsh(monkeypatch, "expose-synq mount host/p").startswith("_arguments")


def test_completion_prints_each_script():
    for shell in completion.Shell:
        assert "_EXPOSE_SYNQ_COMPLETE" in completion.script(shell, "expose-synq")


# ── Real shells, typing Tab on a pty ────────────────────────────────────

def type_into(argv: list[str], env: dict, keys: str) -> str:
    """Start an interactive shell on a pty, type ``keys`` (pausing after each Tab), and return the screen."""
    fd, tty = pty.openpty()
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


def shell_env(tmp_path: Path, cache: Path) -> dict:
    return {"PATH": os.environ["PATH"], "HOME": str(tmp_path), "TERM": "xterm",
            "XDG_CACHE_HOME": str(cache.parent.parent), "ZDOTDIR": str(tmp_path)}


def rc_head(prompt: str) -> str:
    """The start of an rc file: our expose-synq first on PATH (the system rc files run before it and may set PATH)."""
    bin_dir = Path(sys.executable).parent
    assert (bin_dir / "expose-synq").exists()
    return f"PS1='{prompt} '\nPATH={bin_dir}:$PATH\n"


@pytest.mark.skipif(not shutil.which("bash"), reason="no bash")
def test_bash_completes_topic_colon_path(tmp_path, cache):
    rc = tmp_path / "bashrc"
    rc.write_text(rc_head("$") + completion.script("bash", "expose-synq"))
    screen = type_into(["bash", "--noprofile", "--rcfile", str(rc), "-i"], shell_env(tmp_path, cache),
                       "expose-synq file ls host/p\tsrc")
    assert "expose-synq file ls host/proj:src" in screen


@pytest.mark.skipif(not shutil.which("zsh"), reason="no zsh")
def test_zsh_completes_topic_colon_path(tmp_path, cache):
    fpath = tmp_path / "zfunc"
    fpath.mkdir()
    (fpath / "_expose-synq").write_text(completion.script("zsh", "expose-synq"))   # autoloaded, as installed
    (tmp_path / ".zshrc").write_text(rc_head("%") + f"fpath=({fpath} $fpath)\nautoload -U compinit; compinit -u -D\n")
    screen = type_into(["zsh", "-i"], shell_env(tmp_path, cache), "expose-synq file ls host/p\tsrc")
    assert "expose-synq file ls host/proj:src" in screen
