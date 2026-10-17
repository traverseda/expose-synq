"""``expose-synq file``, run as a subprocess against a share on the test network."""
import json
import os
import subprocess
import sys

import pytest

from synq_expose.files import NoTopic, ls_line, parse_mode, split


def test_split():
    assert split("host/expose/proj:src/a.py") == ("host/expose/proj", "src/a.py")
    assert split("src/a.py", "default") == ("default", "src/a.py")
    assert split(":a:b", "default") == ("default", "a:b")
    assert split("./a:b", "default") == ("default", "./a:b")
    assert split("host/expose/proj:") == ("host/expose/proj", ".")
    assert split("host/expose/proj/:x") == ("host/expose/proj", "x")
    with pytest.raises(NoTopic, match="EXPOSE_SYNQ_TOPIC"):
        split("src/a.py")


def test_parse_mode():
    assert parse_mode("644") == parse_mode("0644") == parse_mode("0o644") == 0o644
    with pytest.raises(ValueError):
        parse_mode("rwx")


def test_ls_line():
    line = ls_line({"name": "bin", "type": "dir", "size": 4096, "mtime": 0, "mode": 0o755})
    assert line.startswith("drwxr-xr-x ") and line.endswith(" bin")


@pytest.fixture
def cli(network, share):
    """``cli(*args, stdin=b"", topic=True)``: run ``expose-synq file`` on a writable share. Returns the CompletedProcess."""
    s = share(allow_write=True)

    def run(*args, stdin: bytes = b"", topic: bool = True, check: bool = True):
        env = {**os.environ, "EXPOSE_SYNQ_TOPIC": s.topic if topic else ""}
        result = subprocess.run([sys.executable, "-m", "synq_expose", "file", *args, "-c", network.endpoint],
                                input=stdin, capture_output=True, env=env, timeout=60)
        if check:
            assert result.returncode == 0, result.stderr.decode()
        return result

    run.share = s
    return run


def test_push_pull_round_trip(cli):
    data = os.urandom(3 << 20)
    cli("push", "-", "big.bin", stdin=data)
    assert (cli.share.workspace / "big.bin").read_bytes() == data
    assert cli("pull", "big.bin").stdout == data
    assert cli("pull", "big.bin", "--start", "10", "--end", "20").stdout == data[10:20]


def test_push_append_and_exclusive(cli, tmp_path):
    local = tmp_path / "local.txt"
    local.write_bytes(b"one\n")
    cli("push", str(local), "log")
    cli("push", "--append", "-", "log", stdin=b"two\n")
    assert (cli.share.workspace / "log").read_bytes() == b"one\ntwo\n"
    result = cli("push", "--exclusive", "-", "log", check=False)
    assert result.returncode == 1 and b"exists" in result.stderr


def test_pull_to_a_file_and_missing(cli, tmp_path):
    (cli.share.workspace / "a.txt").write_text("hi")
    cli("pull", "a.txt", str(tmp_path / "out"))
    assert (tmp_path / "out").read_text() == "hi"
    result = cli("pull", "nope", str(tmp_path / "never"), check=False)
    assert result.returncode == 1
    assert not (tmp_path / "never").exists()


def test_topic_in_the_path(cli):
    (cli.share.workspace / "a.txt").write_text("hi")
    assert cli("pull", f"{cli.share.topic}:a.txt", topic=False).stdout == b"hi"
    result = cli("pull", "a.txt", topic=False, check=False)
    assert result.returncode == 1 and b"EXPOSE_SYNQ_TOPIC" in result.stderr


def test_ls_stat_hash_glob(cli):
    ws = cli.share.workspace
    (ws / "src").mkdir()
    (ws / "src" / "a.py").write_text("x = 1\n")
    assert cli("ls").stdout.decode().splitlines()[0].endswith(" src")
    assert json.loads(cli("ls", "src", "--json").stdout)["name"] == "a.py"
    assert "size: 6" in cli("stat", "src/a.py").stdout.decode()
    assert cli("hash", "src/a.py").stdout.decode().startswith("sha256:")
    assert cli("glob", "**/*.py").stdout == b"src/a.py\n"


def test_grep_exit_status(cli):
    (cli.share.workspace / "a.py").write_text("def main():\n    pass\n")
    assert cli("grep", "def ").stdout == b"a.py:1:def main():\n"
    assert cli("grep", "nothing", check=False).returncode == 1
    assert cli("grep", "x", "missing", check=False).returncode == 2


def test_edit(cli, tmp_path):
    target = cli.share.workspace / "a.py"
    target.write_text("a = 1\nb = 2\n")
    cli("edit", "a.py", "a = 1", "a = 3")
    assert target.read_text() == "a = 3\nb = 2\n"
    (tmp_path / "new").write_text("b = 4\nc = 5")
    cli("edit", "a.py", "b = 2", "--new-file", str(tmp_path / "new"))
    assert target.read_text() == "a = 3\nb = 4\nc = 5\n"
    result = cli("edit", "a.py", "zzz", "y", check=False)
    assert result.returncode == 1 and b"0 times" in result.stderr


def test_mkdir_mv_chmod_rm(cli):
    ws = cli.share.workspace
    cli("mkdir", "-p", "a/b")
    cli("push", "-", "a/b/f", stdin=b"x")
    cli("chmod", "600", "a/b/f")
    assert (ws / "a/b/f").stat().st_mode & 0o777 == 0o600
    cli("mv", "a/b/f", "a/g")
    assert (ws / "a/g").read_bytes() == b"x"
    assert cli("rm", "a", check=False).returncode == 1
    cli("rm", "-r", "a")
    assert not (ws / "a").exists()


def test_read_only_share(network, share):
    s = share()
    result = subprocess.run([sys.executable, "-m", "synq_expose", "file", "mkdir", f"{s.topic}:x", "-c", network.endpoint],
                            capture_output=True, timeout=60)
    assert result.returncode == 1 and b"read-only" in result.stderr
