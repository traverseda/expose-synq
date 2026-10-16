"""``expose-synq mount`` in its own process, driven with plain file calls like any program would.

Skipped without FUSE: /dev/fuse, fusermount3, and a libfuse that mfusepy
finds (on NixOS, set FUSE_LIBRARY_PATH to libfuse3.so).
"""
import errno
import os
import shutil
import subprocess
import sys
import time

import pytest

try:
    import mfusepy  # noqa: F401  loads libfuse
except OSError:
    mfusepy = None

pytestmark = pytest.mark.skipif(
    mfusepy is None or not os.path.exists("/dev/fuse") or not shutil.which("fusermount3"),
    reason="needs FUSE (/dev/fuse, fusermount3, libfuse)",
)


@pytest.fixture
def mount(network, tmp_path):
    """Factory: ``mount(share)`` mounts it on ``tmp_path/mnt`` and returns the mountpoint."""
    mounted = []

    def make(share):
        mountpoint = tmp_path / f"mnt{len(mounted)}"
        mountpoint.mkdir()
        proc = subprocess.Popen(
            [sys.executable, "-m", "synq_expose", "mount", share.topic, str(mountpoint), "-c", network.endpoint],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        mounted.append((mountpoint, proc))
        deadline = time.monotonic() + 20
        while not os.path.ismount(mountpoint):
            if proc.poll() is not None:
                pytest.fail(f"mount exited: {proc.stderr.read().decode()}")
            if time.monotonic() > deadline:
                pytest.fail("not mounted after 20 s")
            time.sleep(0.05)
        return mountpoint

    yield make
    for mountpoint, proc in mounted:
        subprocess.run(["fusermount3", "-u", str(mountpoint)], check=False)
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_reads_and_lists(share, mount):
    s = share()
    (s.workspace / "src").mkdir()
    (s.workspace / "src" / "main.py").write_text("print()\n")
    (s.workspace / ".env").write_text("secret")
    mnt = mount(s)
    assert sorted(os.listdir(mnt)) == ["src"]
    assert (mnt / "src" / "main.py").read_text() == "print()\n"
    assert (mnt / "src").is_dir()
    with pytest.raises(FileNotFoundError):
        (mnt / ".env").read_text()


def test_big_file_round_trip(share, mount):
    s = share(allow_write=True)
    mnt = mount(s)
    data = os.urandom(5 << 20)
    (mnt / "big").write_bytes(data)
    assert (s.workspace / "big").read_bytes() == data
    assert (mnt / "big").read_bytes() == data
    assert (mnt / "big").stat().st_size == len(data)


def test_append(share, mount):
    s = share(allow_write=True)
    (s.workspace / "log").write_text("one\n")
    mnt = mount(s)
    with open(mnt / "log", "a") as f:
        f.write("two\n")
    assert (s.workspace / "log").read_text() == "one\ntwo\n"


def test_create_and_mkdir_keep_their_mode(share, mount):
    s = share(allow_write=True)
    mnt = mount(s)
    os.close(os.open(mnt / "private", os.O_WRONLY | os.O_CREAT, 0o600))
    os.mkdir(mnt / "d", 0o700)
    assert (s.workspace / "private").stat().st_mode & 0o777 == 0o600
    assert (s.workspace / "d").stat().st_mode & 0o777 == 0o700


def test_create_exclusive(share, mount):
    s = share(allow_write=True)
    (s.workspace / "there").write_text("x")
    mnt = mount(s)
    with pytest.raises(FileExistsError):
        os.open(mnt / "there", os.O_WRONLY | os.O_CREAT | os.O_EXCL)


def test_read_your_writes_and_truncate(share, mount):
    s = share(allow_write=True)
    mnt = mount(s)
    with open(mnt / "rw", "w+") as f:
        f.write("abcdef")
        f.flush()
        f.seek(0)
        assert f.read() == "abcdef"
        f.truncate(3)
        f.seek(0)
        assert f.read() == "abc"
    assert (s.workspace / "rw").read_text() == "abc"


def test_open_truncate_with_a_reader_open(share, mount):
    s = share(allow_write=True)
    (s.workspace / "f").write_text("0123456789")
    mnt = mount(s)
    with open(mnt / "f") as reader, open(mnt / "f", "w"):
        assert reader.read() == ""          # as on Unix, the reader sees the emptied file


def test_rename_chmod_delete(share, mount):
    s = share(allow_write=True)
    (s.workspace / "a").write_text("a")
    mnt = mount(s)
    os.rename(mnt / "a", mnt / "b")
    os.chmod(mnt / "b", 0o640)
    assert (s.workspace / "b").stat().st_mode & 0o777 == 0o640
    os.mkdir(mnt / "d")
    shutil.move(mnt / "b", mnt / "d" / "b")
    shutil.rmtree(mnt / "d")
    assert os.listdir(s.workspace) == []


def test_errors_carry_their_errno(share, mount):
    s = share(allow_write=True, readonly_paths=("locked",))
    (s.workspace / "locked").write_text("x")
    (s.workspace / "d").mkdir()
    (s.workspace / "d" / "f").write_text("x")
    mnt = mount(s)
    with pytest.raises(OSError) as e:
        (mnt / "locked").write_text("y")
    assert e.value.errno == errno.EROFS
    with pytest.raises(OSError) as e:
        os.rmdir(mnt / "d")
    assert e.value.errno == errno.ENOTEMPTY
    with pytest.raises(FileNotFoundError):
        (mnt / "missing").read_text()


def test_read_only_share_mounts_read_only(share, mount):
    s = share()
    (s.workspace / "f").write_text("x")
    mnt = mount(s)
    with pytest.raises(OSError) as e:
        (mnt / "f").write_text("y")
    assert e.value.errno == errno.EROFS


def test_symlinks_show_but_cant_be_read(share, mount):
    s = share()
    (s.workspace / "f").write_text("x")
    (s.workspace / "link").symlink_to("f")
    mnt = mount(s)
    assert (mnt / "link").is_symlink()
    with pytest.raises(OSError) as e:
        os.readlink(mnt / "link")
    assert e.value.errno == errno.ENOSYS   # no readlink RPC
