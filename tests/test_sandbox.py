from pathlib import Path

from conftest import stub
from synq_expose.config import ExecConfig, SandboxConfig
from synq_expose.sandbox import wrap_command


def wrap(tmp_path, **sandbox):
    return wrap_command(
        ["make", "test"], workspace=tmp_path, writable=False,
        hidden=[tmp_path / ".env"], config=ExecConfig(sandbox=SandboxConfig(**sandbox)),
    )


def has_run(argv, *items) -> bool:
    n = len(items)
    return any(tuple(argv[i:i + n]) == items for i in range(len(argv)))


def test_unsandboxed_runs_argv_as_is(tmp_path):
    assert wrap(tmp_path, enabled=False) == ["make", "test"]


@stub
def test_sandboxed_argv(tmp_path):
    argv = wrap(tmp_path)
    assert Path(argv[0]).name == "bwrap"
    assert argv[-3:] == ["--", "make", "test"]
    assert "--unshare-all" in argv and "--share-net" not in argv
    assert has_run(argv, "--ro-bind", str(tmp_path), str(tmp_path))
    assert has_run(argv, "--chdir", str(tmp_path))
    assert str(tmp_path / ".env") in argv


@stub
def test_allow_network(tmp_path):
    assert "--share-net" in wrap(tmp_path, allow_network=True)
