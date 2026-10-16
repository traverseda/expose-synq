import pytest

from synq_expose.config import ExposeConfig
from synq_expose.policy import PathPolicy, PolicyError


@pytest.fixture
def policy(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / ".env").write_text("SECRET=1")
    config = ExposeConfig(workspace=tmp_path, topic="t", allow_write=True, readonly_paths=(".git/**",))
    return PathPolicy.from_config(config)


def test_resolves_inside_root(policy, tmp_path):
    assert policy.resolve("src") == tmp_path / "src"
    assert policy.resolve("/src") == tmp_path / "src"


def test_rejects_escape(policy):
    with pytest.raises(PolicyError):
        policy.resolve("../etc/passwd")


def test_rejects_symlink_escape(policy, tmp_path):
    (tmp_path / "out").symlink_to("/etc")
    with pytest.raises(PolicyError):
        policy.resolve("out/passwd")


def test_default_excludes_hide_secrets(policy):
    with pytest.raises(PolicyError):
        policy.resolve(".env")


def test_readonly_paths(policy):
    policy.resolve(".git/HEAD")
    with pytest.raises(PolicyError):
        policy.resolve(".git/HEAD", write=True)


@pytest.mark.parametrize("path", ["src/../.env", "src/./x", "src//x", "src/*", "src/**", "a$b", "a#b", "a?b"])
def test_rejects_paths_that_differ_from_their_key(policy, path):
    with pytest.raises(PolicyError):
        policy.resolve(path)


def test_rejects_symlink_inside_workspace(policy, tmp_path):
    # Stays inside the root, but a rule on src/** shouldn't reach .env through it
    (tmp_path / "src" / "env").symlink_to(tmp_path / ".env")
    with pytest.raises(PolicyError):
        policy.resolve("src/env")


def test_root_aliases(policy, tmp_path):
    assert policy.resolve(".") == tmp_path
    assert policy.resolve("") == tmp_path
