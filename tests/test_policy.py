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


@pytest.mark.parametrize("path", ["src/../.env", "src/..", ".."])
def test_rejects_dotdot(policy, path):
    with pytest.raises(PolicyError):
        policy.resolve(path)


@pytest.mark.parametrize("path", ["src/./x", "src//x", "./src/x", "src/x/"])
def test_normalizes_like_unix(policy, tmp_path, path):
    assert policy.resolve(path) == tmp_path / "src" / "x"


def test_any_name_is_allowed(policy, tmp_path):
    assert policy.resolve("src/a*b?c$d#e") == tmp_path / "src" / "a*b?c$d#e"


def test_rejects_symlink_inside_workspace(policy, tmp_path):
    # Stays inside the root, but a rule on src/** shouldn't reach .env through it
    (tmp_path / "src" / "env").symlink_to(tmp_path / ".env")
    with pytest.raises(PolicyError):
        policy.resolve("src/env")


def test_root_aliases(policy, tmp_path):
    assert policy.resolve(".") == tmp_path
    assert policy.resolve("") == tmp_path


def test_directory_patterns_match_directories(policy, tmp_path):
    # gitignore-style: ".git/**" covers .git itself, so it can't be deleted or chmodded
    (tmp_path / ".git").mkdir()
    with pytest.raises(PolicyError, match="read-only"):
        policy.resolve(".git", write=True)
    with pytest.raises(PolicyError, match="read-only"):
        policy.resolve(".git/new", write=True, is_dir=True)   # mkdir of a path that doesn't exist yet


def test_nofollow_allows_only_the_last_segment_to_be_a_symlink(policy, tmp_path):
    (tmp_path / "out").symlink_to("/etc")
    assert policy.resolve("out", nofollow=True) == tmp_path / "out"
    with pytest.raises(PolicyError, match="symlink"):
        policy.resolve("out")
    with pytest.raises(PolicyError, match="symlink"):
        policy.resolve("out/passwd", nofollow=True)
