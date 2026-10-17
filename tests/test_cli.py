from pathlib import Path

import pytest
from typer.testing import CliRunner

from synq_expose.cli import app
from synq_expose.config import default_topic

runner = CliRunner()


def test_default_topic_is_the_absolute_path_under_the_node():
    assert default_topic(Path("/home/me/a/proj")) == "./home/me/a/proj"
    assert default_topic(Path("/home/me/b/proj")) == "./home/me/b/proj"
    assert default_topic(Path("/home/me/my proj.d")) == "./home/me/my proj.d"


@pytest.mark.parametrize("path", ["/", "/tickets", "/tickets/x", "/home/@me", "/home/a*b", "/srv/$x", "/srv/#", "/srv/q?"])
def test_default_topic_refuses_what_cant_be_a_topic(path):
    with pytest.raises(ValueError, match="needs --topic"):
        default_topic(Path(path))


def test_share_refuses_a_path_that_cant_be_a_topic(tmp_path):
    workspace = tmp_path / "a*b"
    workspace.mkdir()
    result = runner.invoke(app, ["share", str(workspace)])
    assert result.exit_code == 1
    assert "needs --topic" in " ".join(result.output.split())


def test_help_lists_capability_flags():
    result = runner.invoke(app, ["share", "--help"])
    assert result.exit_code == 0
    for flag in ("--allow-write", "--allow-execute", "--no-sandbox", "--zenoh-config"):
        assert flag in result.output


def test_no_sandbox_requires_execute(tmp_path):
    result = runner.invoke(app, ["share", str(tmp_path), "--no-sandbox"])
    assert result.exit_code == 1
    assert "--allow-execute" in result.output


def test_share_is_a_subcommand():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "share" in result.output


def test_only_share_enrolls_with_a_server(tmp_path):
    result = runner.invoke(app, ["mount", "boxa/expose/x", str(tmp_path), "-c", "hermes.example.com"])
    assert result.exit_code == 1
    assert "only share enrolls" in result.output

