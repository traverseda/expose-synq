from typer.testing import CliRunner

from synq_expose.cli import app

runner = CliRunner()


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
