"""Regression checks for the shared Acidbase bump command."""

from acidbase.versioning import bump_command
from click.testing import CliRunner

from url_rag.cli import cli


def test_bump_uses_shared_command() -> None:
    """Keep the original Acidbase command registered without a local wrapper."""
    assert cli.commands["bump"] is bump_command


def test_bump_help() -> None:
    """Expose versioning help without changing project metadata."""
    result = CliRunner().invoke(cli, ["bump", "--help"])
    assert result.exit_code == 0, result.output
    assert "--dry-run" in result.output
    assert "patch" in result.output
