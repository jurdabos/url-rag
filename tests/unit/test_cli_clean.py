"""Regression checks for the shared Acidbase clean command."""

from acidbase.clean import clean_command
from click.testing import CliRunner

from url_rag.cli import cli


def test_clean_uses_shared_command() -> None:
    """Keep the original Acidbase command registered without a local wrapper."""
    assert cli.commands["clean"] is clean_command


def test_clean_help() -> None:
    """Expose the cleaner's help without removing anything."""
    result = CliRunner().invoke(cli, ["clean", "--help"])
    assert result.exit_code == 0, result.output
    assert "--dry-run" in result.output
