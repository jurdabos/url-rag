"""
Unit tests for :mod:`url_rag.rag` and :mod:`url_rag.cli`.

These tests deliberately avoid hitting OpenAI, OpenRouter, or Pinecone
network endpoints; they exercise only the helpers and CLI surface area
that can be tested in isolation.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from url_rag.cli import cli
from url_rag.rag import _get_env


class TestGetEnv:
    """Covers the small environment-variable helper used across rag.py."""

    def test_returns_value_when_set(self, monkeypatch):
        """Reads the variable from the process environment."""
        monkeypatch.setenv("URL_RAG_TEST_KEY", "value-1")
        assert _get_env("URL_RAG_TEST_KEY") == "value-1"

    def test_returns_default_when_missing(self, monkeypatch):
        """Falls back to the provided default if the variable is absent."""
        monkeypatch.delenv("URL_RAG_TEST_KEY_MISSING", raising=False)
        assert _get_env("URL_RAG_TEST_KEY_MISSING", "fallback") == "fallback"

    def test_raises_when_missing_and_no_default(self, monkeypatch):
        """Raises RuntimeError if the variable is missing and no default is given."""
        monkeypatch.delenv("URL_RAG_TEST_KEY_MISSING", raising=False)
        with pytest.raises(RuntimeError, match="URL_RAG_TEST_KEY_MISSING"):
            _get_env("URL_RAG_TEST_KEY_MISSING")


class TestCli:
    """Smoke tests for the Click CLI surface."""

    def test_top_level_help_runs(self):
        """`url-rag --help` exits 0 and lists known subcommands."""
        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "query" in result.output

    def test_query_help_runs(self):
        """`url-rag query --help` exposes the -k / -v flags."""
        result = CliRunner().invoke(cli, ["query", "--help"])
        assert result.exit_code == 0
        assert "--top-k" in result.output
        assert "--verbose" in result.output
