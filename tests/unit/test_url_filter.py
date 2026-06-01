"""Unit tests for :mod:`url_rag.url_filter`."""

from __future__ import annotations

import pytest

from url_rag.url_filter import (
    DEFAULT_SKIP_DOMAINS,
    DEFAULT_SKIP_EXTENSIONS,
    DEFAULT_SKIP_PATH_KEYWORDS,
    content_hash,
    should_skip_url,
)


class TestShouldSkipUrl:
    """Covers the URL-filter decision matrix used by the ingestion DAG."""

    @pytest.mark.parametrize(
        "url",
        [
            "https://en.wikipedia.org/wiki/Tuvalu",
            "https://example.com/some-article",
            "https://es.wikipedia.org/wiki/La_novia_gitana_(novela)",
        ],
    )
    def test_keeps_normal_article_urls(self, url):
        """Returns None for a regular text/HTML article URL."""
        assert should_skip_url(url) is None

    @pytest.mark.parametrize(
        "url, expected_substr",
        [
            ("https://example.com/foo.pdf", "pdf"),
            ("https://example.com/img.JPG", "jpg"),
            ("https://example.com/archive.zip", "zip"),
            ("https://example.com/audio.mp3", "mp3"),
        ],
    )
    def test_skips_known_binary_extensions(self, url, expected_substr):
        """Skips files whose extension is in DEFAULT_SKIP_EXTENSIONS."""
        reason = should_skip_url(url)
        assert reason is not None
        assert "file extension" in reason
        assert expected_substr in reason.lower()

    @pytest.mark.parametrize(
        "url, host_substr",
        [
            ("https://youtube.com/watch?v=abc", "youtube.com"),
            ("https://www.youtube.com/watch?v=abc", "youtube.com"),
            ("https://music.youtube.com/playlist", "music.youtube.com"),
            ("https://open.spotify.com/album/123", "open.spotify.com"),
            ("https://chat.openai.com/g/abc", "chat.openai.com"),
        ],
    )
    def test_skips_blocked_domains_and_subdomains(self, url, host_substr):
        """Skips both exact and subdomain matches for any DEFAULT_SKIP_DOMAINS entry."""
        reason = should_skip_url(url)
        assert reason is not None
        assert "skipped domain" in reason
        assert host_substr in reason

    @pytest.mark.parametrize(
        "url",
        [
            "http://localhost:8080/api",
            "http://localhost/health",
            "https://localhost.localdomain/",
        ],
    )
    def test_skips_localhost_hosts(self, url):
        """Skips any URL whose hostname starts with 'localhost'.

        Note: the domain check fires before the loopback check, so the
        exact reason for `localhost`/`localhost:8080` is
        ``skipped domain (localhost)``, while a hostname like
        ``localhost.localdomain`` falls through to the dedicated
        loopback branch and returns just ``localhost``. Either way the
        URL must be rejected.
        """
        reason = should_skip_url(url)
        assert reason is not None
        assert "localhost" in reason

    @pytest.mark.parametrize(
        "url, expected_substr",
        [
            ("https://example.com/login?next=/x", "/login"),
            ("https://example.com/account/settings", "/account"),
            ("https://example.com/profile", "/profile"),
            ("https://example.com/dashboard/widgets", "/dashboard"),
        ],
    )
    def test_skips_auth_and_profile_paths(self, url, expected_substr):
        """Skips URLs whose path contains any DEFAULT_SKIP_PATH_KEYWORDS token."""
        reason = should_skip_url(url)
        assert reason is not None
        assert "auth/profile path" in reason
        assert expected_substr in reason

    def test_skips_google_maps_urls(self):
        """Skips google.com/maps URLs irrespective of the exact path."""
        assert should_skip_url("https://www.google.com/maps/place/Tuvalu") == "Google Maps URL"

    def test_empty_url_is_skipped(self):
        """An empty string is rejected with a descriptive reason."""
        assert should_skip_url("") == "empty url"

    def test_respects_custom_overrides(self):
        """Caller-supplied skip lists override the defaults."""
        # Empty overrides allow URLs that would normally be skipped.
        assert (
            should_skip_url(
                "https://youtube.com/watch?v=abc",
                skip_extensions=(),
                skip_domains=(),
                skip_path_keywords=(),
            )
            is None
        )

    def test_default_constants_are_non_empty(self):
        """Sanity check: defaults must be non-empty tuples."""
        assert isinstance(DEFAULT_SKIP_EXTENSIONS, tuple) and DEFAULT_SKIP_EXTENSIONS
        assert isinstance(DEFAULT_SKIP_DOMAINS, tuple) and DEFAULT_SKIP_DOMAINS
        assert isinstance(DEFAULT_SKIP_PATH_KEYWORDS, tuple) and DEFAULT_SKIP_PATH_KEYWORDS


class TestContentHash:
    """Covers SHA-256 hashing used for CDC change-detection."""

    def test_is_deterministic(self):
        """Same input always produces the same digest."""
        assert content_hash("hello") == content_hash("hello")

    def test_different_inputs_yield_different_digests(self):
        """Different inputs produce different digests."""
        assert content_hash("hello") != content_hash("world")

    def test_digest_is_64_lowercase_hex_chars(self):
        """SHA-256 hex digests are exactly 64 lowercase hex characters."""
        digest = content_hash("Tuvalu")
        assert len(digest) == 64
        assert digest == digest.lower()
        int(digest, 16)  # to ensure all characters are valid hex

    def test_handles_unicode(self):
        """Handles non-ASCII input via UTF-8 encoding."""
        # Just checks the call does not raise and produces a 64-char digest.
        assert len(content_hash("árvíztűrő tükörfúrógép")) == 64
