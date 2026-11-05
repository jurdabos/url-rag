"""Unit tests for :mod:`include.url_filter`."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from include.url_filter import (
    DEFAULT_KEEP_PARAMS,
    DEFAULT_SKIP_DOMAINS,
    DEFAULT_SKIP_EXTENSIONS,
    DEFAULT_SKIP_PATH_KEYWORDS,
    MAX_PARAM_VALUE_LENGTH,
    clean_url,
    clean_urls,
    content_hash,
    should_skip_url,
    vector_ids,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


class TestCleanUrl:
    """Covers the allowlist that decides what survives from a listed URL."""

    @pytest.mark.parametrize(
        "url, expected",
        [
            (
                "https://shop.example/item/1.html?pdp_npi=4&acnt=111-111-1111&gclid=abc",
                "https://shop.example/item/1.html",
            ),
            ("https://example.com/a?utm_source=x&fbclid=y&gbraid=z&_gl=1", "https://example.com/a"),
            ("https://help.example/en?_kx=abc.DEF", "https://help.example/en"),
            ("https://news.example/album?newsletter_id=1&newsletter_sig=abc", "https://news.example/album"),
            ("https://example.com/feed?pl=JTdCJTIyYWRkcmVzcyUyMg", "https://example.com/feed"),
            ("https://example.com/in?email=a%40b.example&autoLogin=true&riid=1", "https://example.com/in"),
            ("https://example.com/bookings?activeBookingId=12345678", "https://example.com/bookings"),
            ("https://example.com/s?q=something+personal&query=more", "https://example.com/s"),
            ("https://example.com/j/1?pwd=abcdef", "https://example.com/j/1"),
        ],
    )
    def test_drops_parameters_outside_the_allowlist(self, url, expected):
        """Drops tracking, session, personal and search parameters."""
        assert clean_url(url) == expected

    @pytest.mark.parametrize(
        "url, expected",
        [
            ("https://example.com/show.php?id=42&utm_source=x", "https://example.com/show.php?id=42"),
            ("https://example.com/list?page=2", "https://example.com/list?page=2"),
            ("https://example.com/doc?docID=1234567", "https://example.com/doc?docID=1234567"),
            ("https://example.com/szotar?szo=alma&ref=x", "https://example.com/szotar?szo=alma"),
            ("https://example.com/?p=9&lang=es", "https://example.com/?p=9&lang=es"),
        ],
    )
    def test_keeps_content_selecting_parameters(self, url, expected):
        """Keeps allowlisted names whatever their case, in their original order."""
        assert clean_url(url) == expected

    @pytest.mark.parametrize(
        "value",
        ["a" * (MAX_PARAM_VALUE_LENGTH + 1), "a%40b.example", "a@b.example", ""],
    )
    def test_rejects_unsafe_values_in_allowlisted_parameters(self, value):
        """Drops tokens, encoded blobs, email addresses and empty values under a kept name."""
        assert clean_url(f"https://example.com/x?id={value}") == "https://example.com/x"

    def test_keeps_a_value_at_the_length_limit(self):
        """Treats the limit as inclusive."""
        url = f"https://example.com/x?id={'a' * MAX_PARAM_VALUE_LENGTH}"
        assert clean_url(url) == url

    @pytest.mark.parametrize(
        "url, expected",
        [
            ("https://user:hunter2@example.com/x", "https://example.com/x"),
            ("https://example.com/x#section-2", "https://example.com/x"),
            ("https://example.com/x?id=1#/route", "https://example.com/x?id=1"),
            ("https://example.com:8443/x?gclid=1", "https://example.com:8443/x"),
            ("http://[2001:db8::1]:8080/x?gclid=1", "http://[2001:db8::1]:8080/x"),
            ("https://EXAMPLE.com/Path", "https://example.com/Path"),
            ("  https://example.com/x  ", "https://example.com/x"),
        ],
    )
    def test_normalizes_the_authority_and_drops_fragments(self, url, expected):
        """Removes userinfo and fragments, keeping the port and the path's case."""
        assert clean_url(url) == expected

    def test_undoes_html_escaped_ampersands(self):
        """Handles the ``&amp;`` that bookmark exports leave in query strings."""
        assert clean_url("https://example.com/x?id=1&amp;sig=abc") == "https://example.com/x?id=1"

    def test_does_not_reencode_kept_pairs(self):
        """Leaves a URL with nothing to drop untouched, to keep change detection stable."""
        url = "https://example.com/x?word=caf%C3%A9&page=2"
        assert clean_url(url) == url

    def test_is_idempotent(self):
        """Cleaning a cleaned URL changes nothing."""
        once = clean_url("https://user:pw@example.com/x?id=1&utm_source=y#z")
        assert clean_url(once) == once

    @pytest.mark.parametrize(
        "url", ["", "   ", "not a url", "https://[bad", "https://example.com:notaport/", "mailto:a@b.example"]
    )
    def test_fails_closed_on_urls_without_a_parsable_host(self, url):
        """Returns an empty string, which ``should_skip_url`` then rejects."""
        assert clean_url(url) == ""
        assert should_skip_url(clean_url(url)) == "empty url"

    def test_accepts_a_custom_allowlist(self):
        """Caller-supplied names replace the defaults."""
        assert clean_url("https://example.com/x?id=1&v=2", keep_params={"v"}) == "https://example.com/x?v=2"

    def test_excludes_search_terms_from_the_defaults(self):
        """Search terms are what makes a search URL personal."""
        assert not {"q", "query", "s", "search"} & DEFAULT_KEEP_PARAMS


class TestCleanUrls:
    """Covers cleaning a whole list."""

    def test_keeps_order_and_collapses_duplicates(self):
        """URLs that differ only in dropped parts become one entry."""
        urls = ["https://b.example/x?utm_source=1", "https://a.example/y", "https://b.example/x?gclid=2#top"]
        assert clean_urls(urls) == ["https://b.example/x", "https://a.example/y"]

    def test_drops_unparsable_entries(self):
        """Entries without a host disappear rather than reaching a fetch."""
        assert clean_urls(["not a url", "https://example.com/x"]) == ["https://example.com/x"]


class TestVectorIds:
    """Covers the IDs handed to ``index.delete``, which rejects anything but strings."""

    def test_extracts_ids_from_list_items(self):
        """Newer Pinecone clients yield objects carrying an ``id`` attribute."""
        page = [
            SimpleNamespace(id="https://example.com/a::chunk_0"),
            SimpleNamespace(id="https://example.com/a::chunk_1"),
        ]
        assert vector_ids(page) == ["https://example.com/a::chunk_0", "https://example.com/a::chunk_1"]

    def test_passes_plain_strings_through(self):
        """Older Pinecone clients yield the IDs themselves."""
        assert vector_ids(["a::chunk_0"]) == ["a::chunk_0"]

    def test_handles_an_empty_page(self):
        """A prefix with no vectors yields nothing to delete."""
        assert vector_ids([]) == []


class TestWiring:
    """Covers how the pipeline reaches the shared filter, which unit tests cannot import through Airflow."""

    def test_purge_reads_ids_and_cannot_fail_silently(self):
        """A failed purge must stop the task before the manifest is saved, which would lose the URLs to purge."""
        source = (REPO_ROOT / "dags" / "ingestionrag.py").read_text(encoding="utf-8")
        assert "ids_to_delete.extend(vector_ids(page))" in source
        assert source.index('raise RuntimeError(f"Could not purge') < source.index("Variable.set(CONTENT_HASHES_VAR")

    def test_ingestion_dag_cleans_at_load(self):
        """Cleans before anything else sees a URL, whichever source it came from."""
        source = (REPO_ROOT / "dags" / "ingestionrag.py").read_text(encoding="utf-8")
        assert 'clean_urls(json.loads(_get_var("rag_source_urls", _load_default_urls())))' in source

    def test_ingestion_dag_has_no_private_skip_lists(self):
        """Keeps one copy of the filter, so the DAG and the tests cannot drift apart."""
        source = (REPO_ROOT / "dags" / "ingestionrag.py").read_text(encoding="utf-8")
        assert "SKIP_DOMAINS" not in source
        assert "_should_skip" not in source

    def test_url_list_stays_out_of_the_image_but_the_filter_does_not(self):
        """Excludes the list from image builds while keeping the module the DAGs import."""
        ignored = (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8").split()
        assert "include/urls.json" in ignored
        assert "include/" not in ignored
        assert "include" not in ignored


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

    @pytest.mark.parametrize(
        "url",
        ["https://www.google.com/search", "https://www.google.es/search", "https://www.google.co.uk/search"],
    )
    def test_skips_google_search_urls(self, url):
        """Skips Google Search on any country domain, because results are not documents."""
        assert should_skip_url(url) == "Google Search URL"

    @pytest.mark.parametrize("url", ["https://example.com/search", "https://notgoogle.com/search"])
    def test_keeps_search_paths_on_other_sites(self, url):
        """Applies the search rule to Google hosts only."""
        assert should_skip_url(url) is None

    @pytest.mark.parametrize(
        "url",
        [
            "http://192.168.1.10/admin",
            "http://10.0.0.5/",
            "http://[fd00::1]/",
            "http://nas.local/share",
            "http://build.internal/x",
            "http://router.lan/",
        ],
    )
    def test_skips_private_hosts(self, url):
        """Skips private addresses and suffixes, which a bookmark export can carry."""
        assert should_skip_url(url) == "private host"

    def test_keeps_public_ip_addresses(self):
        """Applies the private-host rule to private addresses only."""
        assert should_skip_url("http://93.184.216.34/page") is None

    @pytest.mark.parametrize(
        "url",
        [
            "https://contoso-my.sharepoint.com/personal/x",
            "https://dbc-00000000-0000.cloud.databricks.com/",
            "https://us02web.zoom.us/j/123",
            "https://gist.github.com/someone/abc",
            "https://example.notion.site/page",
            "https://www.ubereats.com/es-en/feed",
            "https://www1.sedecatastro.gob.es/CYCBienInmueble/OVCConCiud.aspx",
            "https://my.surfshark.com/alternative-id/1",
            "https://ver.movistarplus.es/",
        ],
    )
    def test_skips_tenants_sharing_and_personal_services(self, url):
        """Matches whole suffixes, so no tenant or workspace has to be named in code."""
        assert should_skip_url(url).startswith("skipped domain")

    def test_skips_opaque_tokens_in_the_path(self):
        """Treats an unguessable path segment as a capability link."""
        url = "https://example.com/file/a1B2c3D4e5F6g7H8i9J0k1L2m3N4o5"
        assert should_skip_url(url) == "opaque token in path"

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/blog/a-very-long-article-title-with-many-words-in-it-2026",
            "https://example.com/wiki/Antidisestablishmentarianism_and_friends",
        ],
    )
    def test_keeps_long_readable_paths(self, url):
        """Does not mistake a long slug for a token."""
        assert should_skip_url(url) is None

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
