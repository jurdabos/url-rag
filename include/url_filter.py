"""
URL cleaning, filtering and content-hashing helpers.

Provides pure, dependency-free functions that reduce a URL to what a fetch
needs, decide whether it is worth scraping for the RAG ingestion pipeline, and
hash content for change detection (CDC).

The module lives in ``include/`` because that is the one location the DAGs and
the plugin can import inside the Airflow image and the unit tests can import
locally: ``src/`` is not part of the image.
"""

from __future__ import annotations

import hashlib
import ipaddress
import re
from collections.abc import Collection, Iterable
from urllib.parse import unquote, urlsplit, urlunsplit

DEFAULT_SKIP_EXTENSIONS: tuple[str, ...] = (
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".svg",
    ".ico",
    ".gif",
    ".mp3",
    ".mp4",
    ".wav",
    ".zip",
    ".gz",
    ".tar",
)
DEFAULT_SKIP_DOMAINS: tuple[str, ...] = (
    "localhost",
    "youtube.com",
    "music.youtube.com",
    "youtu.be",
    "instagram.com",
    "facebook.com",
    "web.whatsapp.com",
    "drive.google.com",
    "docs.google.com",
    "keep.google.com",
    "mail.google.com",
    "calendar.google.com",
    "classroom.google.com",
    "myactivity.google.com",
    "console.cloud.google.com",
    "maps.google.com",
    "open.spotify.com",
    "listen.tidal.com",
    "soundcloud.com",
    "app.prefect.cloud",
    "app.docusign.com",
    "portal.azure.com",
    "signin.aws.amazon.com",
    "onedrive.live.com",
    "sharepoint.com",
    "cloud.databricks.com",
    "gemini.google.com",
    "claude.ai",
    "chat.openai.com",
    "account.jetbrains.com",
    # Sharing services, where an unguessable link is all that protects a page
    "1drv.ms",
    "box.com",
    "dropbox.com",
    "gist.github.com",
    "mega.nz",
    "notion.site",
    "pastebin.com",
    "wetransfer.com",
    "zoom.us",
    # Personal services, whose pages describe an account, an address or a property
    "my.surfshark.com",
    "sedecatastro.gob.es",
    "ubereats.com",
    "ver.movistarplus.es",
)
DEFAULT_SKIP_PATH_KEYWORDS: tuple[str, ...] = (
    "/login",
    "/signin",
    "/signup",
    "/register",
    "/profile",
    "/account",
    "/dashboard",
    "/my-collection",
    "/inbox",
)
# Query parameters that select content. Everything else is dropped, because a blocklist cannot keep up with
# tracking, session and personal parameters: a wrongly dropped one costs a page, a wrongly kept one can leak.
DEFAULT_KEEP_PARAMS: frozenset[str] = frozenset(
    {
        "cat",
        "category",
        "docid",
        "hl",
        "id",
        "lang",
        "language",
        "lemma",
        "p",
        "page",
        "pageid",
        "pid",
        "szo",
        "word",
    }
)
MAX_PARAM_VALUE_LENGTH = 24  # to reject tokens and encoded blobs in an allowlisted parameter
PRIVATE_HOST_SUFFIXES: tuple[str, ...] = (".internal", ".lan", ".local")
_GOOGLE_HOST = re.compile(r"(^|\.)google\.[a-z]{2,3}(\.[a-z]{2})?$")
_OPAQUE_TOKEN = re.compile(
    r"(?<![A-Za-z0-9])(?=[A-Za-z0-9_]*\d)(?=[A-Za-z0-9_]*[A-Za-z])[A-Za-z0-9_]{28,}(?![A-Za-z0-9])"
)


def _keeps_param(pair: str, keep_params: Collection[str]) -> bool:
    """
    Decides whether a raw ``name=value`` pair survives cleaning.
    Args:
        pair: One query pair, still percent-encoded
        keep_params: Lowercase parameter names to keep
    Returns:
        True for an allowlisted name with a short, non-empty value that is not an email address
    """
    name, _, value = pair.partition("=")
    decoded = unquote(value)
    return unquote(name).lower() in keep_params and 0 < len(decoded) <= MAX_PARAM_VALUE_LENGTH and "@" not in decoded


def clean_url(url: str, keep_params: Collection[str] = DEFAULT_KEEP_PARAMS) -> str:
    """
    Reduces a URL to what a fetch needs.

    Keeps the scheme, host, port, path and allowlisted query parameters. Drops
    userinfo, fragments and every other parameter, and undoes HTML-escaped
    ampersands from bookmark exports. Kept pairs are not re-encoded, so a URL
    with nothing to drop stays stable for change detection.
    Args:
        url: The URL as listed
        keep_params: Lowercase parameter names to keep
    Returns:
        The cleaned URL, or an empty string for a URL without a parsable host, to fail closed
    """
    try:
        parts = urlsplit(url.strip().replace("&amp;", "&"))
        hostname = parts.hostname or ""
        port = parts.port
    except ValueError:
        return ""
    if not hostname:
        return ""
    host = f"[{hostname}]" if ":" in hostname else hostname
    netloc = f"{host}:{port}" if port else host
    query = "&".join(pair for pair in parts.query.split("&") if _keeps_param(pair, keep_params))
    return urlunsplit((parts.scheme, netloc, parts.path, query, ""))


def clean_urls(urls: Iterable[str]) -> list[str]:
    """
    Cleans a URL list.
    Args:
        urls: The URLs as listed
    Returns:
        The cleaned URLs in their original order, without empties or duplicates
    """
    return list(dict.fromkeys(cleaned for cleaned in map(clean_url, urls) if cleaned))


def _is_private_host(hostname: str) -> bool:
    """
    Tells whether a hostname points at a private network.
    Args:
        hostname: Lowercase hostname or IP address
    Returns:
        True for a private suffix or a non-global IP address
    """
    if hostname.endswith(PRIVATE_HOST_SUFFIXES):
        return True
    try:
        return not ipaddress.ip_address(hostname).is_global
    except ValueError:
        return False


def should_skip_url(
    url: str,
    skip_extensions: tuple[str, ...] = DEFAULT_SKIP_EXTENSIONS,
    skip_domains: tuple[str, ...] = DEFAULT_SKIP_DOMAINS,
    skip_path_keywords: tuple[str, ...] = DEFAULT_SKIP_PATH_KEYWORDS,
) -> str | None:
    """
    Decides whether a URL should be skipped.

    The function is deliberately exhaustive: it inspects the file extension,
    hostname, private addresses, well-known auth/profile path fragments,
    Google Search and Maps URLs, and opaque tokens in the path, which mark
    capability links. The reason string mirrors the DAG's log lines.
    Args:
        url: The URL to check, preferably after :func:`clean_url`
        skip_extensions: File extensions to skip
        skip_domains: Domains to skip, including their subdomains
        skip_path_keywords: Path fragments to skip
    Returns:
        A human-readable reason, or None when the URL should be fetched
    """
    if not url:
        return "empty url"
    lower = url.lower()
    if any(lower.endswith(ext) for ext in skip_extensions):
        return f"file extension ({lower.rsplit('.', 1)[-1]})"
    parsed = urlsplit(url)
    hostname = (parsed.hostname or "").lower()
    if any(hostname == d or hostname.endswith(f".{d}") for d in skip_domains):
        return f"skipped domain ({hostname})"
    if hostname.startswith("localhost") or hostname.startswith("127."):
        return "localhost"
    if _is_private_host(hostname):
        return "private host"
    path_lower = parsed.path.lower()
    if any(kw in path_lower for kw in skip_path_keywords):
        return f"auth/profile path ({path_lower})"
    if "google.com/maps" in lower:
        return "Google Maps URL"
    if _GOOGLE_HOST.search(hostname) and path_lower.startswith("/search"):
        return "Google Search URL"
    if _OPAQUE_TOKEN.search(parsed.path):
        return "opaque token in path"
    return None


def content_hash(text: str) -> str:
    """
    Hashes document text for the ingestion DAG's change detection.
    Args:
        text: The text to hash
    Returns:
        The SHA-256 hex digest of the UTF-8 encoded text
    """
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
