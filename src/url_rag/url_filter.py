"""
URL filtering and content-hashing helpers.

Provides pure, dependency-free functions that decide whether a given URL
is worth scraping for the RAG ingestion pipeline, plus a small SHA-256
helper for change-detection (CDC).  Kept side-effect free so the same
logic can be reused from Airflow DAGs, the Airflow Jinja-macro plugin
(see ``plugins/url_helpers_plugin.py``), and unit tests.
"""

from __future__ import annotations

import hashlib
from urllib.parse import urlparse

# Default skip lists kept in sync with dags/ingestionrag.py.  Callers may
# override them by passing custom tuples.
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
    "eunorg-my.sharepoint.com",
    "dbc-fd72a54d-4556.cloud.databricks.com",
    "gemini.google.com",
    "claude.ai",
    "chat.openai.com",
    "account.jetbrains.com",
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


def should_skip_url(
    url: str,
    skip_extensions: tuple[str, ...] = DEFAULT_SKIP_EXTENSIONS,
    skip_domains: tuple[str, ...] = DEFAULT_SKIP_DOMAINS,
    skip_path_keywords: tuple[str, ...] = DEFAULT_SKIP_PATH_KEYWORDS,
) -> str | None:
    """Returns a human-readable reason string if the URL should be skipped, else None.

    The function is deliberately exhaustive: it inspects file extension,
    hostname, localhost/loopback addresses, well-known auth/profile path
    fragments, and Google Maps URLs.  The reason string mirrors the
    log lines produced by the ingestion DAG.
    """
    if not url:
        return "empty url"
    lower = url.lower()
    if any(lower.endswith(ext) for ext in skip_extensions):
        return f"file extension ({lower.rsplit('.', 1)[-1]})"
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    if any(hostname == d or hostname.endswith(f".{d}") for d in skip_domains):
        return f"skipped domain ({hostname})"
    if hostname.startswith("localhost") or hostname.startswith("127."):
        return "localhost"
    path_lower = parsed.path.lower()
    if any(kw in path_lower for kw in skip_path_keywords):
        return f"auth/profile path ({path_lower})"
    if "google.com/maps" in lower:
        return "Google Maps URL"
    return None


def content_hash(text: str) -> str:
    """Returns the SHA-256 hex digest of ``text`` (UTF-8 encoded).

    Used by the ingestion DAG's CDC step to decide whether a fetched
    document has changed since the previous run.
    """
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
