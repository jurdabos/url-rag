"""
Fetching and retry helpers for the ingestion DAG.

The DAG's tasks are closures that unit tests cannot reach, so the rules for what
counts as a bad link and when a batch gives up live here. Tasks import this module
lazily, because ``requests`` and ``bs4`` are too heavy for DAG parse time.
"""

from __future__ import annotations

import logging
import re
import time
import traceback
from collections.abc import Callable
from typing import TypeVar
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup
from bs4.exceptions import ParserRejectedMarkup

logger = logging.getLogger(__name__)
T = TypeVar("T")
FETCH_TIMEOUT_SECONDS = 15
MAX_ALERT_HOSTS = 5
MAX_ALERT_TRACEBACK_CHARS = 1500
MAX_TITLE_CHARS = 200
RETRY_ATTEMPTS = 3
USER_AGENT = "AirflowRAG/1.0"
_URL = re.compile(r"https?://\S+")


class EmptyPageError(Exception):
    """Raised when a page loads but holds no extractable text."""


# What a link can do wrong by itself. Anything else points at a bug in this code: pages from the web
# must not be able to stop the batch, so it is caught apart, and reported through build_alert.
FETCH_ERRORS: tuple[type[Exception], ...] = (requests.RequestException, ParserRejectedMarkup, EmptyPageError)


def fetch_document(url: str, max_chars: int) -> dict[str, str]:
    """
    Fetches a page and extracts its readable text.
    Args:
        url: The cleaned URL to fetch
        max_chars: Length to truncate the text to
    Returns:
        The document, with its URL, title and text
    Raises:
        Any member of FETCH_ERRORS when the link itself is the problem
    """
    response = requests.get(url, timeout=FETCH_TIMEOUT_SECONDS, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    for element in soup(["script", "style", "nav", "footer", "header"]):
        element.decompose()
    text = soup.get_text(separator="\n", strip=True)
    if not text.strip():
        raise EmptyPageError("no text after extraction")
    if len(text) > max_chars:
        logger.info("Truncating %s from %d to %d chars", url, len(text), max_chars)
        text = text[:max_chars]
    # An empty <title> gives a title tag whose .string is None
    title = (soup.title.string or "").strip() if soup.title else ""
    return {"url": url, "title": (title or url)[:MAX_TITLE_CHARS], "content": text}


def failure_reason(exc: Exception) -> str:
    """
    Names why a link failed, for counts by reason in the run's report.
    Args:
        exc: A member of FETCH_ERRORS
    Returns:
        A short reason that never contains the URL
    """
    if isinstance(exc, EmptyPageError):
        return "empty page"
    if isinstance(exc, ParserRejectedMarkup):
        return "unparsable page"
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return f"http {exc.response.status_code}"
    if isinstance(exc, requests.Timeout):
        return "timeout"
    if isinstance(exc, requests.exceptions.SSLError):
        return "tls"
    if isinstance(exc, requests.TooManyRedirects):
        return "too many redirects"
    if isinstance(exc, (requests.exceptions.InvalidURL, requests.exceptions.InvalidSchema)):
        return "invalid url"
    if isinstance(exc, requests.ConnectionError):
        text = str(exc)
        return "dns" if "NameResolutionError" in text or "Failed to resolve" in text else "connection"
    return "other request error"


def describe_unexpected(url: str, exc: Exception) -> dict[str, str]:
    """
    Records a failure outside FETCH_ERRORS for the run's alert.

    Keeps the host instead of the URL and masks URLs in the traceback, because the
    alert leaves the stack by email. The full URL stays in the task log.
    Args:
        url: The URL being fetched
        exc: The exception raised
    Returns:
        The exception type, where it was raised, the host and the masked traceback
    """
    frames = traceback.extract_tb(exc.__traceback__)
    location = f"{frames[-1].filename.rsplit('/', 1)[-1]}:{frames[-1].lineno}" if frames else "unknown"
    return {
        "type": type(exc).__name__,
        "location": location,
        "host": urlsplit(url).hostname or "unknown",
        "traceback": _URL.sub("URL", "".join(traceback.format_exception(exc)))[-MAX_ALERT_TRACEBACK_CHARS:],
    }


def build_alert(failures: list[dict[str, str]], urls_tried: int, documents_fetched: int) -> tuple[str, str]:
    """
    Turns a run's unexpected failures into one digest.

    Failures are grouped by exception type and location, so a barrage from one bug
    is one entry in one email rather than an email per page.
    Args:
        failures: Records from describe_unexpected
        urls_tried: How many URLs the run tried to fetch
        documents_fetched: How many of them succeeded, which tells whether the run carried on
    Returns:
        The subject and the plain-text body
    """
    groups: dict[tuple[str, str], list[dict[str, str]]] = {}
    for failure in failures:
        groups.setdefault((failure["type"], failure["location"]), []).append(failure)
    subject = f"[url-rag] {len(failures)} unexpected failure(s) in {len(groups)} place(s)"
    outcome = (
        "The ingest carried on. These pages were left as they were, so the next run retries them."
        if documents_fetched
        else "No page could be fetched, so this run stopped and changed nothing. The next run retries them."
    )
    lines = [
        f"{len(failures)} of {urls_tried} pages hit a failure that is not a known bad-link case.",
        outcome,
        "Full URLs are in the fetch_documents task log.",
    ]
    for (exc_type, location), group in sorted(groups.items(), key=lambda item: -len(item[1])):
        hosts = sorted({failure["host"] for failure in group})
        extra = f" and {len(hosts) - MAX_ALERT_HOSTS} more" if len(hosts) > MAX_ALERT_HOSTS else ""
        lines += [
            "",
            f"{exc_type} at {location}: {len(group)} page(s)",
            f"Hosts: {', '.join(hosts[:MAX_ALERT_HOSTS])}{extra}",
            group[0]["traceback"],
        ]
    return subject, "\n".join(lines)


def with_retries(
    action: Callable[[], T],
    *,
    label: str,
    attempts: int = RETRY_ATTEMPTS,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """
    Runs an action with exponential backoff.

    The last attempt runs outside the handler, so its failure always reaches the
    caller: a batch that keeps failing stops the task instead of being skipped.
    Args:
        action: The call to attempt
        label: What to call the action in log lines
        attempts: Total number of attempts
        sleep: The waiting function, replaceable in tests
    Returns:
        The action's result
    """
    for attempt in range(1, attempts):
        try:
            return action()
        except Exception as exc:  # to retry anything transient; the final attempt below still raises
            wait = 2**attempt
            logger.warning("%s attempt %d/%d failed (%s), retrying in %ds", label, attempt, attempts, exc, wait)
            sleep(wait)
    return action()
