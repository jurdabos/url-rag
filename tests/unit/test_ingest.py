"""Unit tests for :mod:`include.ingest`."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
from bs4.exceptions import ParserRejectedMarkup

from include import ingest
from include.ingest import (
    FETCH_ERRORS,
    MAX_ALERT_HOSTS,
    EmptyPageError,
    build_alert,
    build_failure_alert,
    describe_unexpected,
    failure_reason,
    fetch_document,
    with_retries,
)

URL = "https://example.com/page"
DAG_SOURCE = (Path(__file__).resolve().parents[2] / "dags" / "ingestionrag.py").read_text(encoding="utf-8")


def _serve(monkeypatch: pytest.MonkeyPatch, html: str = "", error: Exception | None = None) -> None:
    """Replaces ``requests.get`` with a response carrying *html*, or raising *error* on status."""

    def raise_for_status() -> None:
        if error:
            raise error

    monkeypatch.setattr(
        ingest.requests,
        "get",
        lambda url, timeout, headers: SimpleNamespace(text=html, raise_for_status=raise_for_status),
    )


class TestFetchDocument:
    """Covers turning a response into a document."""

    def test_extracts_title_and_text(self, monkeypatch):
        """Returns the URL, title and readable text."""
        _serve(monkeypatch, "<html><head><title>Tuvalu</title></head><body><p>Funafuti</p></body></html>")
        assert fetch_document(URL, 100) == {"url": URL, "title": "Tuvalu", "content": "Tuvalu\nFunafuti"}

    @pytest.mark.parametrize(
        "head",
        ["", "<title></title>", "<title>   </title>"],
        ids=["no title tag", "empty title", "blank title"],
    )
    def test_falls_back_to_the_url_without_a_usable_title(self, monkeypatch, head):
        """A page with an empty title is a good page, not a failed link."""
        _serve(monkeypatch, f"<html><head>{head}</head><body><p>Funafuti</p></body></html>")
        assert fetch_document(URL, 100)["title"] == URL

    def test_truncates_long_titles(self, monkeypatch):
        """Keeps metadata within the title limit."""
        _serve(monkeypatch, f"<html><head><title>{'a' * 500}</title></head><body><p>x</p></body></html>")
        assert len(fetch_document(URL, 1000)["title"]) == ingest.MAX_TITLE_CHARS

    def test_truncates_long_text(self, monkeypatch):
        """Caps the extracted text at the given length."""
        _serve(monkeypatch, f"<html><body><p>{'a' * 500}</p></body></html>")
        assert len(fetch_document(URL, 100)["content"]) == 100

    def test_drops_page_furniture(self, monkeypatch):
        """Keeps scripts, styles and navigation out of the text."""
        _serve(
            monkeypatch,
            "<body><nav>menu</nav><script>x()</script><style>p{}</style><p>kept</p><footer>f</footer></body>",
        )
        assert fetch_document(URL, 100)["content"] == "kept"

    def test_rejects_a_page_without_text(self, monkeypatch):
        """Raises rather than returning an empty document."""
        _serve(monkeypatch, "<html><body><script>x()</script></body></html>")
        with pytest.raises(EmptyPageError):
            fetch_document(URL, 100)

    def test_propagates_http_errors(self, monkeypatch):
        """Leaves the failure to the caller, which counts it by reason."""
        _serve(monkeypatch, error=requests.HTTPError(response=SimpleNamespace(status_code=404)))
        with pytest.raises(requests.HTTPError):
            fetch_document(URL, 100)


class TestFetchErrors:
    """Covers the line between a bad link and a bug in this code."""

    @pytest.mark.parametrize(
        "error",
        [requests.ConnectionError, requests.Timeout, requests.HTTPError, ParserRejectedMarkup, EmptyPageError],
    )
    def test_covers_failures_a_link_can_cause(self, error):
        """These are the link's fault, so the batch continues."""
        assert issubclass(error, FETCH_ERRORS)

    @pytest.mark.parametrize("error", [TypeError, AttributeError, KeyError, ValueError, NameError])
    def test_excludes_programming_errors(self, error):
        """These point at our code, so they are reported as bugs rather than posing as a bad link."""
        assert not issubclass(error, FETCH_ERRORS)


class TestFailureReason:
    """Covers the reasons in the run's report."""

    @pytest.mark.parametrize(
        "exc, expected",
        [
            (EmptyPageError("x"), "empty page"),
            (ParserRejectedMarkup("x"), "unparsable page"),
            (requests.HTTPError(response=SimpleNamespace(status_code=403)), "http 403"),
            (requests.HTTPError(response=SimpleNamespace(status_code=500)), "http 500"),
            (requests.exceptions.ConnectTimeout(), "timeout"),
            (requests.exceptions.ReadTimeout(), "timeout"),
            (requests.exceptions.SSLError(), "tls"),
            (requests.TooManyRedirects(), "too many redirects"),
            (requests.exceptions.InvalidURL(), "invalid url"),
            (requests.exceptions.InvalidSchema(), "invalid url"),
            (requests.ConnectionError("Failed to resolve 'gone.example'"), "dns"),
            (requests.ConnectionError("NameResolutionError(...)"), "dns"),
            (requests.ConnectionError("Connection refused"), "connection"),
            (requests.exceptions.ChunkedEncodingError(), "other request error"),
        ],
    )
    def test_names_the_reason(self, exc, expected):
        """Maps each failure onto one reason."""
        assert failure_reason(exc) == expected

    def test_keeps_the_url_out_of_the_reason(self):
        """The report must stay shareable, so a reason never repeats the failing URL."""
        reason = failure_reason(requests.ConnectionError(f"Max retries exceeded with url: {URL}?token=abc"))
        assert "example.com" not in reason


def _raised(exc: Exception) -> Exception:
    """Returns *exc* with a real traceback attached."""
    try:
        raise exc
    except Exception as caught:
        return caught


class TestDescribeUnexpected:
    """Covers what an unexpected failure leaves behind for the alert."""

    def test_records_type_location_and_host(self):
        """Holds what a digest groups by and what the reader needs first."""
        record = describe_unexpected("https://shop.example/item?id=1", _raised(TypeError("boom")))
        assert record["type"] == "TypeError"
        assert record["location"].startswith("test_ingest.py:")
        assert record["host"] == "shop.example"

    def test_keeps_urls_out_of_the_traceback(self):
        """An email leaves the stack, so a URL quoted by an exception must not travel with it."""
        record = describe_unexpected(
            "https://shop.example/a", _raised(ValueError("bad https://shop.example/a?x=1 here"))
        )
        assert "shop.example/a" not in record["traceback"]
        assert "bad URL here" in record["traceback"]

    def test_caps_the_traceback(self):
        """A huge message must not produce a huge email."""
        record = describe_unexpected(URL, _raised(ValueError("x" * 50_000)))
        assert len(record["traceback"]) <= ingest.MAX_ALERT_TRACEBACK_CHARS

    def test_survives_a_url_without_a_host(self):
        """Reporting a failure must never fail itself."""
        assert describe_unexpected("", _raised(ValueError("x")))["host"] == "unknown"


class TestBuildAlert:
    """Covers turning a barrage into one digest."""

    @staticmethod
    def _failure(host: str, exc_type: str = "TypeError", location: str = "ingest.py:60") -> dict[str, str]:
        """Builds one failure record."""
        return {"type": exc_type, "location": location, "host": host, "traceback": f"trace from {host}"}

    def test_a_barrage_from_one_bug_is_one_entry(self):
        """A thousand pages hitting one line must not read like a thousand problems."""
        failures = [self._failure(f"host{i}.example") for i in range(1000)]
        subject, body = build_alert(failures, 2000, 1000)
        assert "1000 unexpected failure(s) in 1 place(s)" in subject
        assert "1000 of 2000 pages" in body
        assert body.count("TypeError at ingest.py:60") == 1
        assert body.count("trace from") == 1

    def test_groups_by_exception_and_location(self):
        """Different bugs stay apart, the most frequent first."""
        failures = [
            self._failure("a.example"),
            self._failure("b.example", "KeyError", "dag.py:9"),
            self._failure("c.example"),
        ]
        subject, body = build_alert(failures, 3, 0)
        assert "in 2 place(s)" in subject
        assert body.index("TypeError at ingest.py:60: 2 page(s)") < body.index("KeyError at dag.py:9: 1 page(s)")

    def test_caps_the_host_list(self):
        """Names a few hosts and counts the rest."""
        failures = [self._failure(f"host{i}.example") for i in range(MAX_ALERT_HOSTS + 3)]
        assert "and 3 more" in build_alert(failures, 100, 90)[1]

    def test_says_the_run_carried_on(self):
        """Tells the reader nothing is stuck and the pages will be retried."""
        body = build_alert([self._failure("a.example")], 2, 1)[1]
        assert "The ingest carried on" in body
        assert "next run retries them" in body

    def test_does_not_claim_the_run_carried_on_when_it_stopped(self):
        """With nothing fetched the run fails, and an alert must not misdescribe that."""
        body = build_alert([self._failure("a.example")], 1, 0)[1]
        assert "carried on" not in body
        assert "this run stopped" in body


class TestWithRetries:
    """Covers giving up, which must never mean skipping."""

    def test_returns_the_first_success(self):
        """Does not wait when nothing failed."""
        waits = []
        assert with_retries(lambda: "ok", label="x", sleep=waits.append) == "ok"
        assert waits == []

    def test_retries_with_exponential_backoff(self):
        """Recovers from transient failures."""
        calls = []
        waits = []

        def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise ValueError("transient")
            return "ok"

        assert with_retries(flaky, label="x", sleep=waits.append) == "ok"
        assert waits == [2, 4]

    def test_raises_when_every_attempt_fails(self):
        """The last failure reaches the caller instead of a silent skip."""
        calls = []

        def broken():
            calls.append(1)
            raise ValueError("still broken")

        with pytest.raises(ValueError, match="still broken"):
            with_retries(broken, label="x", sleep=lambda _: None)
        assert len(calls) == ingest.RETRY_ATTEMPTS


class TestBuildFailureAlert:
    """Covers the email for a run that stopped, which an unattended ingest would otherwise hide."""

    def test_names_the_task_and_the_run(self):
        """Says where the run stopped, in the subject as well."""
        subject, body = build_failure_alert("upsert_to_pinecone", "manual__1", _raised(RuntimeError("Could not purge")))
        assert "upsert_to_pinecone" in subject
        assert "upsert_to_pinecone failed" in body
        assert "manual__1" in body
        assert "Could not purge" in body

    def test_shows_the_failure_reasons_of_an_outage(self):
        """A network outage reads as what it is, from the counts in the exception."""
        body = build_failure_alert("fetch_documents", "r", _raised(RuntimeError("All 3 URLs failed: {'dns': 3}")))[1]
        assert "All 3 URLs failed: {'dns': 3}" in body

    def test_keeps_urls_out_of_the_email(self):
        """An exception quoting a URL must not carry it out of the stack."""
        body = build_failure_alert("t", "r", _raised(ValueError("bad https://shop.example/a?x=1 here")))[1]
        assert "shop.example" not in body

    def test_caps_the_traceback(self):
        """A huge message must not produce a huge email."""
        body = build_failure_alert("t", "r", _raised(ValueError("x" * 50_000)))[1]
        assert len(body) < ingest.MAX_ALERT_TRACEBACK_CHARS + 500

    def test_survives_a_missing_exception(self):
        """Airflow may have no exception to hand over, and reporting must not fail on that."""
        assert "no exception" in build_failure_alert("t", "r", None)[1]


class TestSmtpScript:
    """Covers the connection that scripts/set_smtp_connection.sh stores."""

    def test_implicit_tls_port_skips_starttls(self):
        """Gmail offers no STARTTLS on 465, and the hook calls it unless told not to, even over SSL."""
        script = (Path(__file__).resolve().parents[2] / "scripts" / "set_smtp_connection.sh").read_text(
            encoding="utf-8"
        )
        assert "port=465" in script
        assert '"disable_tls": True' in script

    def test_keeps_the_password_out_of_process_lists(self):
        """The password reaches the container over stdin, never as an argument."""
        script = (Path(__file__).resolve().parents[2] / "scripts" / "set_smtp_connection.sh").read_text(
            encoding="utf-8"
        )
        assert "printf '%s' \"$password\" | docker exec -i" in script
        assert '"$GMAIL_2_USER" 2>/dev/null' in script


class TestDagWiring:
    """Covers how the DAG uses these helpers, because its tasks are closures that tests cannot call."""

    def test_bad_links_and_bugs_are_caught_apart(self):
        """A page can stop neither the batch nor the distinction between a bad link and a bug."""
        assert DAG_SOURCE.index("except FETCH_ERRORS as exc:") < DAG_SOURCE.index("describe_unexpected(url, exc)")

    def test_the_report_survives_a_total_outage(self):
        """Unexpected failures are saved before the outage check can stop the task."""
        saved = DAG_SOURCE.index("_write_unexpected(unexpected, len(filtered_urls), len(documents))")
        assert saved < DAG_SOURCE.index('raise RuntimeError(f"All {len(filtered_urls)} URLs failed')

    def test_the_report_starts_each_run_empty(self):
        """A crash early in a run cannot resend the previous run's failures."""
        assert DAG_SOURCE.index("_write_unexpected([], 0, 0)") < DAG_SOURCE.index("clean_urls(json.loads(")

    def test_the_report_cannot_turn_a_failed_run_green(self):
        """It is a parallel branch: downstream of the upsert, its success would mask a failed run."""
        assert "fetch_result >> report_unexpected_failures()" in DAG_SOURCE
        assert 'upsert_to_pinecone(embedded_path, fetch_result["manifest_path"]) >>' not in DAG_SOURCE
        assert '@task(trigger_rule="all_done")\n    def report_unexpected_failures' in DAG_SOURCE

    def test_every_task_alerts_on_final_failure(self):
        """An outage turns a run red with nobody watching, so every task carries the callback."""
        assert '"on_failure_callback": _alert_run_failure' in DAG_SOURCE

    def test_the_recipient_stays_out_of_the_code(self):
        """The repository is public, so the address lives in an Airflow Variable."""
        assert 'ALERT_RECIPIENT_VAR = "rag_alert_email"' in DAG_SOURCE
        assert "@iu-study.org" not in DAG_SOURCE

    def test_every_failure_mode_stops_before_the_manifest_is_saved(self):
        """A failed purge, a failed batch or a total outage must leave the manifest alone."""
        saved = DAG_SOURCE.index("Variable.set(CONTENT_HASHES_VAR")
        assert DAG_SOURCE.index('raise RuntimeError(f"All {len(filtered_urls)} URLs failed') < saved
        assert DAG_SOURCE.index('raise RuntimeError(f"Could not purge') < saved
        assert DAG_SOURCE.index('label=f"Embedding batch {batch_num}"') < saved
        assert DAG_SOURCE.index('label=f"Upsert batch {bnum}"') < saved

    def test_no_batch_can_be_skipped(self):
        """Nothing is left of the paths that dropped batches and went green."""
        assert "skipping %d" not in DAG_SOURCE
        assert "chunks_skipped" not in DAG_SOURCE

    def test_only_a_missing_manifest_means_a_first_run(self):
        """Any other failure to read it must not pass for an empty manifest."""
        assert 'Variable.get(CONTENT_HASHES_VAR, default="{}")' in DAG_SOURCE
