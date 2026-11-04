"""
Airflow plugin that exposes :mod:`include.url_filter` helpers as Jinja macros.

After this plugin is loaded, DAG authors can use the helpers directly in
templated fields, e. g.::

    BashOperator(
        task_id="echo_decision",
        bash_command=(
            "echo skip-reason="
            "{{ macros.url_rag.should_skip_url(params.url) | default('keep', true) }}"
        ),
        params={"url": "https://example.com/login"},
    )

The pure logic lives in ``include/url_filter.py``, which the Airflow image can
import and the unit tests can run without spinning up Airflow.
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from include.url_filter import (
    DEFAULT_SKIP_DOMAINS,
    DEFAULT_SKIP_EXTENSIONS,
    DEFAULT_SKIP_PATH_KEYWORDS,
    clean_url,
    content_hash,
    should_skip_url,
)


class UrlHelpersPlugin(AirflowPlugin):
    """Registers URL-filter helpers under the ``url_rag`` macros namespace."""

    name = "url_rag"
    macros = [
        clean_url,
        should_skip_url,
        content_hash,
    ]
    # Constants are surfaced as macros too so DAGs can introspect / override.
    macros_extra = {
        "DEFAULT_SKIP_EXTENSIONS": DEFAULT_SKIP_EXTENSIONS,
        "DEFAULT_SKIP_DOMAINS": DEFAULT_SKIP_DOMAINS,
        "DEFAULT_SKIP_PATH_KEYWORDS": DEFAULT_SKIP_PATH_KEYWORDS,
    }
