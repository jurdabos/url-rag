"""
CLI entry point for url-rag.

Provides the ``push`` subcommand (shared from :mod:`acidbase.push`) and
the project-specific ``query`` subcommand that asks a question against
the RAG knowledge base.
"""

from __future__ import annotations

import click
from acidbase.push import push_command


@click.group()
def cli() -> None:
    """CLI tools for the url-rag project."""


cli.add_command(push_command)


@cli.command("query")
@click.argument("question")
@click.option("--top-k", "-k", default=5, show_default=True, help="Number of chunks to retrieve")
@click.option("--verbose", "-v", is_flag=True, help="Show sources and chunk count")
def query_cmd(question: str, top_k: int, verbose: bool) -> None:
    """Asks a question against the RAG knowledge base.

    Example:  uv run url-rag query "Where is Casa del Libro?"
    """
    from url_rag.rag import ask

    click.echo(click.style("\u23f3 Querying knowledge base ...", fg="cyan"))
    try:
        result = ask(question, top_k=top_k)
    except Exception as exc:
        click.echo(click.style(f"\u2717 {exc}", fg="red"), err=True)
        raise SystemExit(1) from exc
    click.echo(f"\n{result['answer']}")
    if verbose:
        click.echo(
            click.style(
                f"\n\u2014 {result['chunks_retrieved']} chunks from {len(result['sources'])} source(s):",
                dim=True,
            )
        )
        for src in result["sources"]:
            click.echo(click.style(f"  \u2022 {src}", dim=True))


def main() -> None:
    """Entry point for the CLI."""
    cli()
