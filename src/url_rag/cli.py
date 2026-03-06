"""
Reusable push CLI — auto-detects project name from pyproject.toml.

Provides the ``push`` subcommand that automates the git commit-and-push
workflow, including pre-commit hook retry logic and optional DVC integration.
"""
import shutil
import subprocess
import tomllib
from pathlib import Path

import click

# Byte threshold above which an untracked file is auto-added to DVC
DEFAULT_SIZE_THRESHOLD = 1_048_576  # 1 MB

# Extensions always routed through DVC regardless of size
DVC_EXTENSIONS: set[str] = {
    # Images
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif", ".webp",
    ".heic", ".heif", ".svg", ".ico", ".raw", ".cr2", ".nef", ".arw",
    # Video
    ".mp4", ".mov", ".avi", ".mkv", ".webm", ".ts", ".flv", ".wmv",
    # Audio
    ".mp3", ".wav", ".m4a", ".ogg", ".flac", ".aac", ".wma",
    # ML artefacts
    ".h5", ".hdf5", ".pkl", ".pickle", ".pt", ".pth", ".onnx",
    ".safetensors", ".bin", ".npy", ".npz",
    # Archives & data
    ".zip", ".tar", ".gz", ".bz2", ".7z", ".rar",
    ".csv", ".parquet", ".feather", ".arrow",
    ".db", ".sqlite", ".sqlite3",
}

# Extensions that are clearly source/config and should stay in git only
_CODE_EXTENSIONS: set[str] = {
    ".py", ".pyi", ".md", ".rst", ".txt", ".toml", ".yaml", ".yml",
    ".json", ".cfg", ".ini", ".sh", ".ps1", ".bat", ".cmd",
    ".html", ".css", ".js", ".ts", ".jsx", ".tsx",
    ".lock", ".gitignore", ".gitattributes", ".dvcignore",
}


def _get_project_root() -> Path:
    """Locates the project root by walking up to find pyproject.toml."""
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return current


def _get_project_name() -> str:
    """Reads the project name from pyproject.toml, falling back to directory name."""
    pyproject = _get_project_root() / "pyproject.toml"
    if pyproject.exists():
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        return data.get("project", {}).get("name", pyproject.parent.name)
    return Path.cwd().name


def _has_dvc() -> bool:
    """Checks whether DVC is available on PATH and the project has .dvc/."""
    return shutil.which("dvc") is not None and (_get_project_root() / ".dvc").is_dir()


def _run(cmd: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Runs a subprocess command, returning CompletedProcess."""
    return subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=check,
    )


def _has_changes(root: Path) -> bool:
    """Checks whether the working tree has any staged or unstaged changes."""
    result = _run(["git", "status", "--porcelain"], cwd=root, check=False)
    return bool(result.stdout.strip())


def _hooks_modified_files(output: str) -> bool:
    """Checks whether pre-commit hooks modified files (retryable failure)."""
    return "files were modified by this hook" in output.lower()


def _find_untracked_for_dvc(root: Path, size_threshold: int) -> list[str]:
    """
    Finds untracked files that should be DVC-tracked.

    Matching by known binary/data extension or by exceeding the size threshold.
    """
    result = _run(["git", "status", "--porcelain"], cwd=root, check=False)
    candidates: list[str] = []
    for line in result.stdout.strip().splitlines():
        if not line.startswith("?? "):
            continue
        rel_path = line[3:].strip().strip('"')
        full_path = root / rel_path
        if not full_path.is_file():
            continue
        # Skipping DVC pointers and gitignore files
        if rel_path.endswith(".dvc") or rel_path.endswith(".gitignore"):
            continue
        suffix = full_path.suffix.lower()
        # Skipping known source/config files
        if suffix in _CODE_EXTENSIONS:
            continue
        # Adding if extension matches DVC set or file exceeds size threshold
        if suffix in DVC_EXTENSIONS or full_path.stat().st_size >= size_threshold:
            candidates.append(rel_path)
    return candidates


def _find_dvc_changed_outs(root: Path) -> list[str]:
    """
    Finds DVC-tracked outputs that have been modified since last ``dvc add``.

    Parses ``dvc status`` text output for 'modified:' lines.
    """
    result = _run(["dvc", "status"], cwd=root, check=False)
    if not result.stdout.strip() or "no changes" in result.stdout.lower():
        return []
    changed: list[str] = []
    for line in result.stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("modified:"):
            path = stripped.split(":", 1)[1].strip()
            changed.append(path)
    return changed


def _auto_commit_message(dvc_files: list[str]) -> str:
    """Generates an automatic commit message from DVC-added file names."""
    if dvc_files:
        names = ", ".join(Path(f).name for f in dvc_files[:3])
        suffix = f" (+{len(dvc_files) - 3} more)" if len(dvc_files) > 3 else ""
        return f"chore: ingest {names}{suffix}"
    return "chore: update tracked files"


@click.group()
def cli() -> None:
    """CLI tools for the url-rag project."""


@cli.command("query")
@click.argument("question")
@click.option("--top-k", "-k", default=5, show_default=True, help="Number of chunks to retrieve")
@click.option("--verbose", "-v", is_flag=True, help="Show sources and chunk count")
def query_cmd(question: str, top_k: int, verbose: bool) -> None:
    """Asks a question against the RAG knowledge base.

    Example:  uv run url-rag query "Where is Casa del Libro?"
    """
    from url_rag.rag import ask
    click.echo(click.style("⏳ Querying knowledge base ...", fg="cyan"))
    try:
        result = ask(question, top_k=top_k)
    except Exception as exc:
        click.echo(click.style(f"✗ {exc}", fg="red"), err=True)
        raise SystemExit(1)
    click.echo(f"\n{result['answer']}")
    if verbose:
        click.echo(click.style(f"\n— {result['chunks_retrieved']} chunks from {len(result['sources'])} source(s):", dim=True))
        for src in result["sources"]:
            click.echo(click.style(f"  • {src}", dim=True))


@cli.command("push")
@click.option("--message", "-m", default=None, help="Custom commit message")
@click.option("--dry-run", is_flag=True, help="Preview without making changes")
@click.option(
    "--size-threshold",
    default=DEFAULT_SIZE_THRESHOLD,
    type=int,
    show_default=True,
    help="Min file size in bytes for auto-DVC tracking",
)
def push(message: str | None, dry_run: bool, size_threshold: int) -> None:
    """
    Stages, commits, and pushes everything.

    Automates the full workflow:
    1. (DVC) Detects untracked data/binary files and runs ``dvc add``
    2. (DVC) Detects modified DVC-tracked files and re-adds them
    3. Stages all changes with ``git add .``
    4. Commits with pre-commit hook retry (up to 3 attempts)
    5. Amends if post-commit hooks leave dirty state
    6. (DVC) ``dvc push`` to remote storage
    7. ``git push`` to GitHub
    """
    root = _get_project_root()
    name = _get_project_name()
    use_dvc = _has_dvc()
    click.echo(click.style(f"\n=== {name} push ===", fg="cyan", bold=True))
    click.echo(f"Root: {root}")
    click.echo(f"DVC:  {'enabled' if use_dvc else 'not configured'}\n")
    all_dvc_files: list[str] = []
    # Step 1–2: DVC ingest (only when DVC is configured)
    if use_dvc:
        new_for_dvc = _find_untracked_for_dvc(root, size_threshold)
        if new_for_dvc:
            click.echo(click.style(f"① {len(new_for_dvc)} new file(s) to DVC-track:", bold=True))
            for f in new_for_dvc:
                size_mb = (root / f).stat().st_size / 1_048_576
                click.echo(f"   {f}  ({size_mb:.1f} MB)")
            if not dry_run:
                for f in new_for_dvc:
                    _run(["dvc", "add", f], cwd=root)
                    click.echo(f"   ✓ dvc add {f}")
            all_dvc_files.extend(new_for_dvc)
        else:
            click.echo("① No new files to DVC-track")
        # Finding modified DVC-tracked outputs that need re-adding
        changed_outs = _find_dvc_changed_outs(root)
        if changed_outs:
            click.echo(click.style(f"\n② {len(changed_outs)} modified DVC output(s) to re-add:", bold=True))
            for f in changed_outs:
                click.echo(f"   {f}")
            if not dry_run:
                for f in changed_outs:
                    _run(["dvc", "add", f], cwd=root)
                    click.echo(f"   ✓ dvc add {f}")
            all_dvc_files.extend(changed_outs)
        else:
            click.echo("② No modified DVC outputs")
    else:
        click.echo("① DVC not configured — skipping ingest steps")
    # Summarising all git changes
    status_result = _run(["git", "status", "--porcelain"], cwd=root, check=False)
    status_lines = [ln for ln in status_result.stdout.strip().splitlines() if ln.strip()]
    if not status_lines:
        click.echo(click.style("\n✓ Nothing to push — working tree is clean.", fg="green"))
        return
    click.echo(click.style(f"\n③ {len(status_lines)} git change(s) detected:", bold=True))
    for ln in status_lines:
        click.echo(f"   {ln}")
    if dry_run:
        click.echo(click.style("\n── dry run ── no changes made", fg="yellow"))
        return
    # Staging all changes
    click.echo(click.style("\n④ Staging all changes ...", bold=True))
    _run(["git", "add", "."], cwd=root)
    staged = _run(["git", "diff", "--cached", "--name-only"], cwd=root, check=False)
    if not staged.stdout.strip():
        click.echo(click.style("   ⚠ Nothing staged after git add — skipping commit.", fg="yellow"))
    else:
        staged_count = len(staged.stdout.strip().splitlines())
        click.echo(f"   {staged_count} file(s) staged")
        # Committing with pre-commit hook retry
        commit_msg = message or _auto_commit_message(all_dvc_files)
        full_msg = f"{commit_msg}\n\nCo-Authored-By: Warp <agent@warp.dev>"
        click.echo(click.style("\n⑤ Committing ...", bold=True))
        max_attempts = 3
        committed = False
        for attempt in range(1, max_attempts + 1):
            try:
                _run(["git", "commit", "-m", full_msg], cwd=root)
                label = f" (attempt {attempt})" if attempt > 1 else ""
                click.echo(f"   ✓ Committed{label}")
                committed = True
                break
            except subprocess.CalledProcessError as exc:
                combined = exc.stdout + exc.stderr
                if _hooks_modified_files(combined) and attempt < max_attempts:
                    click.echo(f"   ⟳ Pre-commit hooks modified files (attempt {attempt}) — re-staging ...")
                    _run(["git", "add", "."], cwd=root)
                    continue
                click.echo(
                    click.style(f"   ✗ Commit failed (attempt {attempt}): {exc.stderr.strip()}", fg="red"),
                    err=True,
                )
                raise SystemExit(1)
        # Handling post-commit hooks that leave dirty state
        if committed and _has_changes(root):
            click.echo("   ⟳ Post-commit hook left changes — amending ...")
            _run(["git", "add", "."], cwd=root)
            _run(["git", "commit", "--amend", "--no-edit", "--no-verify"], cwd=root, check=False)
            click.echo("   ✓ Amended")
    # DVC push (only when DVC is configured)
    if use_dvc:
        click.echo(click.style("\n⑥ DVC push ...", bold=True))
        try:
            _run(["dvc", "push"], cwd=root)
            click.echo("   ✓ Pushed to DVC remote")
        except subprocess.CalledProcessError as exc:
            click.echo(click.style(f"   ✗ dvc push failed: {exc.stderr.strip()}", fg="red"), err=True)
            raise SystemExit(1)
    # Git push
    click.echo(click.style("\n⑦ Pushing to GitHub ...", bold=True))
    try:
        _run(["git", "push"], cwd=root)
        click.echo("   ✓ Pushed to GitHub")
    except subprocess.CalledProcessError as exc:
        click.echo(click.style(f"   ✗ git push failed: {exc.stderr.strip()}", fg="red"), err=True)
        raise SystemExit(1)
    click.echo(click.style("\n✓ All done — changes committed & pushed.", fg="green", bold=True))


def main() -> None:
    """Entry point for the CLI."""
    cli()
