"""Check project output artifacts for tracked files and size limits."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from bioetl.cli._io import git_diff_cached, git_ls
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_CONFIG, CLI_ERROR_INTERNAL
from bioetl.devtools.typer_helpers import TyperApp, get_typer, register_tool_app
from bioetl.tools import get_project_root

__all__ = [
    "MAX_BYTES",
    "check_output_artifacts",
    "app",
    "cli_main",
    "run",
]


MAX_BYTES = 1_000_000
IGNORED_NAMES = {".gitkeep"}
typer: Any = get_typer()


def check_output_artifacts(
    max_bytes: int = MAX_BYTES,
    *,
    git_ls_fn: Callable[[str], Sequence[Path]] | None = None,
    git_diff_fn: Callable[[str], Sequence[Path]] | None = None,
    get_root_fn: Callable[[], Path] | None = None,
    logger_cls: type[UnifiedLogger] | None = None,
) -> list[str]:
    """Validate `data/output` artifacts and return a list of issues."""

    logger_type = logger_cls or UnifiedLogger
    logger_type.configure()
    log = logger_type.get(__name__)

    root_factory = get_root_fn or get_project_root
    repo_root = root_factory()
    output_dir = repo_root / "data" / "output"

    log.info(
        LogEvents.CHECKING_OUTPUT_ARTIFACTS,
        output_dir=str(output_dir),
        max_bytes=max_bytes,
    )

    tracked_source = git_ls_fn or git_ls
    staged_source = git_diff_fn or git_diff_cached

    tracked = [
        p for p in tracked_source("data/output") if p.name not in IGNORED_NAMES
    ]
    staged = [
        p for p in staged_source("data/output") if p.name not in IGNORED_NAMES
    ]

    errors: list[str] = []

    if tracked:
        tracked_list = "\n".join(f"  - {path}" for path in tracked)
        errors.append(
            "Tracked artifacts detected under data/output. Move long-term datasets to external storage:\n"
            f"{tracked_list}"
        )

    if staged:
        staged_list = "\n".join(f"  - {path}" for path in staged)
        errors.append(
            "New artifacts staged under data/output. Commit lightweight samples under data/samples instead:\n"
            f"{staged_list}"
        )

    oversized: list[tuple[Path, int]] = []
    if output_dir.exists():
        for file_path in output_dir.rglob("*"):
            if not file_path.is_file() or file_path.name in IGNORED_NAMES:
                continue
            size = file_path.stat().st_size
            if size > max_bytes:
                oversized.append((file_path.relative_to(repo_root), size))

    if oversized:
        formatted = "\n".join(
            f"  - {path} ({size / 1_000_000:.2f} MB)"
            for path, size in oversized
        )
        errors.append(
            "Large files found in data/output. Upload them to the external bucket and keep only small samples locally:\n"
            f"{formatted}"
        )

    log.info(LogEvents.OUTPUT_ARTIFACT_CHECK_COMPLETED, issues=len(errors))
    return errors


def cli_main(
    max_bytes: int = typer.Option(
        MAX_BYTES,
        "--max-bytes",
        help="File size threshold (bytes) above which a file is flagged as large.",
    ),
) -> None:
    """Run the output artifact inspection."""

    errors: list[str]

    try:
        errors = check_output_artifacts(max_bytes=max_bytes)
    except typer.Exit:
        raise
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Output artifact inspection failed: {exc}",
            context={
                "command": "bioetl-check-output-artifacts",
                "max_bytes": max_bytes,
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    if errors:
        for message in errors:
            typer.echo(message)
        CliCommandBase.emit_error(
            template=CLI_ERROR_CONFIG,
            message=f"Found {len(errors)} output artifacts exceeding limits",
            context={
                "command": "bioetl-check-output-artifacts",
                "max_bytes": max_bytes,
                "error_count": len(errors),
            },
            exit_code=1,
        )

    typer.echo("data/output directory is clean")
    CliCommandBase.exit(0)


app: TyperApp
run: Callable[[], None]
app, run = register_tool_app(
    name="bioetl-check-output-artifacts",
    help_text="Inspect the data/output directory and flag artifacts",
    main_fn=cli_main,
)
