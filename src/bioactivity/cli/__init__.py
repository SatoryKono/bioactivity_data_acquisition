"""Command line interface exposing the document ETL pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import typer

import library.cli as base_cli
from bioactivity.config import (
    ALLOWED_SOURCES,
    DEFAULT_ENV_PREFIX,
    ConfigLoadError,
    load_document_config,
)
from bioactivity.documents.pipeline import (
    DocumentHTTPError,
    DocumentIOError,
    DocumentQCError,
    DocumentValidationError,
    read_document_input,
    run_document_etl,
    write_document_outputs,
)
from library.config import _assign_path


class ExitCode(int):
    """Enumerated exit codes for the document CLI."""

    OK = 0
    VALIDATION_ERROR = 1
    HTTP_ERROR = 2
    QC_ERROR = 3
    IO_ERROR = 4


app = typer.Typer(help=base_cli.app.info.help)
app.command()(base_cli.pipeline)
app.command()(base_cli.version)


def _normalise_sources(raw_sources: Iterable[str]) -> List[str]:
    seen: Dict[str, None] = {}
    for item in raw_sources:
        key = item.lower()
        if key not in ALLOWED_SOURCES:
            raise DocumentValidationError(
                f"Unsupported source '{item}'. Valid options: {', '.join(ALLOWED_SOURCES)}"
            )
        seen.setdefault(key, None)
    return list(seen.keys())


def _build_cli_overrides(
    *,
    documents_csv: Path | None,
    output_dir: Path | None,
    date_tag: str | None,
    timeout_sec: float | None,
    retries: int | None,
    workers: int | None,
    limit: int | None,
    sources: List[str],
    dry_run: Optional[bool],
) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    if documents_csv is not None:
        _assign_path(overrides, ["io", "input", "documents_csv"], str(documents_csv))
    if output_dir is not None:
        _assign_path(overrides, ["io", "output", "dir"], str(output_dir))
    if date_tag is not None:
        _assign_path(overrides, ["runtime", "date_tag"], date_tag)
    if timeout_sec is not None:
        _assign_path(overrides, ["http", "global", "timeout_sec"], timeout_sec)
    if retries is not None:
        _assign_path(overrides, ["http", "global", "retries", "total"], retries)
    if workers is not None:
        _assign_path(overrides, ["runtime", "workers"], workers)
    if limit is not None:
        _assign_path(overrides, ["runtime", "limit"], limit)
    if sources:
        overrides["sources"] = {name: {"enabled": True} for name in sources}
        for name in ALLOWED_SOURCES:
            overrides["sources"].setdefault(name, {"enabled": False})
    if dry_run is not None:
        _assign_path(overrides, ["runtime", "dry_run"], dry_run)
    return overrides


@app.command("get-document-data")
def get_document_data(
    *,
    config: Path | None = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to the YAML configuration file.",
        dir_okay=False,
        resolve_path=True,
    ),
    documents_csv: Path | None = typer.Option(
        None,
        "--documents-csv",
        help="CSV containing document identifiers to enrich.",
        file_okay=True,
        dir_okay=False,
        resolve_path=True,
    ),
    output_dir: Path | None = typer.Option(
        None,
        "--output-dir",
        help="Destination directory for generated artefacts.",
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
    ),
    date_tag: str | None = typer.Option(
        None,
        "--date-tag",
        help="Date tag (YYYYMMDD) embedded in output filenames.",
    ),
    timeout_sec: float | None = typer.Option(
        None,
        "--timeout-sec",
        help="HTTP timeout in seconds applied to all sources.",
    ),
    retries: int | None = typer.Option(
        None,
        "--retries",
        help="Total retry attempts for HTTP requests.",
    ),
    workers: int | None = typer.Option(
        None,
        "--workers",
        help="Number of worker threads for HTTP fetching.",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        help="Limit the number of documents processed.",
    ),
    sources: List[str] = typer.Option(
        [],
        "--source",
        help="Enable only the listed sources (repeat for multiple).",
    ),
    dry_run: Optional[bool] = typer.Option(
        None,
        "--dry-run/--no-dry-run",
        help="Execute without writing artefacts to disk.",
    ),
) -> None:
    """Collect and enrich document metadata from configured sources."""

    try:
        normalised_sources = _normalise_sources(sources)
    except DocumentValidationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc

    overrides = _build_cli_overrides(
        documents_csv=documents_csv,
        output_dir=output_dir,
        date_tag=date_tag,
        timeout_sec=timeout_sec,
        retries=retries,
        workers=workers,
        limit=limit,
        sources=normalised_sources,
        dry_run=dry_run,
    )

    try:
        config_model = load_document_config(
            config,
            overrides=overrides,
            env_prefix=DEFAULT_ENV_PREFIX,
        )
    except ConfigLoadError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc

    if not config_model.enabled_sources():
        typer.echo("At least one document source must be enabled", err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR)

    try:
        input_frame = read_document_input(config_model.io.input.documents_csv)
    except DocumentValidationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc
    except DocumentIOError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.IO_ERROR) from exc

    try:
        result = run_document_etl(config_model, input_frame)
    except DocumentValidationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc
    except DocumentHTTPError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.HTTP_ERROR) from exc
    except DocumentQCError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.QC_ERROR) from exc

    if config_model.runtime.dry_run:
        typer.echo("Dry run completed; no artefacts written.")
        raise typer.Exit(code=ExitCode.OK)

    try:
        outputs = write_document_outputs(
            result,
            config_model.io.output.dir,
            config_model.runtime.date_tag,
        )
    except DocumentIOError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.IO_ERROR) from exc

    for name, path in outputs.items():
        typer.echo(f"{name}: {path}")


def main() -> None:
    """Entrypoint for ``python -m bioactivity.cli``."""

    app()


__all__ = ["ExitCode", "app", "get_document_data", "main"]
