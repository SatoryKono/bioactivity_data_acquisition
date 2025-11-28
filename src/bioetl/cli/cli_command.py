from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Callable, Mapping

import typer
import yaml
from pydantic import ValidationError
from rich.progress import Progress

from bioetl.core.config import PipelineConfig, load_config
from bioetl.core.pipeline.types import PipelineBaseProtocol

PipelineFactory = Callable[[PipelineConfig, str | None], PipelineBaseProtocol]

CONFIG_EXCEPTIONS: tuple[type[Exception], ...] = (FileNotFoundError, TypeError, ValueError, ValidationError)
try:  # pragma: no cover - optional dependency
    from requests import exceptions as requests_exc

    DEP_EXCEPTIONS: tuple[type[Exception], ...] = (requests_exc.RequestException, ConnectionError, TimeoutError)
except Exception:  # pragma: no cover - fall back without requests
    DEP_EXCEPTIONS = (ConnectionError, TimeoutError)


def _parse_cli_overrides(pairs: list[str]) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for pair in pairs:
        if "=" not in pair:
            msg = "--set expects KEY=VALUE pairs"
            raise typer.BadParameter(msg)
        key, raw_value = pair.split("=", 1)
        overrides[key.strip()] = yaml.safe_load(raw_value)
    return overrides


def _filter_run_kwargs(pipeline: Any, options: Mapping[str, Any]) -> dict[str, Any]:
    signature = inspect.signature(pipeline.run)
    accepted = set(signature.parameters)
    return {key: value for key, value in options.items() if key in accepted}


def _handle_cli_exception(exc: Exception) -> None:
    if isinstance(exc, CONFIG_EXCEPTIONS):
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=2)
    if isinstance(exc, DEP_EXCEPTIONS):
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=3)
    typer.secho(str(exc), err=True, fg=typer.colors.RED)
    raise typer.Exit(code=1)


def create_pipeline_command(name: str, factory: PipelineFactory) -> Callable[..., None]:
    """Create a Typer command bound to the provided ``factory``.

    The command supports config layering (profiles, ``--set`` overrides) and maps
    exceptions to exit codes: ``1`` — general error, ``2`` — configuration
    errors, ``3`` — external dependency failures.
    """

    def command(
        config: Path = typer.Option(..., exists=True, help="Path to pipeline config"),
        profile: list[str] | None = typer.Option(None, help="Additional profile files"),
        set: list[str] | None = typer.Option(  # noqa: A002 - public CLI flag
            None,
            help="Override config values (KEY=VALUE)",
            metavar="KEY=VALUE",
        ),
        run_id: str | None = typer.Option(None, help="Explicit run identifier"),
        output_dir: Path = typer.Option(Path("data/output"), help="Output directory"),
        extended: bool = typer.Option(False, help="Persist metadata and manifest"),
        dry_run: bool = typer.Option(False, help="Skip extraction and write empty dataset"),
        sample: int | None = typer.Option(None, help="Sample N rows after extraction"),
        limit: int | None = typer.Option(None, help="Limit rows after extraction"),
        golden: bool = typer.Option(False, help="Emit QC/golden artifacts when supported"),
        verbose: bool = typer.Option(False, help="Enable verbose output when supported"),
        fail_on_schema_drift: bool = typer.Option(
            True, help="Fail when output schema deviates from reference"
        ),
        validate_columns: bool = typer.Option(
            False, help="Validate column ordering/contents when supported"
        ),
        progress: bool = typer.Option(False, help="Show progress bar"),
    ) -> None:
        try:
            cli_overrides = _parse_cli_overrides(set or [])
            pipeline_config = load_config(
                config,
                profiles=profile or (),
                cli_overrides=cli_overrides,
            )
            pipeline = factory(pipeline_config, run_id)
            run_kwargs = _filter_run_kwargs(
                pipeline,
                {
                    "extended": extended,
                    "dry_run": dry_run,
                    "sample": sample,
                    "limit": limit,
                    "include_qc_metrics": golden,
                    "golden": golden,
                    "verbose": verbose,
                    "fail_on_schema_drift": fail_on_schema_drift,
                    "validate_columns": validate_columns,
                },
            )
            if progress:
                with Progress() as bar:
                    task = bar.add_task(f"Running {name}", total=1)
                    result = pipeline.run(output_dir, **run_kwargs)
                    bar.update(task, advance=1)
            else:
                result = pipeline.run(output_dir, **run_kwargs)
            if getattr(result, "success", True) is False:
                message = getattr(result, "error", None) or "Pipeline failed"
                raise RuntimeError(message)
            typer.secho(f"Pipeline {name} finished", fg=typer.colors.GREEN)
        except typer.BadParameter:
            raise
        except Exception as exc:  # pragma: no cover - defensive fallback
            _handle_cli_exception(exc)

    command.__name__ = f"run_{name}"
    command.__doc__ = f"Run the {name} pipeline"
    return command


__all__ = ["create_pipeline_command", "_handle_cli_exception", "_parse_cli_overrides"]
