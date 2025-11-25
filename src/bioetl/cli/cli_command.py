from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import typer
from rich.progress import Progress

from bioetl.config import PipelineConfig, load_config

PipelineFactory = Callable[[PipelineConfig, str | None], Any]


def _parse_cli_overrides(pairs: list[str]) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for pair in pairs:
        if "=" not in pair:
            msg = "--set expects KEY=VALUE pairs"
            raise typer.BadParameter(msg)
        key, raw_value = pair.split("=", 1)
        overrides[key.strip()] = raw_value
    return overrides


def create_pipeline_command(name: str, factory: PipelineFactory) -> Callable[..., None]:
    """Create a Typer command bound to the provided ``factory``."""

    def command(
        config: Path = typer.Option(..., exists=True, help="Path to pipeline config"),
        profile: list[str] = typer.Option(None, help="Additional profile files"),
        set: list[str] = typer.Option(  # noqa: A002 - public CLI flag
            None,
            help="Override config values (KEY=VALUE)",
            metavar="KEY=VALUE",
        ),
        run_id: str | None = typer.Option(None, help="Explicit run identifier"),
        output_dir: Path = typer.Option(Path("data/output"), help="Output directory"),
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
            if progress:
                with Progress() as bar:
                    task = bar.add_task(f"Running {name}", total=1)
                    result = pipeline.run(output_dir)
                    bar.update(task, advance=1)
            else:
                result = pipeline.run(output_dir)
            if getattr(result, "success", True) is False:
                message = getattr(result, "error", None) or "Pipeline failed"
                raise RuntimeError(message)
            typer.secho(f"Pipeline {name} finished", fg=typer.colors.GREEN)
        except FileNotFoundError as exc:
            typer.secho(str(exc), err=True, fg=typer.colors.RED)
            raise typer.Exit(code=2)
        except typer.BadParameter:
            raise
        except Exception as exc:  # pragma: no cover - defensive fallback
            typer.secho(str(exc), err=True, fg=typer.colors.RED)
            raise typer.Exit(code=3)

    command.__name__ = f"run_{name}"
    command.__doc__ = f"Run the {name} pipeline"
    return command


__all__ = ["create_pipeline_command"]
