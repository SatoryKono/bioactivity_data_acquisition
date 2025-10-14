"""Factory to build Typer applications for individual pipelines."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer

from ..config_loader import (
    AppConfig,
    DEFAULT_CONFIG_PATH,
    RuntimeOverrides,
    build_runtime,
)
from ..pipelines.registry import get_pipeline

_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)


def create_pipeline_app(pipeline_name: str) -> typer.Typer:
    """Create a Typer application with a shared interface."""

    _configure_logging()
    app = typer.Typer(add_completion=False, help=f"Run the {pipeline_name} pipeline")

    @app.command()
    def run(
        config: Path = typer.Option(  # type: ignore[arg-type]
            DEFAULT_CONFIG_PATH,
            "--config",
            "-c",
            help="Path to the pipeline configuration file",
        ),
        limit: Optional[int] = typer.Option(
            None, help="Override the configured record limit"
        ),
        output_dir: Optional[Path] = typer.Option(
            None,
            help="Directory where pipeline artifacts will be written",
        ),
        date_tag: Optional[str] = typer.Option(
            None,
            help="Date tag applied to generated artifacts (YYYYMMDD)",
        ),
        dry_run: Optional[bool] = typer.Option(
            None,
            "--dry-run/--no-dry-run",
            help="Toggle dry-run execution mode",
        ),
        postprocess: Optional[bool] = typer.Option(
            None,
            "--postprocess/--no-postprocess",
            help="Toggle the post-processing stage",
        ),
    ) -> None:
        """Execute the configured pipeline."""

        overrides = RuntimeOverrides(
            limit=limit,
            output_dir=output_dir,
            date_tag=date_tag,
            dry_run=dry_run,
            postprocess=postprocess,
        )

        config_model = AppConfig.from_path(config)
        runtime = build_runtime(config_model, pipeline_name, overrides)
        runtime.output_dir.mkdir(parents=True, exist_ok=True)

        handler = get_pipeline(pipeline_name)
        handler(runtime)

    return app
