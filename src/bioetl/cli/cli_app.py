"""Typer application exposing BioETL pipelines.

The CLI is intentionally thin: it resolves pipeline factories from the
registry and delegates execution to the orchestration layer without
leaking infrastructure or domain details.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import typer

from bioetl.cli.cli_registry import PIPELINE_REGISTRY
from bioetl.pipelines.base import PipelineBase

app = typer.Typer(name="bioetl", help="Command line entrypoint for BioETL pipelines")


@app.callback(invoke_without_command=True)
def main() -> None:
    """BioETL multi-command entrypoint."""



def create_pipeline_command(pipeline_name: str) -> Callable[..., None]:
    """Create a Typer command bound to a pipeline factory.

    The generated command resolves the pipeline from :data:`PIPELINE_REGISTRY`
    and forwards run options to the pipeline's :py:meth:`run` method.
    """

    def _command(
        run_id: str = typer.Option(..., "--run-id", help="Unique pipeline run identifier"),
        output_dir: Path = typer.Option(..., "--output-dir", exists=False, help="Target directory for outputs"),
        extended: bool = typer.Option(False, help="Enable extended extraction or transformation steps"),
        include_qc_metrics: bool = typer.Option(
            False, help="Persist optional quality-control metrics when supported"
        ),
        **options: Any,
    ) -> None:
        factory = PIPELINE_REGISTRY.get(pipeline_name)
        if factory is None:
            raise typer.Exit(code=1, message=f"Pipeline '{pipeline_name}' is not registered")

        pipeline = factory(run_id=run_id)
        pipeline.run(
            output_dir=output_dir,
            extended=extended,
            include_qc_metrics=include_qc_metrics,
            **options,
        )

    return _command


@app.command("list")
def list_pipelines() -> None:
    """List registered pipelines available to the CLI."""

    if not PIPELINE_REGISTRY:
        typer.echo("No pipelines registered yet.")
        return

    typer.echo("Available pipelines:")
    for name in sorted(PIPELINE_REGISTRY):
        typer.echo(f"- {name}")


if __name__ == "__main__":  # pragma: no cover
    app()
