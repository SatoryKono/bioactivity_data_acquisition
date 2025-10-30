"""CLI interface using typer."""

from __future__ import annotations

import typer

from bioetl.core.logger import UnifiedLogger
from scripts import PIPELINE_COMMAND_REGISTRY, register_pipeline_command

app = typer.Typer(help="BioETL - Unified ETL framework for bioactivity data")

for key in sorted(PIPELINE_COMMAND_REGISTRY):
    register_pipeline_command(app, key)


@app.command(name="list")
def list_pipelines() -> None:
    """List available pipelines."""
    UnifiedLogger.setup(mode="production")
    typer.echo("Available pipelines:")
    for key in sorted(PIPELINE_COMMAND_REGISTRY):
        config = PIPELINE_COMMAND_REGISTRY[key]
        description = config.description or config.pipeline_name
        typer.echo(f"  - {key} ({description})")


if __name__ == "__main__":
    app()

