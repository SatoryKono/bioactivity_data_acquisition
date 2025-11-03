"""Typer-powered CLI wired to the canonical pipeline registry."""

from __future__ import annotations

import typer

from bioetl.cli.app import (
    PIPELINE_COMMAND_REGISTRY,
    PIPELINE_REGISTRY,
    register_pipeline_command,
)
from bioetl.core.logger import UnifiedLogger

app = typer.Typer(help="BioETL - Unified ETL framework for bioactivity data")

for key in sorted(PIPELINE_REGISTRY):
    register_pipeline_command(app, key)

for legacy_key in sorted(PIPELINE_COMMAND_REGISTRY):
    if legacy_key not in PIPELINE_REGISTRY:
        register_pipeline_command(app, legacy_key)


@app.command(name="list")
def list_pipelines() -> None:
    """List available pipelines."""
    UnifiedLogger.setup(mode="production")
    typer.echo("Available pipelines:")
    for key in sorted(PIPELINE_REGISTRY):
        config = PIPELINE_REGISTRY[key]
        description = config.description or config.pipeline_name
        typer.echo(f"  - {key} ({description})")


if __name__ == "__main__":
    app()
