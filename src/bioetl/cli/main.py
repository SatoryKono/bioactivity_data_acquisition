"""CLI interface using typer."""

from __future__ import annotations

import typer

from bioetl.core.logger import UnifiedLogger
from scripts import PIPELINE_COMMAND_REGISTRY, register_pipeline_command

app = typer.Typer(help="BioETL - Unified ETL framework for bioactivity data")

for key in sorted(PIPELINE_COMMAND_REGISTRY):
    register_pipeline_command(app, key)


@app.command(name="list")
def list():
    """List available pipelines."""
    UnifiedLogger.setup(mode="production")
    typer.echo("Available pipelines:")
    typer.echo("  - assay (ChEMBL assay data)")
    typer.echo("  - activity (ChEMBL activity data)")
    typer.echo("  - testitem (ChEMBL molecules + PubChem)")
    typer.echo("  - target (ChEMBL + UniProt + IUPHAR)")
    typer.echo("  - document (ChEMBL + external sources)")


if __name__ == "__main__":
    app()

