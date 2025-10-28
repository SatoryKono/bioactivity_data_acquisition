"""CLI interface using typer."""

import typer

from bioetl.core.logger import UnifiedLogger

app = typer.Typer(help="BioETL - Unified ETL framework for bioactivity data")


@app.command()
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

