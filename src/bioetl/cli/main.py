"""CLI interface using typer."""

import uuid
from pathlib import Path

import typer
from typer import Option

from bioetl.config import PipelineConfig, load_config, parse_cli_overrides
from bioetl.core.logger import UnifiedLogger

app = typer.Typer(help="BioETL - Unified ETL framework for bioactivity data")


@app.command()
def pipeline_run(
    name: str = typer.Argument(..., help="Pipeline name (assay, activity, etc.)"),
    config: Path = Option(None, "--config", "-c", help="Path to configuration file"),
    extended: bool = Option(False, "--extended", "-e", help="Include extended artifacts"),
    verbose: bool = Option(False, "--verbose", "-v", help="Verbose logging"),
):
    """Run a pipeline."""
    UnifiedLogger.setup(mode="development" if verbose else "production")

    # Generate run_id
    run_id = str(uuid.uuid4())[:8]
    UnifiedLogger.set_context(run_id=run_id, pipeline=name)

    logger = UnifiedLogger.get(__name__)
    logger.info("pipeline_run_started", name=name, run_id=run_id)

    if not config:
        config = Path("configs/base.yaml")

    # Load configuration
    config_obj = load_config(config)
    logger.info("config_loaded", pipeline=config_obj.pipeline.name)

    # For now, just log that we would run the pipeline
    logger.info("pipeline_run_completed", name=name)
    typer.echo(f"Pipeline '{name}' would be run (placeholder)")


@app.command()
def pipeline_list():
    """List available pipelines."""
    UnifiedLogger.setup(mode="production")
    typer.echo("Available pipelines:")
    typer.echo("  - assay (ChEMBL assay data)")
    typer.echo("  - activity (ChEMBL activity data)")
    typer.echo("  - testitem (ChEMBL molecules + PubChem)")
    typer.echo("  - target (ChEMBL + UniProt + IUPHAR)")
    typer.echo("  - document (ChEMBL + external sources)")


@app.command()
def pipeline_validate(
    name: str = typer.Argument(..., help="Pipeline name"),
    config: Path = Option(None, "--config", "-c", help="Path to configuration file"),
):
    """Validate pipeline configuration."""
    UnifiedLogger.setup(mode="development")
    logger = UnifiedLogger.get(__name__)

    if not config:
        config = Path("configs/base.yaml")

    config_obj = load_config(config)
    logger.info("config_validated", pipeline=config_obj.pipeline.name)
    typer.echo(f"Configuration for '{name}' is valid")


if __name__ == "__main__":
    app()

