"""Command line interface for running the bioactivity ETL pipeline."""

from __future__ import annotations

from pathlib import Path

import typer

from bioactivity.config import Config
from bioactivity.etl.run import run_pipeline
from bioactivity.utils.logging import configure_logging

CONFIG_OPTION = typer.Option(
    ...,
    "--config",
    "-c",
    exists=True,
    file_okay=True,
    dir_okay=False,
    readable=True,
    resolve_path=True,
)

app = typer.Typer(help="Bioactivity ETL pipeline")


@app.command()
def pipeline(config: Path = CONFIG_OPTION) -> None:
    """Execute the ETL pipeline using a configuration file."""

    config_model = Config.load(config)
    logger = configure_logging(config_model.logging.level)
    logger = logger.bind(command="pipeline")
    output = run_pipeline(config_model, logger)
    typer.echo(f"Pipeline completed. Output written to {output}")


@app.command()
def version() -> None:
    """Print the package version."""

    typer.echo("bioactivity-data-acquisition 0.1.0")


def main() -> None:
    """Entrypoint for ``python -m bioactivity.cli``."""

    app()


if __name__ == "__main__":  # pragma: no cover - convenience entrypoint
    main()


__all__ = ["app", "main"]
