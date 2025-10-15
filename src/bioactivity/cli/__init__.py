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

OVERRIDE_OPTION = typer.Option(
    None,
    "--set",
    "-s",
    help="Override configuration values using dotted paths, e.g. runtime.log_level=DEBUG.",
)

app = typer.Typer(help="Bioactivity ETL pipeline")


@app.command()
def pipeline(
    config: Path = CONFIG_OPTION,
    set: list[str] | None = OVERRIDE_OPTION,
) -> None:
    """Execute the ETL pipeline using a configuration file."""

    try:
        cli_overrides = Config.parse_cli_overrides(set or [])
    except ValueError as exc:  # pragma: no cover - Typer handles message formatting
        raise typer.BadParameter(str(exc)) from exc

    config_model = Config.load(config, cli_overrides=cli_overrides)
    logger = configure_logging(config_model.runtime.log_level)
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
