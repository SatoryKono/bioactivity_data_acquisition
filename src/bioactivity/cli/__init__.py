"""Command line interface for running the bioactivity ETL pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

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

def _parse_override_args(values: list[str]) -> dict[str, str]:
    assignments: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise typer.BadParameter("Overrides must be in KEY=VALUE format")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise typer.BadParameter("Override key must not be empty")
        assignments[key] = value
    return assignments


def _override_option_callback(
    ctx: typer.Context, param: Any, value: list[str] | None
) -> dict[str, str]:
    del ctx, param
    return _parse_override_args(value or [])


OVERRIDE_OPTION = typer.Option(
    [],
    "--set",
    "-s",
    callback=_override_option_callback,
    help=(
        "Override configuration values using dotted paths (KEY=VALUE), e.g. "
        "runtime.log_level=DEBUG."
    ),
)

app = typer.Typer(help="Bioactivity ETL pipeline")


@app.command()
def pipeline(
    config: Path = CONFIG_OPTION,
    overrides=OVERRIDE_OPTION,
) -> None:
    """Execute the ETL pipeline using a configuration file."""

    override_mapping = cast(dict[str, str], overrides)
    config_model = Config.load(config, overrides=override_mapping)
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
