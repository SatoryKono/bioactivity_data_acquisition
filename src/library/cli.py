"""Typer CLI entrypoint."""
from __future__ import annotations

from pathlib import Path

import typer

from .pipeline import run_pipeline

COMMAND_ARGUMENT = typer.Argument("pipeline", hidden=True)
CONFIG_OPTION = typer.Option(
    ..., exists=True, help="Path to the pipeline YAML config."
)
ENV_FILE_OPTION = typer.Option(
    None, exists=False, help="Optional .env file for secrets."
)

app = typer.Typer(help="Bioactivity ETL pipeline", no_args_is_help=True)


@app.command("pipeline")
def pipeline_command(
    command: str = COMMAND_ARGUMENT,
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_FILE_OPTION,
) -> None:
    """Run the pipeline using ``config`` and optional ``env_file``."""
    if command != "pipeline":
        raise typer.BadParameter(f"Unknown command: {command}")
    run_pipeline(config, env_file=env_file)
    typer.echo("Pipeline completed successfully")


if __name__ == "__main__":
    app()


def main() -> None:
    """Entry point for console script."""
    app()
