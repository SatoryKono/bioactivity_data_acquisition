"""Standalone runner for the OpenAlex pipeline."""

from __future__ import annotations

import typer

from bioetl.cli.command import create_pipeline_command
from bioetl.cli.commands.openalex import build_command_config

app = typer.Typer(help="Run the OpenAlex pipeline")

app.command()(create_pipeline_command(build_command_config()))


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    app()
