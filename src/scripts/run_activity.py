#!/usr/bin/env python3
"""CLI entrypoint for executing the activity pipeline."""

import typer

from scripts import register_pipeline_command

app = typer.Typer(help="Run activity pipeline to extract and transform activity data")


register_pipeline_command(app, "activity")


if __name__ == "__main__":
    app()

