#!/usr/bin/env python3
"""CLI entrypoint for executing the test item pipeline."""

import typer

from scripts import register_pipeline_command

app = typer.Typer(help="Run test item pipeline to extract and transform compound data")


register_pipeline_command(app, "testitem")


if __name__ == "__main__":
    app()

