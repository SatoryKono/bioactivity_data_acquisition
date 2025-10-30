#!/usr/bin/env python3
"""CLI entrypoint for executing the document pipeline."""

import typer

from scripts import register_pipeline_command

app = typer.Typer(help="Run document pipeline to extract and transform document data")


register_pipeline_command(app, "document")


if __name__ == "__main__":
    app()

