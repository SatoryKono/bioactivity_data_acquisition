#!/usr/bin/env python3
"""CLI entrypoint for executing the assay pipeline."""

import typer

from scripts import register_pipeline_command

app = typer.Typer(help="Run assay pipeline to extract and transform assay data")


register_pipeline_command(app, "assay")


if __name__ == "__main__":
    app()

