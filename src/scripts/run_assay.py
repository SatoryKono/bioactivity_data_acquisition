#!/usr/bin/env python3
"""CLI entrypoint for executing the assay pipeline."""

from bioetl.cli.app import create_pipeline_app

app = create_pipeline_app(
    "assay",
    "Run assay pipeline to extract and transform assay data",
)


if __name__ == "__main__":
    app()

