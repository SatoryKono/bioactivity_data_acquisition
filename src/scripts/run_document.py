#!/usr/bin/env python3
"""CLI entrypoint for executing the document pipeline."""

from bioetl.cli.app import create_pipeline_app

app = create_pipeline_app(
    "document",
    "Run document pipeline to extract and transform document data",
)


if __name__ == "__main__":
    app()

