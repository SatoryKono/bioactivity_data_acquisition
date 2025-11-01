#!/usr/bin/env python3
"""CLI entrypoint for executing the test item pipeline."""

from bioetl.cli.app import create_pipeline_app

app = create_pipeline_app(
    "testitem",
    "Run test item pipeline to extract and transform compound data",
)


if __name__ == "__main__":
    app()

