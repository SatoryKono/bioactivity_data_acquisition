#!/usr/bin/env python3
"""CLI entrypoint for executing the activity pipeline."""

from bioetl.cli.app import create_pipeline_app

app = create_pipeline_app(
    "activity",
    "Run activity pipeline to extract and transform activity data",
)


if __name__ == "__main__":
    app()

