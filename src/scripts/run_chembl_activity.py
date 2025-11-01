#!/usr/bin/env python3
"""CLI entrypoint for executing the chembl_activity pipeline."""

from __future__ import annotations

from bioetl.cli.app import create_pipeline_app

app = create_pipeline_app(
    "chembl_activity",
    "Run chembl_activity pipeline to extract and transform activity data",
)

if __name__ == "__main__":
    app()
