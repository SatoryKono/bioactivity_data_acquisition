#!/usr/bin/env python3
"""CLI entrypoint for executing the chembl_target pipeline."""

from __future__ import annotations

from bioetl.cli.app import create_pipeline_app

app = create_pipeline_app(
    "chembl_target",
    "Run chembl_target pipeline to extract and enrich target data",
)


if __name__ == "__main__":
    app()
