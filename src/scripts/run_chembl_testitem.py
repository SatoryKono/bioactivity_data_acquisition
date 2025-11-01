#!/usr/bin/env python3
"""CLI entrypoint for executing the chembl_testitem pipeline."""

from __future__ import annotations

from bioetl.cli.app import create_pipeline_app

app = create_pipeline_app(
    "chembl_testitem",
    "Run chembl_testitem pipeline to extract and enrich test item data",
)


if __name__ == "__main__":
    app()
