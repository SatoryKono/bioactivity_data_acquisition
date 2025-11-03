#!/usr/bin/env python3
"""CLI entrypoint for executing the chembl_document pipeline."""

from __future__ import annotations

from bioetl.cli.app import create_pipeline_app

app = create_pipeline_app(
    "chembl_document",
    "Run chembl_document pipeline to extract and enrich document data",
)


if __name__ == "__main__":
    app()
