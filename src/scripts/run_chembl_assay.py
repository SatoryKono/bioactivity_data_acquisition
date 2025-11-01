#!/usr/bin/env python3
"""CLI entrypoint for executing the chembl_assay pipeline."""

from __future__ import annotations

from bioetl.cli.app import create_pipeline_app

app = create_pipeline_app(
    "chembl_assay",
    "Run chembl_assay pipeline to extract and transform assay data",
)


if __name__ == "__main__":
    app()
