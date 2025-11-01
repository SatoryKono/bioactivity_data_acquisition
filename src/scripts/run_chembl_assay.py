#!/usr/bin/env python3
"""CLI entrypoint for executing the chembl_assay pipeline."""

from __future__ import annotations

<<<<<<<< HEAD:src/scripts/run_chembl_assay.py
from bioetl.cli.app import create_pipeline_app

app = create_pipeline_app(
    "chembl_assay",
    "Run chembl_assay pipeline to extract and transform assay data",
)
========
from scripts.run_chembl_assay import app  # re-export for backwards compatibility
>>>>>>>> origin/codex/-chembl_assay-xcs5km:src/scripts/run_assay.py


if __name__ == "__main__":
    app()
