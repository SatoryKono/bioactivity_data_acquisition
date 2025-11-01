#!/usr/bin/env python3
"""Template for ChEMBL pipeline launch scripts.

This template is used to generate launch scripts for ChEMBL pipelines.
Each pipeline should import this and customize the pipeline_name and description.
"""

from __future__ import annotations

from bioetl.cli.app import create_pipeline_app

# Override these in the actual script files
_PIPELINE_NAME: str = "__PIPELINE_NAME__"
_DESCRIPTION: str = "__DESCRIPTION__"

app = create_pipeline_app(_PIPELINE_NAME, _DESCRIPTION)


if __name__ == "__main__":
    app()

