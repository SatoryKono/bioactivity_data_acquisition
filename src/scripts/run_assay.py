#!/usr/bin/env python3
"""CLI entrypoint for executing the assay pipeline."""

from scripts import create_pipeline_app  # noqa: E402

app = create_pipeline_app(
    "assay",
    "Run assay pipeline to extract and transform assay data",
)


if __name__ == "__main__":
    app()

