#!/usr/bin/env python3
"""CLI entrypoint for executing the assay pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import typer

from bioetl.cli.app import create_pipeline_app  # type: ignore[assignment]

app: typer.Typer = create_pipeline_app(  # type: ignore[call-overload]
    "assay",
    "Run assay pipeline to extract and transform assay data",
)


if __name__ == "__main__":
    app()

