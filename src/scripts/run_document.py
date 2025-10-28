#!/usr/bin/env python3
"""CLI entrypoint for executing the document pipeline."""

from pathlib import Path

import typer

from bioetl.cli.command import PipelineCommandConfig, create_pipeline_command
from bioetl.pipelines.document import DocumentPipeline

app = typer.Typer(help="Run document pipeline to extract and transform document data")


app.command()(
    create_pipeline_command(
        PipelineCommandConfig(
            pipeline_name="document",
            pipeline_factory=lambda: DocumentPipeline,
            default_config=Path("configs/pipelines/document.yaml"),
            default_input=Path("data/input/documents.csv"),
            default_output_dir=Path("data/output/documents"),
            mode_choices=("chembl", "all"),
            default_mode="chembl",
        )
    )
)


if __name__ == "__main__":
    app()

