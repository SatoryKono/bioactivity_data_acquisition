#!/usr/bin/env python3
"""CLI entrypoint for executing the assay pipeline."""

from pathlib import Path

import typer

from bioetl.cli.command import PipelineCommandConfig, create_pipeline_command
from bioetl.pipelines.assay import AssayPipeline

app = typer.Typer(help="Run assay pipeline to extract and transform assay data")


@app.command()
def run():
    create_pipeline_command(
        PipelineCommandConfig(
            pipeline_name="assay",
            pipeline_factory=lambda: AssayPipeline,
            default_config=Path("configs/pipelines/assay.yaml"),
            default_input=Path("data/input/assay.csv"),
            default_output_dir=Path("data/output/assay"),
        )
    )()


if __name__ == "__main__":
    app()

