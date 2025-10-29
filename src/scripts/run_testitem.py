#!/usr/bin/env python3
"""CLI entrypoint for executing the test item pipeline."""

from pathlib import Path

import typer

from bioetl.cli.command import PipelineCommandConfig, create_pipeline_command
from bioetl.pipelines.testitem import TestItemPipeline

app = typer.Typer(help="Run test item pipeline to extract and transform compound data")


@app.command()
def run_testitem():
    """Run test item pipeline to extract and transform compound data."""
    return create_pipeline_command(
        PipelineCommandConfig(
            pipeline_name="testitem",
            pipeline_factory=lambda: TestItemPipeline,
            default_config=Path("configs/pipelines/testitem.yaml"),
            default_input=Path("data/input/testitems.csv"),
            default_output_dir=Path("data/output/testitems"),
        )
    )()


if __name__ == "__main__":
    app()

