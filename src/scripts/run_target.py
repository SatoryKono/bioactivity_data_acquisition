#!/usr/bin/env python3
"""CLI entrypoint for executing the target pipeline."""

from pathlib import Path

import typer

from bioetl.cli.command import PipelineCommandConfig, create_pipeline_command
from bioetl.pipelines.target import TargetPipeline

app = typer.Typer(help="Run target pipeline to extract and transform target data")


app.command()(
    create_pipeline_command(
        PipelineCommandConfig(
            pipeline_name="target",
            pipeline_factory=lambda: TargetPipeline,
            default_config=Path("configs/pipelines/target.yaml"),
            default_input=Path("data/input/targets.csv"),
            default_output_dir=Path("data/output/targets"),
        )
    )
)


if __name__ == "__main__":
    app()

