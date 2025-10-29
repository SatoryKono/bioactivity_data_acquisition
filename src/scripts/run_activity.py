#!/usr/bin/env python3
"""CLI entrypoint for executing the activity pipeline."""

from pathlib import Path

import typer

from bioetl.cli.command import PipelineCommandConfig, create_pipeline_command
from bioetl.pipelines.activity import ActivityPipeline

app = typer.Typer(help="Run activity pipeline to extract and transform activity data")


app.command()(
    create_pipeline_command(
        PipelineCommandConfig(
            pipeline_name="activity",
            pipeline_factory=lambda: ActivityPipeline,
            default_config=Path("configs/pipelines/activity.yaml"),
            default_input=Path("data/input/activity.csv"),
            default_output_dir=Path("data/output/activity"),
        )
    )
)


if __name__ == "__main__":
    app()

