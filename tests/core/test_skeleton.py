"""Smoke tests for basic CLI app and pipeline registry wiring."""

from __future__ import annotations

from typer.testing import CliRunner

from bioetl.cli.cli_app import app
from bioetl.cli.cli_registry import PIPELINE_REGISTRY
from bioetl.core.pipeline.unified import (  # type: ignore[import-untyped]
    UnifiedPipelineBase as PipelineBase,
)


def test_pipeline_base_is_importable() -> None:
    """Ensure PipelineBase exposes the run() method."""
    assert hasattr(PipelineBase, "run"), "PipelineBase must expose run()"


def test_cli_app_list_command_runs() -> None:
    """Ensure the CLI list command runs successfully."""
    runner = CliRunner()
    result = runner.invoke(app, ["list"])  # type: ignore[arg-type]
    assert result.exit_code == 0


def test_pipeline_registry_is_dict() -> None:
    """Ensure the registry is a dict and contains activity_chembl."""
    assert isinstance(PIPELINE_REGISTRY, dict)
    assert "activity_chembl" in PIPELINE_REGISTRY
