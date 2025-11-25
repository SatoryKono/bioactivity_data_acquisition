from __future__ import annotations

from typer.testing import CliRunner

from bioetl.cli.cli_app import app
from bioetl.cli.cli_registry import PIPELINE_REGISTRY
from bioetl.pipelines.base import PipelineBase


def test_pipeline_base_is_importable() -> None:
    assert hasattr(PipelineBase, "run"), "PipelineBase must expose run()"


def test_cli_app_list_command_runs() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0


def test_pipeline_registry_is_dict() -> None:
    assert isinstance(PIPELINE_REGISTRY, dict)
    assert "activity_chembl" in PIPELINE_REGISTRY
