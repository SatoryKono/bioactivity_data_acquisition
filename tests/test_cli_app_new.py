from __future__ import annotations

from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from bioetl.cli.cli_app import app
from bioetl.cli.cli_registry import PIPELINE_REGISTRY
from bioetl.cli.cli_command import create_pipeline_command
from bioetl.config import PipelineConfig


class DummyPipeline:
    def __init__(self, config: PipelineConfig, run_id: str | None) -> None:
        self.config = config
        self.run_id = run_id

    def run(self, output_dir: Path) -> Any:
        output_dir.mkdir(parents=True, exist_ok=True)
        return type("Result", (), {"success": True})()


def test_list_command_handles_empty_registry():
    runner = CliRunner()
    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert "No pipelines registered" in result.output


def test_dynamic_pipeline_command(tmp_path: Path):
    runner = CliRunner()
    try:
        PIPELINE_REGISTRY.clear()
        PIPELINE_REGISTRY["demo"] = lambda config, run_id=None: DummyPipeline(config, run_id)
        app.command("demo")(create_pipeline_command("demo", PIPELINE_REGISTRY["demo"]))

        config_path = tmp_path / "config.yaml"
        config_path.write_text("name: demo", encoding="utf-8")

        result = runner.invoke(app, ["demo", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "Pipeline demo finished" in result.output
    finally:
        PIPELINE_REGISTRY.clear()
