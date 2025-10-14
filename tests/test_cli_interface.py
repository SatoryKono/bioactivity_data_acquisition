from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from library.cli.pipeline_app import create_pipeline_app


def _invoke_cli(pipeline: str, tmp_path: Path) -> None:
    runner = CliRunner()
    app = create_pipeline_app(pipeline)
    result = runner.invoke(
        app,
        [
            "--config",
            str(Path("configs/pipelines.toml")),
            "--limit",
            "5",
            "--output-dir",
            str(tmp_path / pipeline),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.output


def test_activity_cli_shared_interface(tmp_path: Path) -> None:
    _invoke_cli("activity", tmp_path)


def test_assay_cli_shared_interface(tmp_path: Path) -> None:
    _invoke_cli("assay", tmp_path)


def test_target_cli_shared_interface(tmp_path: Path) -> None:
    _invoke_cli("target", tmp_path)


def test_document_cli_shared_interface(tmp_path: Path) -> None:
    _invoke_cli("document", tmp_path)


def test_testitem_cli_shared_interface(tmp_path: Path) -> None:
    _invoke_cli("testitem", tmp_path)
