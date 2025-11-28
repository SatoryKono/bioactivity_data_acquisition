"""CLI smoke tests for the BioETL Typer application."""

from __future__ import annotations

import traceback
from pathlib import Path

from typing import cast

import typer
from typer.testing import CliRunner

from bioetl.cli.cli_app import app


def test_list_command_reports_registered_pipelines() -> None:
    """Ensure list command shows registered pipelines."""

    runner = CliRunner()
    result = runner.invoke(cast(typer.Typer, app), ["list"])
    assert result.exit_code == 0
    assert "activity_chembl" in result.output


def test_config_inspect_merges_overrides(tmp_path: Path) -> None:
    """Override config via --set and show merged configuration."""

    runner = CliRunner()
    config_path = tmp_path / "config.yaml"
    config_path.write_text("pipeline:\n  name: demo\n", encoding="utf-8")

    result = runner.invoke(
        cast(typer.Typer, app),
        [
            "config",
            "inspect",
            "--config",
            str(config_path),
            "--set",
            "metadata.owner=qa",
        ],
    )

    assert result.exit_code == 0
    assert "pipeline:" in result.output
    assert "owner: qa" in result.output


def test_activity_chembl_dry_run(tmp_path: Path) -> None:
    """Run activity_chembl in dry-run mode via CLI."""

    runner = CliRunner()
    output_dir = tmp_path / "out"
    config_path = tmp_path / "chembl.yaml"
    config_path.write_text(
        """
name: chembl
cache:
  namespace: demo
determinism:
  sort:
    by: ["activity_id"]
sources:
  chembl:
    batch_size: 1
    max_url_length: 100
""",
        encoding="utf-8",
    )

    result = runner.invoke(
        cast(typer.Typer, app),
        [
            "activity_chembl",
            "--config",
            str(config_path),
            "--output-dir",
            str(output_dir),
            "--dry-run",
        ],
    )

    if result.exit_code != 0:
        print(f"OUTPUT: {result.output}")
        print(f"EXCEPTION: {result.exception}")
        if result.exc_info:
            traceback.print_exception(*result.exc_info)

    assert result.exit_code == 0
    assert "Pipeline activity_chembl finished" in result.stdout
