"""Smoke tests for CLI examples from documentation.

These tests verify that all CLI examples from README and pipeline documentation
execute successfully with --dry-run flag.

Note: These tests use subprocess to run CLI commands in separate processes.
Since all tests use --dry-run flag, API calls are prevented by the pipeline
logic (dry_run mode returns empty DataFrames without making API calls).
Therefore, mocking is not needed for these tests, but they serve as smoke
tests to verify that CLI commands work correctly.
"""

from pathlib import Path

import pytest
from tests.tools.cli import run_cli_command

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.integration
def test_list_command():
    """Test CLI list command from README."""
    result = run_cli_command(["list"], cwd=ROOT, timeout=30)
    assert result.returncode == 0, f"list command failed: {result.stderr}"


@pytest.mark.integration
def test_activity_chembl_dry_run():
    """Test activity_chembl command with --dry-run from README."""
    config_path = ROOT / "configs" / "pipelines" / "activity" / "activity_chembl.yaml"
    output_dir = ROOT / "data" / "output" / "activity"

    result = run_cli_command(
        [
            "activity_chembl",
            "--config",
            str(config_path),
            "--output-dir",
            str(output_dir),
            "--dry-run",
        ],
        cwd=ROOT,
        timeout=60,
    )
    assert result.returncode == 0, f"activity_chembl --dry-run failed: {result.stderr}"


@pytest.mark.integration
def test_assay_chembl_dry_run():
    """Test assay_chembl command with --dry-run from docs/pipelines/assay-chembl/16-assay-chembl-cli.md."""
    config_path = ROOT / "configs" / "pipelines" / "assay" / "assay_chembl.yaml"
    output_dir = ROOT / "data" / "output" / "assay"

    result = run_cli_command(
        [
            "assay_chembl",
            "--config",
            str(config_path),
            "--output-dir",
            str(output_dir),
            "--dry-run",
        ],
        cwd=ROOT,
        timeout=60,
    )
    assert result.returncode == 0, f"assay_chembl --dry-run failed: {result.stderr}"


@pytest.mark.integration
def test_testitem_dry_run():
    """Test testitem command with --dry-run from docs/pipelines/testitem-chembl/16-testitem-chembl-cli.md."""
    config_path = ROOT / "configs" / "pipelines" / "testitem" / "testitem_chembl.yaml"
    output_dir = ROOT / "data" / "output" / "testitem"

    result = run_cli_command(
        [
            "testitem_chembl",
            "--config",
            str(config_path),
            "--output-dir",
            str(output_dir),
            "--dry-run",
        ],
        cwd=ROOT,
        timeout=60,
    )
    assert result.returncode == 0, f"testitem --dry-run failed: {result.stderr}"


@pytest.mark.integration
def test_testitem_chembl_alias_dry_run():
    """Test testitem_chembl alias command with --dry-run."""
    config_path = ROOT / "configs" / "pipelines" / "testitem" / "testitem_chembl.yaml"
    output_dir = ROOT / "data" / "output" / "testitem"

    result = run_cli_command(
        [
            "testitem_chembl",
            "--config",
            str(config_path),
            "--output-dir",
            str(output_dir),
            "--dry-run",
        ],
        cwd=ROOT,
        timeout=60,
    )
    assert result.returncode == 0, f"testitem_chembl --dry-run failed: {result.stderr}"
