"""Filesystem-related pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest


__all__ = ["golden_dir", "tmp_logs_dir", "tmp_output_dir"]


@pytest.fixture  # type: ignore[misc]
def tmp_output_dir(tmp_path: Path) -> Path:
    """Temporary directory for pipeline output artifacts."""
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


@pytest.fixture  # type: ignore[misc]
def tmp_logs_dir(tmp_path: Path) -> Path:
    """Temporary directory for pipeline logs."""
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir


@pytest.fixture  # type: ignore[misc]
def golden_dir(tmp_path: Path) -> Path:
    """Directory for golden test snapshots."""
    golden = tmp_path / "golden"
    golden.mkdir(parents=True, exist_ok=True)
    return golden
