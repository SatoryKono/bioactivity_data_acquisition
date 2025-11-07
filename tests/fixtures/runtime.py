"""Runtime-related pytest fixtures."""

from __future__ import annotations

import pytest


__all__ = ["run_id"]


@pytest.fixture  # type: ignore[misc]
def run_id() -> str:
    """Sample run_id for testing."""
    return "test-run-12345"
