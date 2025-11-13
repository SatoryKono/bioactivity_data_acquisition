from __future__ import annotations

import pytest
from typer.testing import CliRunner


@pytest.fixture()
def runner() -> CliRunner:
    """Return a CliRunner instance for Typer tests."""
    return CliRunner()


