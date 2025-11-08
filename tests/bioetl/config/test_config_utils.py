"""Tests for configuration utility helpers."""

from __future__ import annotations

import pytest

from bioetl.config import utils


@pytest.mark.unit
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, True),
        (False, False),
        ("true", True),
        (" YES ", True),
        ("False", False),
        ("off", False),
        (1, True),
        (0, False),
        ([], False),
        (["item"], True),
    ],
)
def test_coerce_bool(value: object, expected: bool) -> None:
    """Ensure boolean coercion behaves deterministically across inputs."""

    assert utils.coerce_bool(value) is expected
