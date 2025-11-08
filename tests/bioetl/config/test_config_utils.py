"""Tests for configuration utility helpers."""

from __future__ import annotations

from pydantic import SecretStr
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
        ("1", True),
        ("0", False),
        ("On", True),
        ("No", False),
        (1, True),
        (0, False),
        ([], False),
        (["item"], True),
    ],
)
def test_coerce_bool(value: object, expected: bool) -> None:
    """Ensure boolean coercion behaves deterministically across inputs."""

    assert utils.coerce_bool(value) is expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (SecretStr("true"), True),
        (SecretStr(" yes "), True),
        (SecretStr("on"), True),
        (SecretStr("1"), True),
        (SecretStr("false"), False),
        (SecretStr(" no "), False),
        (SecretStr("off"), False),
        (SecretStr("0"), False),
    ],
)
def test_coerce_bool_secret_str(value: SecretStr, expected: bool) -> None:
    """SecretStr values should round-trip through coerce_bool."""

    assert utils.coerce_bool(value) is expected
