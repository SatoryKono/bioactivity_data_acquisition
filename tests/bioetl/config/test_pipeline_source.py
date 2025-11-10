"""Tests for pipeline source configuration helpers."""

from __future__ import annotations

from pydantic import SecretStr
import pytest

from bioetl.config.pipeline_source import ChemblPipelineSourceConfig


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, ChemblPipelineSourceConfig.defaults.handshake_enabled),
        ("YES", True),
        ("no", False),
        (SecretStr("on"), True),
        (SecretStr("0"), False),
    ],
)
def test_handshake_enabled_uses_shared_coercion(raw: object, expected: bool) -> None:
    """Coercion for handshake_enabled should delegate to the shared helper."""

    assert ChemblPipelineSourceConfig._resolve_handshake_enabled(raw) is expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, ChemblPipelineSourceConfig.defaults.handshake_timeout_sec),
        ("5", 5.0),
        (7, 7.0),
    ],
)
def test_handshake_timeout_uses_positive_float(raw: object, expected: float) -> None:
    """handshake_timeout_sec should coerce to a positive float."""

    assert ChemblPipelineSourceConfig._resolve_handshake_timeout(raw) == expected


@pytest.mark.unit
@pytest.mark.parametrize("raw", [0, "0", -1, "-2", "foo"])
def test_handshake_timeout_rejects_invalid(raw: object) -> None:
    """Invalid handshake_timeout values should raise ValueError."""

    with pytest.raises(ValueError):
        ChemblPipelineSourceConfig._resolve_handshake_timeout(raw)
