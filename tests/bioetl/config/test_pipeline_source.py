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
