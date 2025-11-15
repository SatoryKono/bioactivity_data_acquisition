"""Tests for ChEMBL enrichment helper functions."""

from __future__ import annotations

from typing import Any, Mapping

from unittest.mock import Mock

import pytest

from bioetl.chembl.common.descriptor import ChemblPipelineBase
from bioetl.chembl.common.enrich import (
    _extract_enrich_config,
    enrich_flag,
)


@pytest.mark.parametrize(
    ("config", "path", "expected"),
    [
        (None, ("activity", "enrich", "assay", "enabled"), False),
        ({"activity": {"enrich": None}}, ("activity", "enrich", "assay", "enabled"), False),
        ({"activity": {"enrich": {"assay": {"enabled": True}}}}, ("activity", "enrich", "assay", "enabled"), True),
    ],
)
def test_enrich_flag_handles_optional_config(
    config: Mapping[str, Any] | None,
    path: tuple[str, ...],
    expected: bool,
) -> None:
    """Ensure ``enrich_flag`` never raises when config entries are missing."""

    assert enrich_flag(config, path) is expected


def test_extract_enrich_config_handles_none_root() -> None:
    """``_extract_enrich_config`` should return an empty mapping for None config."""

    log = Mock()
    result = _extract_enrich_config(None, ("activity",), log=log)
    assert result == {}
    log.warning.assert_not_called()


def test_extract_enrich_config_handles_none_branch() -> None:
    """``_extract_enrich_config`` should swallow attribute errors from None branches."""

    log = Mock()
    config = {"activity": {"enrich": None}}
    result = _extract_enrich_config(config, ("activity", "enrich", "assay"), log=log)
    assert result == {}
    log.warning.assert_called()


@pytest.mark.parametrize(
    "payload",
    [None, {}],
)
def test_extract_chembl_release_handles_none_payload(payload: Mapping[str, Any] | None) -> None:
    """``_extract_chembl_release`` should tolerate ``None`` payloads."""

    assert ChemblPipelineBase._extract_chembl_release(payload) is None
