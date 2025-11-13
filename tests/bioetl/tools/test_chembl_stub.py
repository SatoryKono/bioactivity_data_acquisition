from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.tools import chembl_stub


def test_safe_int_handles_various_inputs() -> None:
    assert chembl_stub._safe_int(True, 5) == 1  # bool treated separately
    assert chembl_stub._safe_int(3.7, 0) == 3
    assert chembl_stub._safe_int(" 42 ", 0) == 42
    assert chembl_stub._safe_int("not-a-number", 7) == 7
    assert chembl_stub._safe_int(None, 9) == 9


def test_offline_resource_filter_and_only() -> None:
    resource = chembl_stub._OfflineResource(
        [
            {"value": 1, "other": "a"},
            {"value": 2, "other": "b"},
            {"value": 3, "other": "c"},
        ]
    )

    # respect limit/offset conversions
    query = resource.filter(limit="2", offset=1.0)
    rows = list(query)
    assert len(rows) == 2
    assert rows[0]["value"] == 2
    assert rows[1]["value"] == 3

    only_query = query.only("other")
    trimmed = list(only_query)
    assert trimmed == [{"other": "b"}, {"other": "c"}]


def test_get_offline_new_client_returns_deterministic_client() -> None:
    client = chembl_stub.get_offline_new_client()
    results = list(client.activity.filter(limit=5))
    assert results == [
        {
            "standard_type": "IC50",
            "standard_units": "nM",
            "standard_relation": "=",
            "bao_format": "BAO_0000015",
        }
    ]
    assert list(client.assay.filter())[0]["assay_type"] == "B"

