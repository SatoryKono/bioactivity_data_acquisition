"""Tests for reusable HTTP client helpers."""

from __future__ import annotations

from bioetl.clients.base import (
    build_filters_payload,
    merge_select_fields,
    normalize_select_fields,
)


def test_normalize_select_fields_from_sequence() -> None:
    fields = normalize_select_fields(["assay_chembl_id", "document_chembl_id", "assay_chembl_id"])
    assert fields == ("assay_chembl_id", "document_chembl_id")


def test_normalize_select_fields_with_default() -> None:
    fields = normalize_select_fields(None, default=("molecule_chembl_id",))
    assert fields == ("molecule_chembl_id",)


def test_merge_select_fields_preserves_required() -> None:
    merged = merge_select_fields(("document_chembl_id",), ("document_chembl_id", "doi"))
    assert merged == ("document_chembl_id", "doi")


def test_build_filters_payload_compacts_none_values() -> None:
    payload, compact = build_filters_payload(
        limit=100,
        page_size=25,
        select_fields=("assay_chembl_id",),
        extra_filters={"batch_size": 25, "requested_ids": ["A", "B"]},
    )
    assert payload["mode"] == "all"
    assert compact["batch_size"] == 25
    assert compact["requested_ids"] == ["A", "B"]

