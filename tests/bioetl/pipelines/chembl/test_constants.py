"""Smoke tests for provider-specific constants."""

from __future__ import annotations

from bioetl.pipelines.chembl._constants import (
    API_ACTIVITY_FIELDS,
    API_DOCUMENT_FIELDS,
    ASSAY_MUST_HAVE_FIELDS,
    DOCUMENT_MUST_HAVE_FIELDS,
    TESTITEM_MUST_HAVE_FIELDS,
)


def test_activity_fields_are_unique_and_ordered() -> None:
    assert API_ACTIVITY_FIELDS[0] == "activity_id"
    assert len(API_ACTIVITY_FIELDS) == len(set(API_ACTIVITY_FIELDS))


def test_document_fields_cover_identifiers() -> None:
    assert API_DOCUMENT_FIELDS[0] == "document_chembl_id"
    assert "doi" in API_DOCUMENT_FIELDS
    assert len(API_DOCUMENT_FIELDS) == len(set(API_DOCUMENT_FIELDS))


def test_assay_must_have_fields() -> None:
    assert ASSAY_MUST_HAVE_FIELDS == ("assay_chembl_id",)


def test_document_must_have_subset_of_fields() -> None:
    for field in DOCUMENT_MUST_HAVE_FIELDS:
        assert field in API_DOCUMENT_FIELDS


def test_testitem_must_have_fields_include_nested_roots() -> None:
    nested_roots = {"molecule_properties", "molecule_structures", "molecule_hierarchy"}
    assert nested_roots.issubset(set(TESTITEM_MUST_HAVE_FIELDS))

