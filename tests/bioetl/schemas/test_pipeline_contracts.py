"""Unit tests for pipeline schema helper utilities."""

from __future__ import annotations

import pytest

import bioetl.schemas.pipeline_contracts as pipeline_contracts

from bioetl.schemas.pipeline_contracts import (
    PipelineSchemaContract,
    get_business_key_fields,
    get_column_order,
    get_in_schema,
    get_out_schema,
    get_pipeline_contract,
    resolve_contract_sequence,
)


def test_get_pipeline_contract_returns_declared_mapping() -> None:
    contract = get_pipeline_contract("activity_chembl")
    assert isinstance(contract, PipelineSchemaContract)
    assert contract.pipeline_code == "activity_chembl"
    assert contract.schema_out.endswith("ActivitySchema")


def test_get_out_schema_uses_registry_cache() -> None:
    descriptor = get_out_schema("activity_chembl")
    assert descriptor.identifier.endswith("ActivitySchema")
    assert descriptor.column_order


def test_get_business_key_fields_matches_schema_metadata() -> None:
    business_keys = get_business_key_fields("activity_chembl")
    descriptor = get_out_schema("activity_chembl")
    assert business_keys == descriptor.business_key_fields


def test_get_in_schema_returns_none_when_not_declared() -> None:
    assert get_in_schema("activity_chembl") is None


def test_get_column_order_matches_schema_metadata() -> None:
    column_order = get_column_order("activity_chembl")
    descriptor = get_out_schema("activity_chembl")
    assert column_order == descriptor.column_order


def test_unknown_pipeline_raises_key_error() -> None:
    with pytest.raises(KeyError):
        get_pipeline_contract("unknown_pipeline")


class _StubDescriptor:
    def __init__(
        self,
        *,
        metadata: dict[str, object] | None = None,
        column_order: tuple[str, ...] | None = None,
        business_key_fields: tuple[str, ...] | None = None,
    ) -> None:
        self.metadata = metadata or {}
        self.column_order = column_order or ()
        self.business_key_fields = business_key_fields or ()


def test_resolve_contract_sequence_prefers_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    contract = PipelineSchemaContract(
        pipeline_code="stub",
        schema_out="stub.schema",
    )
    descriptor = _StubDescriptor(
        metadata={"business_key_fields": ["id", "source"]},
        business_key_fields=("fallback",),
    )

    monkeypatch.setattr(pipeline_contracts, "get_schema", lambda identifier: descriptor)

    resolved = resolve_contract_sequence(contract, "business_key_fields", "business_key_fields")

    assert resolved == ("id", "source")


def test_resolve_contract_sequence_uses_descriptor_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    contract = PipelineSchemaContract(
        pipeline_code="stub",
        schema_out="stub.schema",
    )
    descriptor = _StubDescriptor(
        metadata={},
        column_order=("chembl_id", "pref_name"),
    )

    monkeypatch.setattr(pipeline_contracts, "get_schema", lambda identifier: descriptor)

    resolved = resolve_contract_sequence(contract, "column_order", "column_order")

    assert resolved == ("chembl_id", "pref_name")

