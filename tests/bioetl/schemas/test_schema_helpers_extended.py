from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Any

import pandas as pd
import pytest
from pandera import Column, DataFrameSchema

from bioetl.schemas import SchemaDescriptor, SchemaRegistry, _split_identifier
from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.activity import (
    _is_valid_activity_properties,
    _is_valid_activity_property_item,
)
from bioetl.schemas.schema_vocabulary_helper import (
    VOCAB_STORE_ENV_VAR,
    refresh_vocab_store_cache,
    required_vocab_ids,
)


def test_activity_property_item_validation_branches() -> None:
    valid_item: dict[str, object] = {
        "type": "curation",
        "relation": "=",
        "units": "nM",
        "value": 1.0,
        "text_value": "note",
        "result_flag": True,
    }
    assert _is_valid_activity_property_item(valid_item) is True

    replacements: list[tuple[str, object]] = [
        ("type", 123),
        ("relation", 0),
        ("units", 1),
        ("value", {"nested": True}),
        ("text_value", 42),
    ]
    for key, replacement in replacements:
        bad: dict[str, object] = dict(valid_item)
        bad[key] = replacement
        assert _is_valid_activity_property_item(bad) is False

    bad_flag = dict(valid_item, result_flag=2)
    assert _is_valid_activity_property_item(bad_flag) is False

    bad_keys = dict(valid_item)
    bad_keys.pop("type")
    assert _is_valid_activity_property_item(bad_keys) is False


def test_activity_property_series_validator() -> None:
    valid_payload = [
        {
            "type": "curation",
            "relation": "=",
            "units": "nM",
            "value": 1,
            "text_value": "ok",
            "result_flag": True,
        }
    ]
    assert _is_valid_activity_properties(None) is True
    assert _is_valid_activity_properties(pd.NA) is True
    assert _is_valid_activity_properties(float("nan")) is True
    assert _is_valid_activity_properties("not-json") is False
    assert _is_valid_activity_properties('"not list"') is False

    invalid_item_payload = pd.Series([{"type": 1}]).to_json()
    assert _is_valid_activity_properties(invalid_item_payload) is False

    valid_payload_json = pd.Series(valid_payload).to_json()
    assert _is_valid_activity_properties(valid_payload_json) is True


def test_create_schema_order_validation() -> None:
    schema_columns = {
        "id": Column(int),
        "value": Column(str),
    }
    with pytest.raises(ValueError, match="missing columns"):
        create_schema(columns=schema_columns, version="1.0", name="TestSchema", column_order=("id", "missing"))

    with pytest.raises(ValueError, match="duplicates"):
        create_schema(columns=schema_columns, version="1.0", name="TestSchema", column_order=("id", "id"))


def _build_simple_schema() -> DataFrameSchema:
    return DataFrameSchema({"id": Column(int), "value": Column(str)})


def test_schema_registry_registration_errors(tmp_path: Any) -> None:
    registry = SchemaRegistry()
    schema = _build_simple_schema()
    descriptor = SchemaDescriptor.from_components(
        identifier="pkg.schema.Test",
        schema=schema,
        version="1.0.0",
    )
    entry = registry.register(descriptor)
    assert entry.identifier == "pkg.schema.Test"

    with pytest.raises(ValueError):
        registry.register(descriptor)

    mismatched_schema = DataFrameSchema({"different": Column(int)})
    with pytest.raises(ValueError, match="column order references missing columns"):
        SchemaDescriptor.from_components(
            identifier="pkg.schema.Other",
            schema=mismatched_schema,
            version="1.0.0",
            column_order=("missing",),
        )


def test_schema_registry_metadata_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    schema = _build_simple_schema()
    schema = schema.set_metadata({"version": "2.0.0", "column_order": ("id", "value")})  # type: ignore[attr-defined]
    with pytest.raises(ValueError, match="metadata version"):
        SchemaDescriptor.from_components(
            identifier="pkg.meta.Schema",
            schema=schema,
            version="1.0.0",
        )

    bad_metadata = schema.set_metadata({"version": "2.0.0", "column_order": ("value", "id")})  # type: ignore[attr-defined]
    with pytest.raises(ValueError, match="metadata column_order"):
        SchemaDescriptor.from_components(
            identifier="pkg.meta.Schema2",
            schema=bad_metadata,
            version="2.0.0",
        )


def test_schema_registry_dynamic_import(monkeypatch: pytest.MonkeyPatch) -> None:
    module = ModuleType("tests.schemas.dynamic_module")
    schema = _build_simple_schema().set_metadata({"version": "1.2.3", "column_order": ("id", "value")})  # type: ignore[attr-defined]
    setattr(module, "DynamicSchema", schema)
    setattr(module, "SCHEMA_VERSION", "1.2.3")
    setattr(module, "COLUMN_ORDER", ("id", "value"))
    setattr(module, "BUSINESS_KEY_FIELDS", ("id",))
    setattr(module, "REQUIRED_FIELDS", ("value",))
    monkeypatch.setitem(sys.modules, module.__name__, module)

    registry = SchemaRegistry()
    entry = registry.get(f"{module.__name__}.DynamicSchema")
    assert entry.version == "1.2.3"
    assert entry.column_order == ("id", "value")
    assert entry.business_key_fields == ("id",)
    assert entry.required_fields == ("value",)


def test_schema_descriptor_vocabulary_bindings() -> None:
    column = Column(str, metadata={"vocabulary": {"id": "test_vocab", "allowed_statuses": ("active",)}})
    schema = DataFrameSchema({"value": column})
    descriptor = SchemaDescriptor.from_components(
        identifier="pkg.vocab.Schema",
        schema=schema,
        version="1.0.0",
    )
    assert descriptor.vocabulary_requirements == ("test_vocab",)
    assert len(descriptor.vocabulary_bindings) == 1
    binding = descriptor.vocabulary_bindings[0]
    assert binding.column == "value"
    assert binding.vocabulary_id == "test_vocab"
    assert binding.allowed_statuses == ("active",)
    assert binding.required is True


def test_split_identifier_errors() -> None:
    with pytest.raises(ValueError):
        _split_identifier("invalid")
    with pytest.raises(ValueError):
        _split_identifier("pkg:")


def test_required_vocab_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    helper = importlib.import_module("bioetl.schemas.schema_vocabulary_helper")
    store = {"valid": {"ids": ["one", "two"], "status": ["active", "inactive"]}}

    def fake_load() -> dict[str, Any]:
        return store

    monkeypatch.setenv(VOCAB_STORE_ENV_VAR, "/tmp/dictionaries")
    monkeypatch.setattr(helper, "load_vocab_store", lambda path: fake_load())
    refresh_vocab_store_cache()
    assert required_vocab_ids("valid") == {"one", "two"}

    monkeypatch.setattr(helper, "_get_ids", lambda store, name, allowed_statuses=None: [])
    refresh_vocab_store_cache()
    with pytest.raises(RuntimeError, match="empty"):
        required_vocab_ids("valid")

    monkeypatch.setattr(
        helper,
        "load_vocab_store",
        lambda path: (_ for _ in ()).throw(helper.VocabStoreError("failed")),
    )
    refresh_vocab_store_cache()
    with pytest.raises(RuntimeError, match="Unable to load vocabulary"):
        required_vocab_ids("valid")

