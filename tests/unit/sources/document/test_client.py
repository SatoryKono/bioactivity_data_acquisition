"""Tests for document source configuration helpers."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.document.pipeline import AdapterDefinition, ExternalEnrichmentResult, FieldSpec


def test_field_spec_returns_default_values() -> None:
    """FieldSpec.get_default returns the configured default value."""

    spec = FieldSpec(default="fallback")
    assert spec.get_default() == "fallback"


def test_field_spec_uses_factory_without_sharing_instances() -> None:
    """Default factories should produce independent copies."""

    spec = FieldSpec(default_factory=lambda: {"key": "value"})

    first = spec.get_default()
    second = spec.get_default()

    assert first == {"key": "value"}
    assert second == {"key": "value"}
    assert first is not second


def test_adapter_definition_carries_configuration_spec() -> None:
    """AdapterDefinition stores adapter class and field specs."""

    class _Adapter:  # pragma: no cover - simple placeholder
        pass

    definition = AdapterDefinition(
        adapter_cls=_Adapter,
        api_fields={"base_url": FieldSpec(default="https://example.org")},
        adapter_fields={"batch_size": FieldSpec(default=100)},
    )

    assert definition.adapter_cls is _Adapter
    assert definition.api_fields["base_url"].get_default() == "https://example.org"
    assert definition.adapter_fields["batch_size"].get_default() == 100


def test_external_enrichment_result_flags_errors() -> None:
    """The ExternalEnrichmentResult helper reports error presence."""

    result = ExternalEnrichmentResult(
        dataframe=pd.DataFrame(),
        status="failed",
        errors={"pubmed": "timeout"},
    )
    assert result.has_errors() is True

    clean_result = ExternalEnrichmentResult(
        dataframe=pd.DataFrame(),
        status="completed",
        errors={},
    )
    assert clean_result.has_errors() is False
