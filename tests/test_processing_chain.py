import hashlib
from typing import Mapping

import pytest

from src.bioetl.core.pipeline.processing import (
    BusinessKeyDeduplicator,
    CleanupTransformer,
    ColumnHasher,
    CompositeSideInputProvider,
    HashingTransformer,
    MergeByBusinessKey,
    NormalizationTransformer,
    ProcessingChain,
    SHA256BusinessKeyDeriver,
    SimpleLookupEnricher,
    StaticSideInputProvider,
)


@pytest.fixture
def sample_records() -> list[Mapping[str, object]]:
    return [
        {"Id": 1, "Name": " Foo ", "Value": 10},
        {"Id": 2, "Name": "Bar", "Value": 10},
        {"Id": 1, "Name": "foo", "Value": 10},
    ]


def test_normalization_and_cleanup(sample_records: list[Mapping[str, object]]) -> None:
    normalizer = NormalizationTransformer(field_map={"Id": "entity_id"})
    cleanup = CleanupTransformer(required_fields=["entity_id"], defaults={"status": "active"})

    normalized = normalizer.transform(sample_records)
    cleaned = cleanup.transform(normalized)

    assert all("entity_id" in record for record in normalized)
    assert all(record["status"] == "active" for record in cleaned)
    assert cleaned[0]["name"] == "Foo"


def test_side_inputs_and_lookup_enrichment() -> None:
    provider = CompositeSideInputProvider(
        [
            StaticSideInputProvider({"countries": {"US": {"name": "United States"}}}),
            StaticSideInputProvider({"countries": {"GB": {"name": "United Kingdom"}}}),
        ]
    )
    lookups = provider.load()["countries"]
    enricher = SimpleLookupEnricher(lookup=lookups, record_key="country_code", merge_to_field="country")

    enriched = enricher.enrich(
        [
            {"country_code": "US", "value": 1},
            {"country_code": "GB", "value": 2},
            {"country_code": "DE", "value": 3},
        ]
    )

    assert enriched[0]["country"] == {"name": "United States"}
    assert "country" not in enriched[2]


def test_business_key_and_hash_generation(sample_records: list[Mapping[str, object]]) -> None:
    normalizer = NormalizationTransformer(field_map={"Id": "entity_id"})
    normalized = normalizer.transform(sample_records)

    key_deriver = SHA256BusinessKeyDeriver(fields=["entity_id", "name"])
    derived = key_deriver.derive(normalized)

    assert all(len(record["business_key"]) == 64 for record in derived)

    hasher = ColumnHasher(fields=["entity_id", "name", "value"])
    hashing_transformer = HashingTransformer(hasher=hasher, target_field="row_hash")
    hashed = hashing_transformer.transform(derived)

    expected_hash = hashlib.sha256(
        b'{"entity_id": 1, "name": "Foo", "value": 10}'
    ).hexdigest()
    assert hashed[0]["row_hash"] == expected_hash


def test_deduplication_strategies(sample_records: list[Mapping[str, object]]) -> None:
    normalizer = NormalizationTransformer(field_map={"Id": "entity_id"})
    normalized = normalizer.transform(sample_records)
    key_deriver = SHA256BusinessKeyDeriver(fields=["entity_id", "name"])
    derived = key_deriver.derive(normalized)

    deduplicator_first = BusinessKeyDeduplicator(keep="first")
    deduplicated_first = deduplicator_first.deduplicate(derived)
    assert len(deduplicated_first) == 2
    assert deduplicated_first[0]["entity_id"] == 1

    deduplicator_last = BusinessKeyDeduplicator(keep="last")
    deduplicated_last = deduplicator_last.deduplicate(derived)
    assert len(deduplicated_last) == 2
    assert any(record["name"] == "foo" for record in deduplicated_last)


def test_merge_strategy() -> None:
    merge_strategy = MergeByBusinessKey(
        merge_func=lambda primary, secondary: {**primary, **{"value": secondary.get("value", primary.get("value"))}}
    )
    primary = [
        {"business_key": "a", "value": 1},
        {"business_key": "b", "value": 2},
    ]
    secondary = [
        {"business_key": "b", "value": 20},
        {"business_key": "c", "value": 3},
    ]

    merged = list(merge_strategy.merge(primary, secondary))
    assert any(record["business_key"] == "b" and record["value"] == 20 for record in merged)
    assert any(record["business_key"] == "c" for record in merged)


def test_full_processing_chain(sample_records: list[Mapping[str, object]]) -> None:
    provider = StaticSideInputProvider({"countries": {"us": {"country": "United States"}}})
    lookup = SimpleLookupEnricher(lookup={"1": {"country": "United States"}}, record_key="entity_id")

    chain = ProcessingChain(
        normalizers=[NormalizationTransformer(field_map={"Id": "entity_id"})],
        cleanup=CleanupTransformer(required_fields=["entity_id"]),
        side_input_provider=provider,
        enricher=lookup,
        business_key_deriver=SHA256BusinessKeyDeriver(fields=["entity_id", "name"]),
        deduplicator=BusinessKeyDeduplicator(),
        hash_generator=HashingTransformer(hasher=ColumnHasher(fields=["entity_id", "name", "value"]), target_field="row_hash"),
    )

    processed = chain.run(sample_records)
    assert all("business_key" in record for record in processed)
    assert all(len(record["business_key"]) == 64 for record in processed)
    assert all("row_hash" in record for record in processed)
    assert len(processed) == 2  # дедупликация по бизнес-ключу
