from hashlib import sha256

import pytest

from bioetl.core.pipeline.processing import (
    BusinessKeyDeduplicator,
    CleanupTransformer,
    ColumnHashingTransformer,
    MappingLookupEnricher,
    NormalizationTransformer,
    PreferPrimaryMergeStrategy,
    ProcessingChain,
    SHA256BusinessKeyDeriver,
    StaticSideInputProvider,
)


def test_normalization_and_cleanup():
    records = [
        {" Name ": "  Aspirin ", "Country_Code": "US", "notes": ""},
        {" Name ": "  ", "Country_Code": None},
    ]
    chain = ProcessingChain(
        transformers=[
            NormalizationTransformer(field_renames={" Name ": "name", "Country_Code": "country_code"}),
        ],
    )
    normalized = chain.run(records)

    assert len(normalized) == 2
    assert normalized[0]["name"] == "Aspirin"
    assert normalized[0]["country_code"] == "US"
    assert "notes" in normalized[0]

    cleanup_chain = ProcessingChain(
        transformers=[
            NormalizationTransformer(field_renames={" Name ": "name", "Country_Code": "country_code"}),
            ColumnHashingTransformer(["name"]),
        ],
    )
    cleaned = cleanup_chain.run(records)
    assert len(cleaned) == 2
    assert all(len(row["name_hash"]) == 64 for row in cleaned)


def test_cleanup_drops_missing_required_fields_and_nulls():
    records = [
        {"name": "Aspirin", "country": None, "notes": ""},
        {"name": "Ibuprofen", "country": "DE", "notes": None},
    ]

    chain = ProcessingChain(
        transformers=[
            CleanupTransformer(required_fields=("country",), drop_null_fields=("notes",)),
        ],
    )

    cleaned = chain.run(records)

    assert len(cleaned) == 1
    assert cleaned[0]["name"] == "Ibuprofen"
    assert "notes" not in cleaned[0]


@pytest.mark.parametrize("default_region", [None, "N/A"])
def test_lookup_enrichment_and_business_key(default_region):
    records = [
        {"name": "Aspirin", "country_code": "FR"},
        {"name": "Aspirin", "country_code": "FR"},
    ]
    chain = ProcessingChain(
        transformers=[NormalizationTransformer()],
        side_input_providers=[StaticSideInputProvider("countries", {"FR": {"country": "France", "region": "EU"}})],
        enricher=MappingLookupEnricher(lookup_name="countries", source_field="country_code", default={"region": default_region}),
        business_key_deriver=SHA256BusinessKeyDeriver(["name", "country"]),
        deduplicator=BusinessKeyDeduplicator(),
    )

    enriched = chain.run(records)

    assert len(enriched) == 1
    enriched_record = enriched[0]
    assert enriched_record["country"] == "France"
    assert "region" in enriched_record
    assert len(enriched_record["business_key"]) == 64


def test_merge_strategy_with_secondary_records():
    primary_records = [{"name": "aspirin", "country": "France"}]
    hasher = sha256("name=aspirin".encode("utf-8")).hexdigest()
    secondary_records = [
        {"business_key": hasher, "name": "aspirin", "source": "warehouse"},
        {"business_key": sha256("name=paracetamol".encode("utf-8")).hexdigest(), "name": "paracetamol"},
    ]

    chain = ProcessingChain(
        transformers=[NormalizationTransformer()],
        business_key_deriver=SHA256BusinessKeyDeriver(["name"], hasher=None),
        merge_strategy=PreferPrimaryMergeStrategy(),
    )

    merged = chain.run(primary_records, secondary_records=secondary_records)

    assert len(merged) == 2
    aspirin = next(item for item in merged if item["name"] == "aspirin")
    assert aspirin["country"] == "France"
    assert aspirin["source"] == "warehouse"

