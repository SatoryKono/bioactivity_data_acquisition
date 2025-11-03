"""Tests for the PubChem normalization layer."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.pubchem.normalizer.pubchem_normalizer import PubChemNormalizer
from bioetl.utils.json import canonical_json


class DummyPubChemClient:
    """Minimal client stub exposing ``enrich_batch``."""

    def __init__(self, records: list[dict[str, object]]):
        self.records = records
        self.calls: list[list[str]] = []

    def enrich_batch(self, inchikeys: list[str]) -> list[dict[str, object]]:
        self.calls.append(inchikeys)
        return self.records


def test_normalize_record_serializes_synonyms_deterministically() -> None:
    normalizer = PubChemNormalizer(timestamp_factory=lambda: "2024-01-01T00:00:00+00:00")
    synonyms = [
        {"name": "beta", "type": "primary"},
        {"name": "alpha", "type": "alternate"},
    ]
    record = {
        "CID": 123,
        "Synonyms": synonyms,
        "_source_identifier": "XYZ",
    }

    normalized = normalizer.normalize_record(record)

    assert normalized["pubchem_synonyms"] == canonical_json(synonyms)
    assert normalized["pubchem_cid"] == 123
    assert normalized["pubchem_lookup_inchikey"] == "XYZ"


def test_ensure_columns_populates_missing_fields() -> None:
    normalizer = PubChemNormalizer()
    df = pd.DataFrame({"molecule_chembl_id": ["CHEMBL1"]})

    ensured = normalizer.ensure_columns(df)

    for column in normalizer.get_pubchem_columns():
        assert column in ensured.columns


def test_enrich_dataframe_merges_records_using_inchikey() -> None:
    normalizer = PubChemNormalizer(timestamp_factory=lambda: "2024-01-01T00:00:00+00:00")
    df = pd.DataFrame(
        {
            "molecule_chembl_id": ["CHEMBL1"],
            "standard_inchi_key": ["ABCDEFGHIJKLMN-OPQRSTUVWX-Y"],
        }
    )
    client = DummyPubChemClient(
        [
            {
                "CID": 999,
                "SMILES": "CC",
                "_source_identifier": "ABCDEFGHIJKLMN-OPQRSTUVWX-Y",
                "_cid_source": "inchikey",
                "_enrichment_attempt": 1,
                "_fallback_used": False,
                "InChIKey": "ABCDEFGHIJKLMN-OPQRSTUVWX-Y",
            }
        ]
    )

    enriched = normalizer.enrich_dataframe(df, client=client)

    assert int(enriched.loc[0, "pubchem_cid"]) == 999
    assert enriched.loc[0, "pubchem_enrichment_attempt"] == 1
    assert enriched.loc[0, "pubchem_inchi_key"] == "ABCDEFGHIJKLMN-OPQRSTUVWX-Y"
    assert client.calls == [["ABCDEFGHIJKLMN-OPQRSTUVWX-Y"]]
