"""PubChem normalizer tests."""

from __future__ import annotations

from bioetl.utils.json import canonical_json
from tests.sources.pubchem import PubChemAdapterTestCase


class TestPubChemNormalizer(PubChemAdapterTestCase):
    """Validate PubChem normalization helpers."""

    def test_normalize_record_serializes_synonyms_with_canonical_json(self) -> None:
        """Synonym collections should be serialized deterministically."""

        adapter = self.adapter
        synonyms = [
            {"name": "beta", "type": "primary"},
            {"name": "alpha", "type": "alternate"},
        ]
        record = {
            "Synonyms": synonyms,
            "CID": 42,
            "_source_identifier": "ABC",
        }

        normalized = adapter.normalize_record(record)

        expected = canonical_json(synonyms)
        self.assertEqual(normalized["pubchem_synonyms"], expected)
