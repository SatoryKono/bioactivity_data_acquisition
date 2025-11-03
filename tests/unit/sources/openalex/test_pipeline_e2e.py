"""OpenAlex pipeline end-to-end tests."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from tests.unit.sources.openalex import OpenAlexAdapterTestCase


class TestOpenAlexPipelineE2E(OpenAlexAdapterTestCase):
    """Exercise the fetch and normalize cycle for OpenAlex."""

    def test_fetch_and_normalize_roundtrip(self) -> None:
        adapter = self.adapter
        payload = {
            "id": "https://openalex.org/W123",
            "doi": "10.1000/example",
            "title": "Pipeline",
            "publication_date": "2024-02-01",
            "concepts": [],
            "open_access": {},
            "primary_location": {"source": {}},
        }

        with patch.object(adapter, "_fetch_batch", return_value=[payload]):
            records = adapter.fetch_by_ids(["W123"])

        normalized = pd.DataFrame([adapter.normalize_record(record) for record in records])
        self.assertIn("openalex_id", normalized.columns)
        self.assertEqual(normalized.loc[0, "openalex_id"], "W123")
