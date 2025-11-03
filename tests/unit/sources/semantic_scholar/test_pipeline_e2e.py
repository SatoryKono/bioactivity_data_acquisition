"""Semantic Scholar pipeline end-to-end tests."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from tests.unit.sources.semantic_scholar import SemanticScholarAdapterTestCase


class TestSemanticScholarPipelineE2E(SemanticScholarAdapterTestCase):
    """Exercise batch fetch, normalization and DataFrame materialisation."""

    def test_fetch_and_normalize_roundtrip(self) -> None:
        adapter = self.adapter
        payload = {
            "paperId": "id-1",
            "externalIds": {"DOI": "10.1000/example"},
            "title": "Pipeline",
            "publicationDate": "2024-01-01",
            "authors": [],
        }

        with patch.object(adapter, "_fetch_batch", return_value=[payload]):
            records = adapter.fetch_by_ids(["10.1000/example"])

        normalized = pd.DataFrame([adapter.normalize_record(record) for record in records])
        self.assertIn("paper_id", normalized.columns)
        self.assertEqual(normalized.loc[0, "paper_id"], "id-1")
