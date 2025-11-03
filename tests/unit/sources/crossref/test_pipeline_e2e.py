"""Crossref pipeline end-to-end tests."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from tests.unit.sources.crossref import CrossrefAdapterTestCase


class TestCrossrefPipelineE2E(CrossrefAdapterTestCase):
    """Simulate a fetch + normalize cycle as executed by the pipeline."""

    def test_fetch_and_normalize_roundtrip(self) -> None:
        """A fetched record can be normalized and materialised into a DataFrame."""

        adapter = self.adapter
        sample = {
            "DOI": "10.1111/example",
            "title": ["Pipeline Test"],
            "container-title": ["Journal"],
            "published-print": {"date-parts": [[2024, 5, 4]]},
            "author": [],
        }

        with patch.object(adapter, "_fetch_batch", return_value=[sample]):
            records = adapter.fetch_by_ids(["10.1111/example"])

        self.assertEqual(records, [sample])

        normalized = pd.DataFrame([adapter.normalize_record(record) for record in records])
        self.assertIn("crossref_doi", normalized.columns)
        self.assertEqual(normalized.loc[0, "crossref_doi"], "10.1111/example")
