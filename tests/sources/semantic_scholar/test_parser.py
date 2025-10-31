"""Semantic Scholar parser tests."""

from __future__ import annotations

from unittest.mock import patch

from tests.sources.semantic_scholar import SemanticScholarAdapterTestCase


class TestSemanticScholarParser(SemanticScholarAdapterTestCase):
    """Validate parsing behaviour for Semantic Scholar."""

    def test_fetch_batch_handles_errors(self) -> None:
        adapter = self.adapter
        with patch.object(adapter, "_fetch_paper", side_effect=RuntimeError("boom")):
            records = adapter._fetch_batch(["id1"])

        self.assertEqual(records, [])

    def test_fetch_paper_uses_formatted_id(self) -> None:
        adapter = self.adapter
        with patch.object(adapter.api_client, "request_json", return_value={"paperId": "id1"}) as request_mock:
            record = adapter._fetch_paper("123")

        self.assertEqual(record, {"paperId": "id1"})
        request_mock.assert_called_once()
        called_url = request_mock.call_args.args[0]
        self.assertIn("PMID:123", called_url)
