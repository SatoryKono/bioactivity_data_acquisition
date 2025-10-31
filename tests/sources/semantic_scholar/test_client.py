"""Semantic Scholar client behaviour tests."""

from __future__ import annotations

from unittest.mock import patch

from tests.sources.semantic_scholar import SemanticScholarAdapterTestCase


class TestSemanticScholarClient(SemanticScholarAdapterTestCase):
    """Validate Semantic Scholar client helpers."""

    def test_fetch_by_ids_batches_using_class_default(self) -> None:
        """Batch helper respects ``DEFAULT_BATCH_SIZE`` fallback."""

        adapter = self.ADAPTER_CLASS(
            self.api_config,
            self.make_adapter_config(batch_size=0, api_key="test_key"),
        )

        total = adapter.DEFAULT_BATCH_SIZE * 2 + 5
        identifiers = [f"DOI:{i}" for i in range(total)]

        with patch.object(adapter, "_fetch_batch", return_value=[], autospec=True) as batch_mock:
            adapter.fetch_by_ids(identifiers)

        expected_calls = -(-total // adapter.DEFAULT_BATCH_SIZE)
        self.assertEqual(batch_mock.call_count, expected_calls)
        first_batch_args = batch_mock.call_args_list[0].args
        self.assertEqual(len(first_batch_args[-1]), adapter.DEFAULT_BATCH_SIZE)
        remainder = total % adapter.DEFAULT_BATCH_SIZE
        expected_last = remainder or adapter.DEFAULT_BATCH_SIZE
        last_batch_args = batch_mock.call_args_list[-1].args
        self.assertEqual(len(last_batch_args[-1]), expected_last)

    def test_format_paper_id(self):
        """Paper IDs are formatted for Semantic Scholar endpoints."""

        self.assertEqual(self.adapter._format_paper_id("10.1371/journal.pone.0123456"), "10.1371/journal.pone.0123456")
        self.assertEqual(self.adapter._format_paper_id("12345678"), "PMID:12345678")
        self.assertEqual(self.adapter._format_paper_id("arXiv:1234.5678"), "arXiv:1234.5678")
