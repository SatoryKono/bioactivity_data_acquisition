"""OpenAlex client behaviour tests."""

from __future__ import annotations

from unittest.mock import patch

from tests.sources.openalex import OpenAlexAdapterTestCase


class TestOpenAlexClient(OpenAlexAdapterTestCase):
    """Validate OpenAlex client batching and polite behaviour."""

    def test_fetch_by_ids_batches_using_class_default(self) -> None:
        """Batch helper falls back to ``DEFAULT_BATCH_SIZE``."""

        adapter = self.ADAPTER_CLASS(
            self.api_config,
            self.make_adapter_config(batch_size=0),
        )

        total = adapter.DEFAULT_BATCH_SIZE * 2 + 5
        identifiers = [f"10.1234/openalex{i}" for i in range(total)]

        with patch.object(adapter, "_fetch_batch", return_value=[], autospec=True) as batch_mock:
            adapter.fetch_by_ids(identifiers)

        expected_calls = -(-total // adapter.DEFAULT_BATCH_SIZE)
        self.assertEqual(batch_mock.call_count, expected_calls)
        self.assertEqual(len(batch_mock.call_args_list[0].args[1]), adapter.DEFAULT_BATCH_SIZE)
        remainder = total % adapter.DEFAULT_BATCH_SIZE
        expected_last = remainder or adapter.DEFAULT_BATCH_SIZE
        self.assertEqual(len(batch_mock.call_args_list[-1].args[1]), expected_last)

    def test_fetch_batch_routes_identifiers(self) -> None:
        """Identifiers are routed via DOI, PMID and OpenAlex helpers."""

        adapter = self.adapter
        with (
            patch.object(adapter, "_fetch_works", return_value=[{"id": "foo"}]) as works_mock,
        ):
            records = adapter._fetch_batch(["10.1/demo", "12345", "W123"])

        self.assertEqual(len(records), 3)
        recorded_urls = [recorded.args[0] for recorded in works_mock.call_args_list]
        self.assertIn("/works/https://doi.org/10.1/demo", recorded_urls)
        self.assertIn("/works/pmid:12345", recorded_urls)
