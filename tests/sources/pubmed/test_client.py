"""PubMed client behaviour tests."""

from __future__ import annotations

from unittest.mock import patch

from tests.sources.pubmed import PubMedAdapterTestCase


class TestPubMedClient(PubMedAdapterTestCase):
    """Validate client level helpers for PubMed."""

    def test_fetch_by_ids_batches_using_class_default(self) -> None:
        """Batching fallback honours ``DEFAULT_BATCH_SIZE``."""

        adapter = self.ADAPTER_CLASS(
            self.api_config,
            self.make_adapter_config(batch_size=0, email="test@example.com", api_key="test_key"),
        )

        total = adapter.DEFAULT_BATCH_SIZE * 2 + 5
        identifiers = [str(100000 + i) for i in range(total)]

        with patch.object(adapter, "_fetch_batch", return_value=[], autospec=True) as batch_mock:
            adapter.fetch_by_ids(identifiers)

        expected_calls = -(-total // adapter.DEFAULT_BATCH_SIZE)
        self.assertEqual(batch_mock.call_count, expected_calls)
        self.assertEqual(len(batch_mock.call_args_list[0].args[-1]), adapter.DEFAULT_BATCH_SIZE)
        remainder = total % adapter.DEFAULT_BATCH_SIZE
        expected_last = remainder or adapter.DEFAULT_BATCH_SIZE
        self.assertEqual(len(batch_mock.call_args_list[-1].args[-1]), expected_last)

    def test_common_params_include_required_fields(self) -> None:
        """Email and tool metadata propagate to request parameters."""

        adapter = self.adapter
        self.assertEqual(adapter.common_params["email"], "test@example.com")
        self.assertEqual(adapter.common_params["tool"], "bioactivity_etl")
        self.assertEqual(adapter.common_params["api_key"], "test_key")
