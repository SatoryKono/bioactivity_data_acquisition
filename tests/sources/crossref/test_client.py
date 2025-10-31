"""Crossref client behaviour tests."""

from __future__ import annotations

from unittest.mock import patch

from tests.sources.crossref import CrossrefAdapterTestCase


class TestCrossrefClient(CrossrefAdapterTestCase):
    """Validate Crossref client level helpers and batching."""

    def test_fetch_by_ids_batches_using_class_default(self) -> None:
        """Batch helper respects ``DEFAULT_BATCH_SIZE`` fallback."""

        adapter = self.ADAPTER_CLASS(
            self.api_config,
            self.make_adapter_config(batch_size=0, mailto="test@example.com"),
        )

        total = adapter.DEFAULT_BATCH_SIZE * 2 + 5
        identifiers = [f"10.1234/test{i}" for i in range(total)]

        with patch.object(adapter, "_fetch_batch", return_value=[], autospec=True) as batch_mock:
            adapter.fetch_by_ids(identifiers)

        expected_calls = -(-total // adapter.DEFAULT_BATCH_SIZE)
        self.assertEqual(batch_mock.call_count, expected_calls)
        first_batch = batch_mock.call_args_list[0].args[1]
        self.assertEqual(len(first_batch), adapter.DEFAULT_BATCH_SIZE)
        last_batch = batch_mock.call_args_list[-1].args[1]
        remainder = total % adapter.DEFAULT_BATCH_SIZE
        expected_last = remainder or adapter.DEFAULT_BATCH_SIZE
        self.assertEqual(len(last_batch), expected_last)

    def test_mailto_header_added_to_user_agent(self) -> None:
        """Polite pool mailto is appended to the API client's user agent."""

        adapter = self.adapter

        user_agent = adapter.api_client.session.headers.get("User-Agent", "")
        self.assertIn("mailto:test@example.com", user_agent)
