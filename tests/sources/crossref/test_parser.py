"""Crossref parser tests."""

from __future__ import annotations

from unittest.mock import patch

from tests.sources.crossref import CrossrefAdapterTestCase


class TestCrossrefParser(CrossrefAdapterTestCase):
    """Verify parsing of Crossref API payloads."""

    def test_fetch_batch_extracts_message_payload(self) -> None:
        """API responses with ``message`` are flattened to record payloads."""

        adapter = self.adapter
        message = {"DOI": "10.1000/demo", "title": ["Demo"]}

        with patch.object(adapter.api_client, "request_json", return_value={"message": message}):
            records = adapter._fetch_batch(["10.1000/demo"])

        self.assertEqual(records, [message])

    def test_fetch_batch_handles_request_errors(self) -> None:
        """Errors from ``request_json`` are logged and skipped."""

        adapter = self.adapter

        with patch.object(adapter.api_client, "request_json", side_effect=RuntimeError("boom")):
            records = adapter._fetch_batch(["10.9999/error"])

        self.assertEqual(records, [])
