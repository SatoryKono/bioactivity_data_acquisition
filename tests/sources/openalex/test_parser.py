"""OpenAlex parser tests."""

from __future__ import annotations

from unittest.mock import patch

from tests.sources.openalex import OpenAlexAdapterTestCase


class TestOpenAlexParser(OpenAlexAdapterTestCase):
    """Validate parsing of OpenAlex API payloads."""

    def test_fetch_works_normalizes_single_payload(self) -> None:
        """``_fetch_works`` wraps dict payloads as a list."""

        adapter = self.adapter
        payload = {"id": "https://openalex.org/W1"}

        with patch.object(adapter.api_client, "request_json", return_value=payload):
            records = adapter._fetch_works("/works/W1")

        self.assertEqual(records, [payload])

    def test_fetch_batch_handles_errors(self) -> None:
        """Errors raised by ``_fetch_works`` are swallowed and logged."""

        adapter = self.adapter
        with patch.object(adapter, "_fetch_works", side_effect=RuntimeError("boom")):
            records = adapter._fetch_batch(["W1"])

        self.assertEqual(records, [])
