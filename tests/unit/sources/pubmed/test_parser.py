"""PubMed adapter parsing and search tests."""

from __future__ import annotations

from collections.abc import Mapping
from unittest.mock import MagicMock, patch

import pandas as pd

from tests.unit.sources.pubmed import PubMedAdapterTestCase


class TestPubMedParser(PubMedAdapterTestCase):
    """Validate interaction between the adapter and parsing helpers."""

    def test_fetch_batch_uses_parse_helper(self) -> None:
        """The adapter should delegate XML parsing to :func:`parse_efetch_response`."""

        adapter = self.adapter

        with (
            patch.object(adapter.api_client, "request_text", return_value="<xml/>") as request_mock,
            patch("bioetl.adapters.pubmed.parse_efetch_response", return_value=[{"pmid": 1}]) as parser_mock,
        ):
            records = adapter._fetch_batch(["123"])

        request_mock.assert_called_once()
        parser_mock.assert_called_once_with("<xml/>")
        self.assertEqual(records, [{"pmid": 1}])

    def test_fetch_batch_logs_and_swallows_errors(self) -> None:
        """Network errors should be logged and result in an empty batch."""

        adapter = self.adapter

        with patch.object(adapter.api_client, "request_text", side_effect=RuntimeError("boom")):
            records = adapter._fetch_batch(["456"])

        self.assertEqual(records, [])


class TestPubMedSearch(PubMedAdapterTestCase):
    """Verify the ``esearch`` + ``efetch`` orchestration helper."""

    def test_search_uses_paginator(self) -> None:
        """``search`` should instantiate :class:`WebEnvPaginator` with adapter settings."""

        adapter = self.adapter

        paginator_mock = MagicMock()
        paginator_mock.fetch_all.return_value = [{"pmid": 1}]

        with patch("bioetl.adapters.pubmed.WebEnvPaginator", return_value=paginator_mock) as paginator_cls:
            results = adapter.search({"term": "aspirin"})

        paginator_cls.assert_called_once()
        kwargs = paginator_cls.call_args.kwargs
        self.assertEqual(kwargs.get("batch_size"), adapter.adapter_config.batch_size)
        paginator_mock.fetch_all.assert_called_once_with({"term": "aspirin"}, fetch_params=None)
        self.assertEqual(results, [{"pmid": 1}])

    def test_process_search_returns_dataframe(self) -> None:
        """``process_search`` normalises search results into a dataframe."""

        adapter = self.adapter
        raw_record: Mapping[str, object] = {
            "pmid": 123,
            "title": "Example",
            "journal": "Journal",
            "authors": [{"last_name": "Doe", "fore_name": "Jane"}],
        }

        with patch.object(adapter, "search", return_value=[raw_record]):
            frame = adapter.process_search({"term": "example"})

        assert isinstance(frame, pd.DataFrame)
        assert "title" in frame.columns
        assert frame.loc[0, "title"] == "Example"
