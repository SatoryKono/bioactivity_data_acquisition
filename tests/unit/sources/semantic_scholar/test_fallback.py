from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from tests.unit.sources.semantic_scholar import SemanticScholarAdapterTestCase


class TestSemanticScholarFallback(SemanticScholarAdapterTestCase):
    """Ensure Semantic Scholar adapter applies the configured fallback order."""

    def test_doi_precedes_pmid_and_titles_invoked(self) -> None:
        adapter = self.adapter

        doi_record = {
            "paperId": "WDOI",
            "externalIds": {"DOI": "10.1/x"},
            "title": "Via DOI",
        }

        def fake_fetch(paper_id: str) -> dict | None:
            if paper_id.startswith("10.1"):
                return doi_record
            raise RuntimeError("missing")

        with (
            patch.object(adapter, "_fetch_paper", side_effect=fake_fetch) as fetch_mock,
            patch.object(
                adapter,
                "process_titles",
                return_value=pd.DataFrame({"title": ["Fallback"]}),
            ) as titles_mock,
        ):
            frame = adapter.process_identifiers(
                pmids=["123"],
                dois=["10.1/x"],
                titles=["Fallback"],
                records=[{"doi": "10.1/x", "pmid": "123", "title": "Fallback"}],
            )

        assert not frame.empty
        first_call = fetch_mock.call_args_list[0].args[0]
        assert first_call == "10.1/x"
        assert any(args[0] == "123" for args, _ in [(call.args, call.kwargs) for call in fetch_mock.call_args_list[1:]])
        titles_mock.assert_called_once_with(["Fallback"])
