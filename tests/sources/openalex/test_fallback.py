from __future__ import annotations

from unittest.mock import patch

from requests import HTTPError, Response

from tests.sources.openalex import OpenAlexAdapterTestCase


class TestOpenAlexFallback(OpenAlexAdapterTestCase):
    """Validate OpenAlex fallback search behaviour."""

    def test_title_search_used_when_doi_missing(self) -> None:
        adapter = self.adapter

        not_found = HTTPError(response=Response())
        not_found.response.status_code = 404

        def request_json(url: str, params: dict | None = None, headers: dict | None = None) -> dict:
            if url.endswith("/works/https://doi.org/10.1/missing"):
                raise not_found
            assert url.endswith("/works")
            if params and params.get("search"):
                return {
                    "results": [
                        {
                            "id": "https://openalex.org/W123",
                            "title": "Recovered",
                            "publication_year": 2020,
                            "authorships": [],
                        }
                    ]
                }
            return {"results": []}

        with patch.object(adapter.api_client, "request_json", side_effect=request_json) as mock_request:
            frame = adapter.process_identifiers(
                pmids=[],
                dois=["10.1/missing"],
                titles=["Recovered"],
                records=[{"doi": "10.1/missing", "title": "Recovered"}],
            )

        assert not frame.empty
        assert "openalex_doi" in frame.columns or "openalex_id" in frame.columns
        assert mock_request.call_count >= 2

    def test_failure_record_created_for_unresolved_ids(self) -> None:
        adapter = self.adapter

        not_found = HTTPError(response=Response())
        not_found.response.status_code = 404

        def request_json(url: str, params: dict | None = None, headers: dict | None = None) -> dict:
            raise not_found

        with patch.object(adapter.api_client, "request_json", side_effect=request_json):
            frame = adapter.process_identifiers(
                pmids=["123"],
                dois=["10.2/none"],
                titles=["Missing"],
                records=[{"pmid": "123", "title": "Missing"}],
            )

        assert "openalex_error" in frame.columns
        assert frame["openalex_error"].notna().any()
