from __future__ import annotations

from unittest.mock import patch

from requests import HTTPError, Response

from tests.unit.sources.crossref import CrossrefAdapterTestCase


class TestCrossrefFallback(CrossrefAdapterTestCase):
    """Ensure Crossref adapter applies filter and title fallbacks."""

    def test_filter_fallback_returns_record(self) -> None:
        adapter = self.adapter

        response_payload = {
            "message": {
                "items": [
                    {
                        "DOI": "10.1/test",
                        "title": ["Fallback Title"],
                        "author": [],
                    }
                ]
            }
        }

        not_found = HTTPError(response=Response())
        not_found.response.status_code = 404

        def request_json(url: str, params: dict | None = None, headers: dict | None = None) -> dict:
            if url.endswith("/works/10.1/test"):
                raise not_found
            assert url.endswith("/works")
            assert params and params.get("filter")
            return response_payload

        with patch.object(adapter.api_client, "request_json", side_effect=request_json) as mock_request:
            frame = adapter.process_identifiers(
                pmids=[],
                dois=["10.1/test"],
                titles=["Fallback Title"],
                records=[{"doi": "10.1/test", "title": "Fallback Title"}],
            )

        assert not frame.empty
        assert "crossref_doi" in frame.columns
        assert frame.loc[0, "crossref_doi"].lower() == "10.1/test"
        assert mock_request.call_count == 2

    def test_title_fallback_records_failure(self) -> None:
        adapter = self.adapter

        response_payload = {
            "message": {
                "items": [
                    {
                        "DOI": "10.2/alt",
                        "title": ["Alternate"],
                        "author": [],
                    }
                ]
            }
        }

        not_found = HTTPError(response=Response())
        not_found.response.status_code = 404

        def request_json(url: str, params: dict | None = None, headers: dict | None = None) -> dict:
            if url.endswith("/works/10.2/missing"):
                raise not_found
            if params and params.get("filter"):
                return {"message": {"items": []}}
            assert params and params.get("query.bibliographic")
            return response_payload

        with patch.object(adapter.api_client, "request_json", side_effect=request_json):
            frame = adapter.process_identifiers(
                pmids=[],
                dois=["10.2/missing"],
                titles=["Alternate"],
                records=[{"doi": "10.2/missing", "title": "Alternate"}],
            )

        assert "crossref_error" in frame.columns
        errors = frame.loc[frame["crossref_error"].notna(), "crossref_error"].tolist()
        assert errors and errors[0]
        matches = frame.loc[frame["crossref_doi"].notna(), "crossref_doi"].tolist()
        assert any("10.2" in value for value in matches) or not matches
