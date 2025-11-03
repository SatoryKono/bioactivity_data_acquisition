"""Crossref REST API adapter."""

from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import quote

import pandas as pd
from requests import HTTPError

from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from bioetl.adapters.base import AdapterConfig, AdapterFetchError, ExternalAdapter
from bioetl.core.api_client import APIConfig
from bioetl.normalizers.bibliography import normalize_common_bibliography
from bioetl.sources.crossref.normalizer import (
    normalize_crossref_affiliations,
    normalize_crossref_authors,
)
from bioetl.sources.crossref.request import CrossrefRequestBuilder

NORMALIZER_ID, NORMALIZER_STRING = get_bibliography_normalizers()


class CrossrefAdapter(ExternalAdapter):
    """Adapter for Crossref REST API.

    The :meth:`_fetch_batch` method serves as the extension point for batch
    retrieval when combined with :meth:`ExternalAdapter._fetch_in_batches`.
    """

    DEFAULT_BATCH_SIZE = 100

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        """Initialize Crossref adapter."""
        super().__init__(api_config, adapter_config)
        self.request_builder = CrossrefRequestBuilder(api_config, adapter_config)
        self.api_client.session.headers.update(self.request_builder.base_headers)

    def _fetch_batch(self, ids: list[str]) -> list[dict[str, Any]]:
        """Fetch a batch of DOIs."""
        # Crossref requires individual queries for multiple DOIs
        # Fetch each DOI separately
        all_items: list[dict[str, Any]] = []
        failures: dict[str, str] = {}
        for doi in ids:
            url = f"/works/{doi}"
            request_spec = self.request_builder.build(url)
            try:
                response = self.api_client.request_json(
                    request_spec.url,
                    params=request_spec.params,
                    headers=request_spec.headers,
                )
                if isinstance(response, dict) and "message" in response:
                    all_items.append(response["message"])
            except Exception as exc:
                error_message = str(exc)
                failures[doi] = error_message
                self.logger.warning(
                    "fetch_doi_failed",
                    doi=doi,
                    error=error_message,
                    request_id=request_spec.metadata.get("request_id"),
                )

        if failures:
            raise AdapterFetchError(
                "Crossref batch experienced network errors",
                failed_ids=list(failures),
                partial_records=all_items,
                errors=failures,
            )

        return all_items

    def process_identifiers(
        self,
        *,
        pmids: Sequence[str] | None = None,
        dois: Sequence[str] | None = None,
        titles: Sequence[str] | None = None,
        records: Sequence[Mapping[str, str]] | None = None,
    ) -> pd.DataFrame:
        """Process identifiers with DOI and title fallbacks."""

        normalised_records: list[dict[str, Any]] = []
        failure_records: list[dict[str, Any]] = []

        doi_titles: dict[str, str] = {}
        if records:
            for entry in records:
                doi = entry.get("doi") or entry.get("chembl_doi")
                title = entry.get("title")
                if doi and title and doi not in doi_titles:
                    doi_titles[str(doi)] = str(title)

        title_iter = iter(titles or [])

        for doi in dict.fromkeys(dois or []):
            if not doi:
                continue

            record, error = self._fetch_work_by_doi(doi)
            if record:
                normalised = self.normalize_record(record)
                normalised.setdefault("crossref_doi", doi)
                normalised.setdefault("crossref_doi_clean", doi)
                normalised_records.append(normalised)
                continue

            error_message = error or "not_found"
            fallback_record = self._fetch_work_by_filter(doi)
            if fallback_record:
                normalised = self.normalize_record(fallback_record)
                normalised.setdefault("crossref_doi", doi)
                normalised.setdefault("crossref_doi_clean", doi)
                if error_message:
                    normalised.setdefault("crossref_error", error_message)
                normalised_records.append(normalised)
                continue

            title = doi_titles.get(doi)
            if title is None:
                try:
                    title = next(title_iter)
                except StopIteration:
                    title = None

            if title:
                search_record = self._search_by_title(title)
                if search_record:
                    normalised = self.normalize_record(search_record)
                    normalised.setdefault("crossref_doi", doi)
                    normalised.setdefault("crossref_doi_clean", doi)
                    if error_message:
                        normalised.setdefault("crossref_error", error_message)
                    normalised_records.append(normalised)
                    continue

            message = error or "not_found"
            failure_records.append(
                {
                    "crossref_doi": doi,
                    "crossref_doi_clean": doi,
                    "crossref_error": message,
                }
            )

        if not normalised_records and not failure_records:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        if normalised_records:
            frames.append(self.to_dataframe(normalised_records))
        if failure_records:
            frames.append(pd.DataFrame(failure_records))

        combined = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        return combined.reset_index(drop=True)

    def _fetch_work_by_doi(self, doi: str) -> tuple[dict[str, Any] | None, str | None]:
        """Fetch a single Crossref work by DOI returning payload and error message."""

        url = f"/works/{doi}"
        request_spec = self.request_builder.build(url)
        try:
            response = self.api_client.request_json(
                request_spec.url,
                params=request_spec.params,
                headers=request_spec.headers,
            )
        except HTTPError as exc:
            self.logger.warning(
                "fetch_doi_failed",
                doi=doi,
                error=str(exc),
                request_id=request_spec.metadata.get("request_id"),
            )
            return None, str(exc)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "fetch_doi_failed",
                doi=doi,
                error=str(exc),
                request_id=request_spec.metadata.get("request_id"),
            )
            return None, str(exc)

        message = response.get("message") if isinstance(response, dict) else None
        if isinstance(message, dict):
            return message, None
        return None, "no_message"

    def _fetch_work_by_filter(self, doi: str) -> dict[str, Any] | None:
        """Fallback to filter query for DOIs that fail direct lookup."""

        encoded = quote(doi, safe="")
        params = {"filter": f"doi:{encoded}"}
        spec = self.request_builder.build("/works", params=params)
        try:
            response = self.api_client.request_json(
                spec.url,
                params=spec.params,
                headers=spec.headers,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("crossref_filter_failed", doi=doi, error=str(exc))
            return None

        if isinstance(response, dict):
            message = response.get("message")
            if isinstance(message, dict):
                items = message.get("items")
                if isinstance(items, list) and items:
                    return items[0]
        return None

    def _search_by_title(self, title: str) -> dict[str, Any] | None:
        """Fallback search for works using ``query.bibliographic``."""

        params = {"query.bibliographic": title, "rows": 1}
        spec = self.request_builder.build("/works", params=params)
        try:
            response = self.api_client.request_json(
                spec.url,
                params=spec.params,
                headers=spec.headers,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("crossref_title_search_failed", title=title[:80], error=str(exc))
            return None

        if isinstance(response, dict):
            message = response.get("message")
            if isinstance(message, dict):
                items = message.get("items")
                if isinstance(items, list) and items:
                    return items[0]
        return None

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize Crossref record."""
        common = normalize_common_bibliography(
            record,
            doi="DOI",
            title="title",
            journal=("container-title", "short-container-title"),
            authors="author",
            authors_normalizer=normalize_crossref_authors,
            journal_normalizer=lambda value: (
                NORMALIZER_STRING.normalize(value) if value is not None else None
            ),
        )

        normalized = dict(common)

        doi_clean = common.get("doi_clean")
        if doi_clean:
            normalized["crossref_doi"] = doi_clean
            normalized.setdefault("crossref_doi_clean", doi_clean)

        # Publication dates - priority: published-print > published-online > issued > created
        date_field = None
        if "published-print" in record:
            date_field = record["published-print"]
        elif "published-online" in record:
            date_field = record["published-online"]
        elif "issued" in record:
            date_field = record["issued"]
        elif "created" in record:
            date_field = record["created"]

        if date_field and "date-parts" in date_field:
            date_parts = date_field["date-parts"][0] if date_field["date-parts"] else []
            if len(date_parts) > 0:
                normalized["year"] = date_parts[0]
            if len(date_parts) > 1:
                normalized["month"] = date_parts[1]
            if len(date_parts) > 2:
                normalized["day"] = date_parts[2]

        # Volume, Issue, Pages
        if "volume" in record:
            normalized["volume"] = str(record["volume"])
        if "issue" in record:
            normalized["issue"] = str(record["issue"])
        if "page" in record:
            normalized["first_page"] = str(record["page"])

        # ISSN
        if "ISSN" in record and record["ISSN"]:
            issn_list = record["ISSN"]
            normalized["issn_print"] = issn_list[0] if len(issn_list) > 0 else None
            normalized["issn_electronic"] = issn_list[1] if len(issn_list) > 1 else None

        # ISBN type - check if there's issn-type field
        if "issn-type" in record and record["issn-type"]:
            for issn_item in record["issn-type"]:
                issn_type = issn_item.get("type")
                issn_value = issn_item.get("value")
                if issn_type == "print" and issn_value:
                    normalized["issn_print"] = issn_value
                elif issn_type == "electronic" and issn_value:
                    normalized["issn_electronic"] = issn_value

        # Authors
        authors_raw = record.get("author")

        if isinstance(authors_raw, list):
            for author in authors_raw:
                orcid = author.get("ORCID") if isinstance(author, dict) else None
                if orcid:
                    orcid_normalized = NORMALIZER_ID.normalize_orcid(orcid)
                    if orcid_normalized:
                        normalized.setdefault("orcid", orcid_normalized)
                        break

        # Publisher
        if "publisher" in record:
            normalized["publisher"] = NORMALIZER_STRING.normalize(record["publisher"])

        # Subject
        if "subject" in record and record["subject"]:
            subjects = "; ".join(str(value) for value in record["subject"] if value)
            subject_normalized = NORMALIZER_STRING.normalize(subjects)
            if subject_normalized:
                normalized["subject"] = subject_normalized

        affiliations = normalize_crossref_affiliations(record.get("author"))
        if affiliations:
            normalized["author_affiliations"] = affiliations

        # Doc type
        if "type" in record:
            normalized["doc_type"] = record["type"]

        return normalized
