"""Semantic Scholar Graph API adapter."""

import os
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import backoff
import pandas as pd
from requests import RequestException

from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from bioetl.adapters.base import AdapterConfig, AdapterFetchError, ExternalAdapter
from bioetl.core.api_client import APIConfig
from bioetl.normalizers.bibliography import normalize_common_bibliography

NORMALIZER_ID, NORMALIZER_STRING = get_bibliography_normalizers()


class SemanticScholarAdapter(ExternalAdapter):
    """Adapter for Semantic Scholar Graph API.

    Override :meth:`_fetch_batch` to integrate with the shared batching helper
    for ID-based retrieval.
    """

    DEFAULT_BATCH_SIZE = 50

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        """Initialize Semantic Scholar adapter."""
        super().__init__(api_config, adapter_config)

        # API key is required for production
        self.api_key = adapter_config.__dict__.get("api_key", os.getenv("SEMANTIC_SCHOLAR_API_KEY", ""))
        if self.api_key:
            self.api_client.session.headers["x-api-key"] = self.api_key
        else:
            self.logger.warning("semantic_scholar_api_key_missing", note="Rate limits will be lower")

        interval = float(api_config.rate_limit_period) / max(1, int(api_config.rate_limit_max_calls))
        self._backoff_interval = max(interval, 0.1)
        self._backoff_max_tries = max(1, int(self.api_client.retry_policy.total))

    def _fetch_batch(self, ids: list[str]) -> list[dict[str, Any]]:
        """Fetch a batch of papers by their IDs."""
        records: list[dict[str, Any]] = []
        failures: dict[str, str] = {}

        for paper_id in ids:
            try:
                record = self._fetch_paper(paper_id)
            except AdapterFetchError as exc:
                failures[paper_id] = str(exc)
                if exc.partial_records:
                    records.extend(exc.partial_records)
                continue
            except Exception as exc:  # pragma: no cover - defensive
                error_message = str(exc)
                failures[paper_id] = error_message
                self.logger.error("fetch_paper_error", paper_id=paper_id, error=error_message)
                continue

            if record:
                dict_record: dict[str, Any] = dict(record)
                records.append(dict_record)

        if failures:
            raise AdapterFetchError(
                "Semantic Scholar batch experienced network errors",
                failed_ids=list(failures),
                partial_records=records,
                errors=failures,
            )

        return records

    def fetch_by_titles(self, titles: list[str]) -> list[dict[str, Any]]:
        """Fetch records by titles using search endpoint."""
        if not titles:
            return []

        all_records: list[dict[str, Any]] = []
        failures: dict[str, str] = {}

        for title in titles:
            if not title or pd.isna(title):
                continue

            try:
                # Search for paper by title
                results = self._search_by_title(title)
            except AdapterFetchError as exc:
                failures[title] = str(exc)
                if exc.partial_records:
                    all_records.extend(exc.partial_records)
                continue
            except Exception as exc:  # pragma: no cover - defensive
                error_message = str(exc)
                failures[title] = error_message
                self.logger.error("search_by_title_error", title=title[:100], error=error_message)
                continue

            if results:
                # Take first result that matches
                all_records.extend(results[:1])  # Take only first match

        if failures:
            raise AdapterFetchError(
                "Semantic Scholar title search experienced network errors",
                failed_ids=list(failures),
                partial_records=all_records,
                errors=failures,
            )

        return all_records

    def process_titles(self, titles: list[str]) -> pd.DataFrame:
        """Process list of titles: fetch, normalize, convert to DataFrame."""
        return self._process_collection(
            titles,
            self.fetch_by_titles,
            start_event="starting_fetch_by_titles",
            no_items_event="no_titles_provided",
        )

    def process_identifiers(
        self,
        *,
        pmids: Sequence[str] | None = None,
        dois: Sequence[str] | None = None,
        titles: Sequence[str] | None = None,
        records: Sequence[Mapping[str, str]] | None = None,
    ) -> pd.DataFrame:
        """Process Semantic Scholar identifiers with fallback ordering."""

        frames: list[pd.DataFrame] = []
        seen_pmids: set[str] = set()
        seen_dois: set[str] = set()

        doi_title_map: dict[str, str] = {}
        pmid_title_map: dict[str, str] = {}
        if records:
            for entry in records:
                title = entry.get("title")
                if not title:
                    continue
                title_str = str(title)
                doi = entry.get("doi") or entry.get("chembl_doi")
                pmid = entry.get("pmid") or entry.get("chembl_pmid")
                if doi and doi not in doi_title_map:
                    doi_title_map[str(doi)] = title_str
                if pmid and pmid not in pmid_title_map:
                    pmid_title_map[str(pmid)] = title_str

        title_pool: set[str] = {str(value) for value in titles or [] if value}
        for mapping in (doi_title_map, pmid_title_map):
            title_pool.update(mapping.values())

        def _collect(frame: pd.DataFrame) -> None:
            if frame.empty:
                return
            frames.append(frame)
            if "semantic_scholar_doi" in frame.columns:
                seen_dois.update(frame["semantic_scholar_doi"].dropna().astype(str))
            if "doi_clean" in frame.columns:
                seen_dois.update(frame["doi_clean"].dropna().astype(str))
            if "pubmed_id" in frame.columns:
                seen_pmids.update(frame["pubmed_id"].dropna().astype(str))

        doi_list = list(dict.fromkeys(dois or []))
        if doi_list:
            doi_frame = self.process(doi_list)
            _collect(doi_frame)
            for doi in doi_list:
                if doi in seen_dois:
                    title = doi_title_map.get(doi)
                    if title:
                        title_pool.discard(title)

        remaining_pmids: list[str] = []
        for pmid in dict.fromkeys(pmids or []):
            if pmid and pmid not in seen_pmids:
                remaining_pmids.append(str(pmid))

        if remaining_pmids:
            pmid_frame = self.process(remaining_pmids)
            _collect(pmid_frame)
            for pmid in remaining_pmids:
                if pmid in seen_pmids:
                    title = pmid_title_map.get(pmid)
                    if title:
                        title_pool.discard(title)

        title_candidates = [title for title in title_pool if title]
        if title_candidates:
            title_frame = self.process_titles(title_candidates)
            _collect(title_frame)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        if "paper_id" in combined.columns:
            combined = combined.drop_duplicates(subset=["paper_id"], keep="first")
        return combined.reset_index(drop=True)

    def _search_by_title(self, title: str) -> list[dict[str, Any]]:
        """Search for papers by title."""
        url = "/paper/search"
        params = {
            "query": title,
            "fields": "paperId,externalIds,title,abstract,venue,year,publicationDate,referenceCount,citationCount,influentialCitationCount,isOpenAccess,publicationTypes,fieldsOfStudy,authors",
            "limit": 1,  # Only need first match
        }

        try:
            response: Mapping[str, Any] = self.api_client.request_json(url, params=params)
        except Exception as exc:  # pragma: no cover - defensive
            error_message = str(exc) or "Semantic Scholar search failed"
            raise AdapterFetchError(
                "Semantic Scholar search request failed",
                failed_ids=[title],
                errors={title: error_message},
            ) from exc

        data = response.get("data", [])
        if isinstance(data, list):
            return data
        return []

    def _fetch_paper(self, paper_id: str) -> Mapping[str, Any] | None:
        """Fetch a single paper by ID (DOI, PMID, or ArXiv)."""
        # Format paper ID
        formatted_id = self._format_paper_id(paper_id)

        url = f"/paper/{formatted_id}"
        # Fields to request
        params = {
            "fields": "paperId,externalIds,title,abstract,venue,year,publicationDate,referenceCount,citationCount,influentialCitationCount,isOpenAccess,publicationTypes,fieldsOfStudy",
        }

        try:
            response = self.api_client.request_json(url, params=params)
        except Exception as exc:
            error_message = str(exc) or "Semantic Scholar fetch failed"
            if "403" in error_message:
                self.logger.error("access_denied_403", paper_id=paper_id)
            self.logger.error("fetch_paper_error", paper_id=paper_id, error=error_message)
            raise AdapterFetchError(
                "Semantic Scholar paper request failed",
                failed_ids=[paper_id],
                errors={paper_id: error_message},
            ) from exc
        return response

    def _format_paper_id(self, paper_id: str) -> str:
        """Format paper ID for Semantic Scholar API."""
        # If DOI, just return as is
        if paper_id.startswith("10."):
            return paper_id

        # If numeric, assume it's PMID
        if paper_id.isdigit():
            return f"PMID:{paper_id}"

        # If ArXiv ID
        if paper_id.startswith("arXiv:"):
            return paper_id

        # Otherwise return as is
        return paper_id

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize Semantic Scholar record."""
        common = normalize_common_bibliography(
            record,
            doi=lambda rec: (
                (rec.get("externalIds") or {}).get("DOI")
                if isinstance(rec.get("externalIds"), dict)
                else None
            ),
            title="title",
            journal="venue",
            authors="authors",
            journal_normalizer=lambda value: (
                NORMALIZER_STRING.normalize(value) if value is not None else None
            ),
        )

        normalized = dict(common)

        # Paper ID
        if "paperId" in record:
            normalized["paper_id"] = record["paperId"]

        # External IDs
        if "externalIds" in record:
            ex_ids = record["externalIds"]
            if "PubMed" in ex_ids:
                normalized["pubmed_id"] = NORMALIZER_ID.normalize(str(ex_ids["PubMed"]))

        doi_clean = common.get("doi_clean")
        if doi_clean:
            normalized["doi_clean"] = doi_clean
            normalized["semantic_scholar_doi"] = doi_clean

        if "title" in normalized:
            normalized["_title_for_join"] = normalized["title"]

        # Abstract
        if "abstract" in record:
            normalized["abstract"] = NORMALIZER_STRING.normalize(record["abstract"])

        # Year
        if "year" in record:
            normalized["year"] = record["year"]

        # Publication date
        if "publicationDate" in record:
            normalized["publication_date"] = record["publicationDate"]

        # Citation metrics (unique to Semantic Scholar)
        if "citationCount" in record:
            normalized["citation_count"] = record["citationCount"]
        if "influentialCitationCount" in record:
            normalized["influential_citations"] = record["influentialCitationCount"]
        if "referenceCount" in record:
            normalized["reference_count"] = record["referenceCount"]

        # Open Access
        if "isOpenAccess" in record:
            normalized["is_oa"] = record["isOpenAccess"]

        # Publication types
        if "publicationTypes" in record and record["publicationTypes"]:
            normalized["publication_types"] = [pt.lower() for pt in record["publicationTypes"]]
            # Also add doc_type field
            normalized["doc_type"] = "; ".join([pt.lower() for pt in record["publicationTypes"]])

        # Fields of study
        if "fieldsOfStudy" in record and record["fieldsOfStudy"]:
            normalized["fields_of_study"] = record["fieldsOfStudy"]

        # ISSN - not typically available in Semantic Scholar API
        # Semantic Scholar API does not provide ISSN data in their paper endpoint
        # This field will remain empty for Semantic Scholar records

        return normalized

    def _call_with_backoff(self, operation: Callable[[], Any]) -> Any:
        """Execute an API operation respecting configured rate limits."""

        wrapped = backoff.on_exception(
            wait_gen=backoff.constant,
            exception=RequestException,
            interval=self._backoff_interval,
            max_tries=self._backoff_max_tries,
        )(operation)
        return wrapped()

