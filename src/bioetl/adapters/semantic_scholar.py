"""Semantic Scholar Graph API adapter."""

import os
from typing import Any

import pandas as pd

from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.core.api_client import APIConfig
from bioetl.normalizers import registry
from bioetl.normalizers.bibliography import (
    normalize_authors,
    normalize_doi,
    normalize_title,
)

NORMALIZER_ID = registry.get("identifier")
NORMALIZER_STRING = registry.get("string")


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

    def _fetch_batch(self, ids: list[str]) -> list[dict[str, Any]]:
        """Fetch a batch of papers by their IDs."""
        records = []

        for paper_id in ids:
            try:
                record = self._fetch_paper(paper_id)
                if record:
                    records.append(record)
            except Exception as e:
                self.logger.error("fetch_paper_error", paper_id=paper_id, error=str(e))

        return records

    def fetch_by_titles(self, titles: list[str]) -> list[dict[str, Any]]:
        """Fetch records by titles using search endpoint."""
        if not titles:
            return []

        all_records = []

        for title in titles:
            if not title or pd.isna(title):
                continue

            try:
                # Search for paper by title
                results = self._search_by_title(title)
                if results:
                    # Take first result that matches
                    all_records.extend(results[:1])  # Take only first match
            except Exception as e:
                self.logger.error("search_by_title_error", title=title[:100], error=str(e))

        return all_records

    def process_titles(self, titles: list[str]) -> pd.DataFrame:
        """Process list of titles: fetch, normalize, convert to DataFrame."""
        if not titles:
            self.logger.info("no_titles_provided", adapter=self.__class__.__name__)
            return pd.DataFrame()

        self.logger.info("starting_fetch_by_titles", adapter=self.__class__.__name__, count=len(titles))

        # Fetch records
        raw_records = self.fetch_by_titles(titles)

        if not raw_records:
            self.logger.warning("no_records_fetched", adapter=self.__class__.__name__)
            return pd.DataFrame()

        # Normalize each record
        normalized_records = []
        for raw_record in raw_records:
            try:
                normalized = self.normalize_record(raw_record)
                if normalized:
                    normalized_records.append(normalized)
            except Exception as e:
                self.logger.error(
                    "normalization_failed",
                    adapter=self.__class__.__name__,
                    error=str(e),
                )
                continue

        # Convert to DataFrame
        df = self.to_dataframe(normalized_records)
        self.logger.info("fetch_completed", adapter=self.__class__.__name__, fetched=len(raw_records), normalized=len(normalized_records))

        return df

    def _search_by_title(self, title: str) -> list[dict[str, Any]]:
        """Search for papers by title."""
        url = "/paper/search"
        params = {
            "query": title,
            "fields": "paperId,externalIds,title,abstract,venue,year,publicationDate,referenceCount,citationCount,influentialCitationCount,isOpenAccess,publicationTypes,fieldsOfStudy,authors",
            "limit": 1,  # Only need first match
        }

        try:
            response: dict[str, Any] = self.api_client.request_json(url, params=params)
            data = response.get("data", [])
            if isinstance(data, list):
                return data
            return []
        except Exception as e:
            self.logger.error("search_error", title=title[:100], error=str(e))
            return []

    def _fetch_paper(self, paper_id: str) -> dict[str, Any] | None:
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
            return response
        except Exception as e:
            # Handle 403 Access Denied
            if "403" in str(e):
                self.logger.error("access_denied_403", paper_id=paper_id)
            self.logger.error("fetch_paper_error", paper_id=paper_id, error=str(e))
            return None

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
        normalized = {}

        # Paper ID
        if "paperId" in record:
            normalized["paper_id"] = record["paperId"]

        # External IDs
        if "externalIds" in record:
            ex_ids = record["externalIds"]
            doi_clean = normalize_doi(ex_ids.get("DOI")) if isinstance(ex_ids, dict) else None
            if doi_clean:
                normalized["doi_clean"] = doi_clean
            if "PubMed" in ex_ids:
                normalized["pubmed_id"] = NORMALIZER_ID.normalize(str(ex_ids["PubMed"]))

        # Title
        title = normalize_title(record.get("title"))
        if title:
            normalized["title"] = title
            normalized["_title_for_join"] = title

        # Abstract
        if "abstract" in record:
            normalized["abstract"] = NORMALIZER_STRING.normalize(record["abstract"])

        # Venue (journal)
        if "venue" in record:
            normalized["journal"] = NORMALIZER_STRING.normalize(record["venue"])

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

        # Authors
        authors = normalize_authors(record.get("authors"))
        if authors:
            normalized["authors"] = authors

        # ISSN - not typically available in Semantic Scholar API
        # Semantic Scholar API does not provide ISSN data in their paper endpoint
        # This field will remain empty for Semantic Scholar records

        return normalized

