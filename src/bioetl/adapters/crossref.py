"""Crossref REST API adapter."""

import os
from typing import Any

from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.core.api_client import APIConfig
from bioetl.normalizers import registry

NORMALIZER_ID = registry.get("identifier")
NORMALIZER_STRING = registry.get("string")


class CrossrefAdapter(ExternalAdapter):
    """Adapter for Crossref REST API.

    The :meth:`_fetch_batch` method serves as the extension point for batch
    retrieval when combined with :meth:`ExternalAdapter._fetch_in_batches`.
    """

    DEFAULT_BATCH_SIZE = 100

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        """Initialize Crossref adapter."""
        super().__init__(api_config, adapter_config)

        # Get mailto for polite pool
        self.mailto = adapter_config.__dict__.get("mailto", os.getenv("CROSSREF_MAILTO", ""))
        if self.mailto:
            # Add to headers for polite pool
            self.api_client.session.headers["User-Agent"] = f"bioactivity_etl/1.0 (mailto:{self.mailto})"

    def _fetch_batch(self, dois: list[str]) -> list[dict[str, Any]]:
        """Fetch a batch of DOIs."""
        # Crossref requires individual queries for multiple DOIs
        # Fetch each DOI separately
        all_items = []
        for doi in dois:
            url = f"/works/{doi}"
            try:
                response = self.api_client.request_json(url, params={})
                if isinstance(response, dict) and "message" in response:
                    all_items.append(response["message"])
            except Exception as e:
                self.logger.warning("fetch_doi_failed", doi=doi, error=str(e))

        return all_items

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize Crossref record."""
        normalized = {}

        # DOI
        if "DOI" in record:
            normalized["doi_clean"] = NORMALIZER_ID.normalize(record["DOI"])
            # Also add crossref_doi field
            normalized["crossref_doi"] = normalized["doi_clean"]

        # Title - Crossref returns list
        if "title" in record and record["title"]:
            titles = [t for t in record["title"] if t]
            if titles:
                normalized["title"] = NORMALIZER_STRING.normalize(titles[0])

        # Container title (journal)
        if "container-title" in record and record["container-title"]:
            container_titles = [t for t in record["container-title"] if t]
            if container_titles:
                normalized["journal"] = NORMALIZER_STRING.normalize(container_titles[0])

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
        if "author" in record and record["author"]:
            authors = []
            for author in record["author"]:
                given = author.get("given", "")
                family = author.get("family", "")
                if family:
                    author_name = f"{family}"
                    if given:
                        author_name = f"{family}, {given}"
                    authors.append(author_name)

                    # Add ORCID if available
                    orcid = author.get("ORCID")
                    if orcid:
                        orcid_normalized = NORMALIZER_ID.normalize_orcid(orcid)
                        if orcid_normalized:
                            # Store ORCID for first author only for simplicity
                            if len(authors) == 1:
                                normalized["orcid"] = orcid_normalized

            if authors:
                normalized["authors"] = "; ".join(authors)

        # Publisher
        if "publisher" in record:
            normalized["publisher"] = NORMALIZER_STRING.normalize(record["publisher"])

        # Subject
        if "subject" in record and record["subject"]:
            subjects = "; ".join(record["subject"])
            normalized["subject"] = subjects

        # Doc type
        if "type" in record:
            normalized["doc_type"] = record["type"]

        return normalized

