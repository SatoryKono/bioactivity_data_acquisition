"""Crossref REST API adapter."""

from typing import Any

from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.core.api_client import APIConfig
from bioetl.normalizers.bibliography import normalize_common_bibliography
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

    def _fetch_batch(self, dois: list[str]) -> list[dict[str, Any]]:
        """Fetch a batch of DOIs."""
        # Crossref requires individual queries for multiple DOIs
        # Fetch each DOI separately
        all_items = []
        for doi in dois:
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
            except Exception as e:
                self.logger.warning(
                    "fetch_doi_failed",
                    doi=doi,
                    error=str(e),
                    request_id=request_spec.metadata.get("request_id"),
                )

        return all_items

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize Crossref record."""
        common = normalize_common_bibliography(
            record,
            doi="DOI",
            title="title",
            journal=("container-title", "short-container-title"),
            authors="author",
        )

        normalized = dict(common)

        doi_clean = common.get("doi_clean")
        if doi_clean:
            normalized["crossref_doi"] = doi_clean

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
            subjects = "; ".join(record["subject"])
            normalized["subject"] = subjects

        # Doc type
        if "type" in record:
            normalized["doc_type"] = record["type"]

        return normalized

