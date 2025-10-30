"""OpenAlex Works API adapter."""

from typing import Any

from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from bioetl.adapters.base import ExternalAdapter
from bioetl.normalizers.bibliography import normalize_common_bibliography

NORMALIZER_ID, NORMALIZER_STRING = get_bibliography_normalizers()


class OpenAlexAdapter(ExternalAdapter):
    """Adapter for OpenAlex Works API.

    Override :meth:`_fetch_batch` to plug into the shared batching helper.
    """

    DEFAULT_BATCH_SIZE = 100

    def _fetch_batch(self, ids: list[str]) -> list[dict[str, Any]]:
        """Fetch a batch of works."""
        # Try to fetch by DOI first
        dois = [id for id in ids if id.startswith("10.")]
        pmids = [id for id in ids if id not in dois and id.isdigit()]
        oa_ids = [id for id in ids if id not in dois and id not in pmids]

        all_items = []

        # Fetch by DOI - OpenAlex requires single DOI per request
        if dois:
            for doi in dois:
                # Remove https://doi.org/ prefix if present
                clean_doi = doi.replace("https://doi.org/", "")
                url = f"/works/https://doi.org/{clean_doi}"
                item = self._fetch_works(url)
                if item:
                    all_items.extend(item)

        # Fetch by PMID - OpenAlex requires single PMID per request
        if pmids:
            for pmid in pmids:
                url = f"/works/pmid:{pmid}"
                item = self._fetch_works(url)
                if item:
                    all_items.extend(item)

        # Fetch by OpenAlex ID
        if oa_ids:
            # Can fetch directly by ID
            for oa_id in oa_ids:
                # Ensure it's a full URL
                if not oa_id.startswith("https://"):
                    oa_id = f"https://openalex.org/{oa_id}"
                url = f"/works/{oa_id.split('/')[-1]}"
                item = self._fetch_works(url)
                if item:
                    all_items.extend(item)

        return all_items

    def _fetch_works(self, url: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Fetch works from OpenAlex API."""
        try:
            response = self.api_client.request_json(url, params=params or {})
            # OpenAlex returns single object for /works/{id}
            if isinstance(response, dict):
                if "results" in response:
                    return response["results"]
                else:
                    # Single work
                    return [response]
            return []
        except Exception as e:
            self.logger.error("fetch_works_error", error=str(e), url=url)
            return []

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize OpenAlex record."""
        common = normalize_common_bibliography(
            record,
            doi="doi",
            title="title",
            journal=lambda rec: (
                (rec.get("primary_location") or {})
                .get("source", {})
                .get("display_name")
            )
            or (
                (rec.get("primary_location") or {})
                .get("source", {})
                .get("name")
            ),
            authors="authorships",
            journal_normalizer=lambda value: (
                NORMALIZER_STRING.normalize(value) if value is not None else None
            ),
        )

        normalized = dict(common)

        # OpenAlex ID
        if "id" in record:
            normalized["openalex_id"] = NORMALIZER_ID.normalize_openalex_id(record["id"])

        doi_clean = common.get("doi_clean")
        if doi_clean:
            normalized["openalex_doi"] = doi_clean

        # PMID from ids
        if "ids" in record:
            ids = record["ids"]
            if "pmid" in ids:
                pmid = ids["pmid"].replace("https://pubmed.ncbi.nlm.nih.gov/", "")
                normalized["openalex_pmid"] = int(pmid) if pmid.isdigit() else None

        # Publication date
        if "publication_date" in record:
            normalized["publication_date"] = record["publication_date"]
            # Parse year from date
            if record["publication_date"]:
                parts = record["publication_date"].split("-")
                if parts:
                    normalized["year"] = int(parts[0]) if parts[0].isdigit() else None

        if "publication_year" in record:
            normalized["year"] = record["publication_year"]

        # Type
        if "type" in record:
            normalized["type"] = record["type"]

        # Language
        if "language" in record:
            normalized["language"] = record["language"]

        # Open Access info
        if "open_access" in record:
            oa = record["open_access"]
            normalized["is_oa"] = oa.get("is_oa", False)
            normalized["oa_status"] = oa.get("oa_status")
            normalized["oa_url"] = oa.get("oa_url")

        # Concepts - top 3 by score
        if "concepts" in record and record["concepts"]:
            concepts_sorted = sorted(
                record["concepts"], key=lambda c: c.get("score", 0), reverse=True
            )
            top3 = concepts_sorted[:3]
            normalized["concepts_top3"] = [
                c.get("display_name", "") for c in top3 if c.get("display_name")
            ]

        # Venue (journal) and ISSN
        if "primary_location" in record and record["primary_location"]:
            source = record["primary_location"].get("source")
            if source and isinstance(source, dict):
                # ISSN
                if "issn_l" in source and source["issn_l"]:
                    normalized["openalex_issn"] = source["issn_l"]
                elif "issn" in source and source["issn"]:
                    issn_list = source["issn"] if isinstance(source["issn"], list) else [source["issn"]]
                    normalized["openalex_issn"] = "; ".join(issn_list) if issn_list else None

                # Crossref doc type if available
                if "type" in source:
                    normalized["crossref_doc_type"] = source["type"]

        # Authors (primary location)
        if "primary_location" in record and record["primary_location"]:
            landing_page = record["primary_location"].get("landing_page_url")
            if landing_page:
                normalized["landing_page"] = landing_page

        # Doc type from record
        if "type" in normalized:
            normalized["doc_type"] = normalized["type"]

        return normalized

