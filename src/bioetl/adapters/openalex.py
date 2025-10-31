"""OpenAlex Works API adapter."""

from typing import Any

from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.core.api_client import APIConfig
from bioetl.normalizers.bibliography import normalize_common_bibliography
from bioetl.sources.openalex.request import OpenAlexRequestBuilder

NORMALIZER_ID, NORMALIZER_STRING = get_bibliography_normalizers()


class OpenAlexAdapter(ExternalAdapter):
    """Adapter for OpenAlex Works API.

    Override :meth:`_fetch_batch` to plug into the shared batching helper.
    """

    DEFAULT_BATCH_SIZE = 100
    DEFAULT_WORK_FIELDS = (
        "id,doi,title,authorships,primary_location,publication_date,"
        "publication_year,type,language,open_access,concepts,ids,cited_by_count"
    )

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        super().__init__(api_config, adapter_config)
        self.request_builder = OpenAlexRequestBuilder(api_config, adapter_config)
        self.api_client.session.headers.update(self.request_builder.base_headers)

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
                clean_doi = doi.replace("https://doi.org/", "")
                url = f"/works/https://doi.org/{clean_doi}"
                try:
                    item = self._fetch_works(url)
                except Exception as exc:  # pragma: no cover - defensive
                    self.logger.error("fetch_works_error", error=str(exc), url=url)
                    continue
                if item:
                    all_items.extend(item)

        # Fetch by PMID - OpenAlex requires single PMID per request
        if pmids:
            for pmid in pmids:
                url = f"/works/pmid:{pmid}"
                try:
                    item = self._fetch_works(url)
                except Exception as exc:  # pragma: no cover - defensive
                    self.logger.error("fetch_works_error", error=str(exc), url=url)
                    continue
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
                try:
                    item = self._fetch_works(url)
                except Exception as exc:  # pragma: no cover - defensive
                    self.logger.error("fetch_works_error", error=str(exc), url=url)
                    continue
                if item:
                    all_items.extend(item)

        return all_items

    def _fetch_works(self, url: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Fetch works from OpenAlex API."""
        try:
            query_params = dict(params or {})
            query_params.setdefault("fields", self.DEFAULT_WORK_FIELDS)
            spec = self.request_builder.build(url, params=query_params)
            response = self.api_client.request_json(
                spec.url,
                params=spec.params,
                headers=spec.headers,
            )
            # OpenAlex returns single object for /works/{id}
            if isinstance(response, dict):
                if "results" in response:
                    return response["results"]
                else:
                    # Single work
                    return [response]
            return []
        except Exception as e:
            self.logger.error(
                "fetch_works_error",
                error=str(e),
                url=url,
                request_id=spec.metadata.get("request_id"),
            )
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
            normalized["openalex_doi_clean"] = doi_clean

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

        if "cited_by_count" in record:
            normalized["openalex_citation_count"] = record.get("cited_by_count")

        return normalized

