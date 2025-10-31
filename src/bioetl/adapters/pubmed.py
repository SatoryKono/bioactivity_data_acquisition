"""PubMed E-utilities API adapter."""

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pandas as pd

from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.core.api_client import APIConfig
from bioetl.normalizers.bibliography import normalize_common_bibliography
from bioetl.sources.pubmed.request import PubMedRequestBuilder
from bioetl.sources.pubmed.pagination import WebEnvPaginator
from bioetl.sources.pubmed.parser import parse_efetch_response

NORMALIZER_ID, NORMALIZER_STRING = get_bibliography_normalizers()


class PubMedAdapter(ExternalAdapter):
    """Adapter for PubMed E-utilities API.

    :meth:`_fetch_batch` implements the PubMed-specific fetching that is reused
    via the shared batching helper.
    """

    DEFAULT_BATCH_SIZE = 200

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        """Initialize PubMed adapter."""
        super().__init__(api_config, adapter_config)

        self.tool = adapter_config.__dict__.get("tool", "bioactivity_etl")
        self.email = adapter_config.__dict__.get("email", "")
        self.api_key = adapter_config.__dict__.get("api_key", "")

        if not self.email:
            self.logger.warning("pubmed_email_missing", note="PubMed requires 'email' parameter")

        self.request_builder = PubMedRequestBuilder(api_config, adapter_config)
        self.api_client.session.headers.update(self.request_builder.base_headers)
        self.common_params: dict[str, Any] = {"tool": self.tool}
        if self.email:
            self.common_params["email"] = self.email
        if self.api_key:
            self.common_params["api_key"] = self.api_key

    def _fetch_batch(self, pmids: list[str]) -> list[dict[str, Any]]:
        """Fetch a batch of PMIDs using EFetch directly."""
        request_spec = self.request_builder.efetch(pmids)

        try:
            payload = self.api_client.request_text(
                request_spec.url,
                params=request_spec.params,
                method="GET",
                headers=request_spec.headers,
            )
        except Exception as exc:
            self.logger.error(
                "fetch_batch_error",
                error=str(exc),
                pmids=pmids[:3],
                request_id=request_spec.metadata.get("request_id"),
            )
            return []

        return parse_efetch_response(payload)

    def search(
        self,
        search_params: Mapping[str, Any],
        *,
        fetch_params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute an ``esearch``/``efetch`` workflow and return raw records."""

        batch_size = self.adapter_config.batch_size
        if not isinstance(batch_size, int) or batch_size <= 0:
            batch_size = self.DEFAULT_BATCH_SIZE

        paginator = WebEnvPaginator(self.api_client, batch_size=batch_size)
        return paginator.fetch_all(search_params, fetch_params=fetch_params)

    def process_search(
        self,
        search_params: Mapping[str, Any],
        *,
        fetch_params: Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Run :meth:`search` and normalise the resulting records."""

        records = self.search(search_params, fetch_params=fetch_params)
        if not records:
            return pd.DataFrame()

        normalised = [self.normalize_record(record) for record in records]
        return self.to_dataframe(normalised)

    def process_identifiers(
        self,
        *,
        pmids: Sequence[str] | None = None,
        dois: Sequence[str] | None = None,
        titles: Sequence[str] | None = None,
        records: Sequence[Mapping[str, str]] | None = None,
    ) -> pd.DataFrame:
        """Process identifiers with DOI/title fallbacks when PMIDs are missing."""

        frames: list[pd.DataFrame] = []
        seen_pmids: set[str] = set()

        def _collect(frame: pd.DataFrame) -> None:
            if frame is None or frame.empty:
                return
            frames.append(frame)
            if "pmid" in frame.columns:
                seen_pmids.update(frame["pmid"].dropna().astype(str).tolist())
            elif "pubmed_pmid" in frame.columns:
                seen_pmids.update(frame["pubmed_pmid"].dropna().astype(str).tolist())

        pmid_list = list(dict.fromkeys(pmids or []))
        if pmid_list:
            _collect(self.process(pmid_list))

        doi_list = list(dict.fromkeys(dois or []))
        title_list = list(dict.fromkeys(titles or []))

        fallback_pmids: list[str] = []
        if doi_list:
            doi_pmids = self.resolve_pmids_from_dois(doi_list)
            fallback_pmids.extend(doi_pmids)

        title_candidates: list[str] = []
        if records:
            for entry in records:
                if entry.get("pmid"):
                    continue
                title_value = entry.get("title")
                if title_value:
                    title_candidates.append(title_value)
        if not title_candidates:
            title_candidates = title_list

        if title_candidates:
            title_pmids = self._resolve_pmids_from_titles(title_candidates)
            fallback_pmids.extend(title_pmids)

        missing_pmids: list[str] = []
        for pmid in fallback_pmids:
            if not pmid or pmid in seen_pmids:
                continue
            missing_pmids.append(pmid)

        if missing_pmids:
            self.logger.info(
                "pubmed_fallback_fetch", missing=len(missing_pmids), sources=len(fallback_pmids)
            )
            _collect(self.process(missing_pmids))

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        if "pmid" in combined.columns:
            combined = combined.drop_duplicates(subset=["pmid"], keep="first")
        elif "pubmed_pmid" in combined.columns:
            combined = combined.drop_duplicates(subset=["pubmed_pmid"], keep="first")
        return combined.reset_index(drop=True)

    def resolve_pmids_from_dois(self, dois: Sequence[str]) -> list[str]:
        """Resolve PMIDs associated with provided DOIs via ``esearch``."""

        return self._resolve_pmids_from_terms(
            (
                (doi, f'"{self._sanitize_query_value(doi)}"[AID]')
                for doi in dict.fromkeys(dois or [])
                if doi
            ),
            label="doi",
        )

    def _resolve_pmids_from_titles(self, titles: Sequence[str]) -> list[str]:
        """Resolve PMIDs for titles when DOI lookup fails."""

        return self._resolve_pmids_from_terms(
            (
                (title, f'"{self._sanitize_query_value(title)}"[Title]')
                for title in dict.fromkeys(titles or [])
                if title
            ),
            label="title",
        )

    def _resolve_pmids_from_terms(
        self,
        terms: Iterable[tuple[str, str]],
        *,
        label: str,
    ) -> list[str]:
        """Query ``esearch`` terms returning flattened PMIDs."""

        resolved: list[str] = []
        for raw_value, term in terms:
            spec = self.request_builder.esearch(
                {
                    "db": "pubmed",
                    "retmode": "json",
                    "retmax": 5,
                    "term": term,
                }
            )
            try:
                payload = self.api_client.request_json(
                    spec.url,
                    params=spec.params,
                    headers=spec.headers,
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "pubmed_esearch_failed",
                    query=term,
                    identifier=raw_value,
                    label=label,
                    error=str(exc),
                )
                continue

            pmids = self._extract_pmids(payload)
            if not pmids:
                continue

            resolved.extend(pmids)

        return resolved

    @staticmethod
    def _extract_pmids(payload: Mapping[str, Any] | None) -> list[str]:
        """Extract PMIDs from an ``esearch`` response payload."""

        if not isinstance(payload, Mapping):
            return []

        result = payload.get("esearchresult")
        if isinstance(result, Mapping):
            data = result
        else:
            data = payload

        id_list = data.get("idlist") or data.get("IdList")
        if isinstance(id_list, Mapping):  # pragma: no cover - compatibility
            candidates = list(id_list.values())
        else:
            candidates = id_list

        pmids: list[str] = []
        if isinstance(candidates, Sequence) and not isinstance(candidates, (str, bytes)):
            for value in candidates:
                if value in (None, ""):
                    continue
                pmid = str(value)
                if pmid:
                    pmids.append(pmid)
        elif candidates not in (None, ""):
            pmids.append(str(candidates))
        return pmids

    @staticmethod
    def _sanitize_query_value(value: str) -> str:
        """Sanitise query values for PubMed search terms."""

        return value.replace("\"", " ").strip()

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize PubMed record."""
        common = normalize_common_bibliography(
            record,
            doi="doi",
            title="title",
            journal="journal",
            authors="authors",
            journal_normalizer=lambda value: (
                NORMALIZER_STRING.normalize(value) if value is not None else None
            ),
        )

        normalized = dict(common)

        # PMID
        pmid_value = record.get("pmid")
        pmid_int: int | None = None
        if pmid_value not in (None, ""):
            try:
                pmid_int = int(pmid_value)
            except (TypeError, ValueError):
                pmid_int = None

        if pmid_int is not None:
            normalized["pmid"] = pmid_int
            normalized["pubmed_pmid"] = pmid_int
            normalized["pubmed_id"] = NORMALIZER_ID.normalize(str(pmid_int))
        else:
            normalized.setdefault("pmid", None)
            normalized.setdefault("pubmed_pmid", None)
            normalized.setdefault("pubmed_id", None)

        doi_clean = common.get("doi_clean")
        if doi_clean:
            normalized["pubmed_doi"] = doi_clean

        abstract = record.get("abstract")
        if isinstance(abstract, str):
            normalized["abstract"] = NORMALIZER_STRING.normalize(abstract)

        journal_abbrev = record.get("journal_abbrev")
        if isinstance(journal_abbrev, str):
            normalized["journal_abbrev"] = NORMALIZER_STRING.normalize(journal_abbrev)

        # ISSN
        issn_values = record.get("issn")
        if isinstance(issn_values, list) and issn_values:
            normalized["issn_print"] = issn_values[0] if len(issn_values) > 0 else None
            normalized["issn_electronic"] = issn_values[1] if len(issn_values) > 1 else None
            joined_issn = "; ".join(str(value) for value in issn_values if value)
            if joined_issn:
                normalized["pubmed_issn"] = NORMALIZER_STRING.normalize(joined_issn)

        # Volume, Issue
        volume_value = record.get("volume")
        if volume_value:
            normalized["volume"] = str(volume_value)
        issue_value = record.get("issue")
        if issue_value:
            normalized["issue"] = str(issue_value)

        # Pages
        first_page = record.get("first_page")
        if first_page:
            normalized["first_page"] = str(first_page)
        last_page = record.get("last_page")
        if last_page:
            normalized["last_page"] = str(last_page)

        # Date
        year_value = record.get("year")
        if isinstance(year_value, int):
            normalized["year"] = year_value
        month_value = record.get("month")
        if isinstance(month_value, int):
            normalized["month"] = month_value
        day_value = record.get("day")
        if isinstance(day_value, int):
            normalized["day"] = day_value

        def _join_terms(values: Any) -> str | None:
            if not isinstance(values, list):
                return None
            normalised_terms: list[str] = []
            for value in values:
                if not value:
                    continue
                as_string = NORMALIZER_STRING.normalize(str(value))
                if as_string:
                    normalised_terms.append(as_string)
            if not normalised_terms:
                return None
            return " | ".join(normalised_terms)

        mesh_terms = _join_terms(record.get("mesh_terms") or record.get("mesh_descriptors"))
        if mesh_terms:
            normalized["mesh_descriptors"] = mesh_terms

        mesh_qualifiers = _join_terms(record.get("mesh_qualifiers"))
        if mesh_qualifiers:
            normalized["mesh_qualifiers"] = mesh_qualifiers

        chemical_list = _join_terms(record.get("chemicals") or record.get("chemical_list"))
        if chemical_list:
            normalized["chemical_list"] = chemical_list

        # Doc type
        doc_type = record.get("doc_type")
        if isinstance(doc_type, str):
            normalized["doc_type"] = NORMALIZER_STRING.normalize(doc_type)

        # Dates
        for key in ("year_completed", "month_completed", "day_completed", "year_revised", "month_revised", "day_revised"):
            value = record.get(key)
            if isinstance(value, int):
                normalized[key] = value

        return normalized

