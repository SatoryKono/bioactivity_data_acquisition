"""Client for the OpenAlex API."""
from __future__ import annotations

from typing import Any

from library.clients.base import ApiClientError, BaseApiClient
from library.config import APIClientConfig


class OpenAlexClient(BaseApiClient):
    """HTTP client for OpenAlex works."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def fetch_by_doi(self, doi: str) -> dict[str, Any]:
        """Fetch a work by DOI with fallback to a filter query."""

        # Use OpenAlex API format: https://api.openalex.org/works/https://doi.org/{doi}
        path = f"https://api.openalex.org/works/https://doi.org/{doi}"
        try:
            payload = self._request("GET", path)
            return self._parse_work(payload)
        except ApiClientError as exc:
            # Специальная обработка для ошибок 429 от OpenAlex
            if exc.status_code == 429:
                self.logger.warning(
                    "openalex_rate_limited",
                    doi=doi,
                    error=str(exc),
                    message="OpenAlex API rate limit exceeded. Consider getting an API key."
                )
                return self._create_empty_record(doi, f"Rate limited: {str(exc)}")
            
            self.logger.info("openalex_doi_fallback", doi=doi, error=str(exc))
            try:
                # Fallback to OpenAlex search API
                payload = self._request("GET", "", params={"filter": f"doi:{doi}"})
                results = payload.get("results", [])
                if not results:
                    raise
                return self._parse_work(results[0])
            except ApiClientError as fallback_exc:
                if fallback_exc.status_code == 429:
                    return self._create_empty_record(doi, f"Rate limited: {str(fallback_exc)}")
                raise

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any]:
        """Fetch a work by PMID with fallback search."""

        try:
            payload = self._request("GET", "", params={"filter": f"pmid:{pmid}"})
            results = payload.get("results", [])
            if results:
                return self._parse_work(results[0])

            self.logger.info("openalex_pmid_fallback", pmid=pmid)
            payload = self._request("GET", "", params={"search": pmid})
            results = payload.get("results", [])
            if not results:
                raise ApiClientError(f"No OpenAlex work found for PMID {pmid}")
            return self._parse_work(results[0])
        except ApiClientError as exc:
            # Специальная обработка для ошибок 429 от OpenAlex
            if exc.status_code == 429:
                self.logger.warning(
                    "openalex_rate_limited",
                    pmid=pmid,
                    error=str(exc),
                    message="OpenAlex API rate limit exceeded. Consider getting an API key."
                )
                return self._create_empty_record(pmid, f"Rate limited: {str(exc)}")
            raise

    def _parse_work(self, work: dict[str, Any]) -> dict[str, Any]:
        ids = work.get("ids") or {}
        
        # Debug logging
        self.logger.debug("openalex_parse_work", 
                         work_type=work.get("type"),
                         work_type_crossref=work.get("type_crossref"),
                         work_keys=list(work.keys()))
        
        # Извлекаем DOI - проверяем разные возможные поля
        doi_value = work.get("doi") or ids.get("doi") or work.get("DOI")
        if doi_value and doi_value.startswith("https://doi.org/"):
            doi_value = doi_value.replace("https://doi.org/", "")
        
        # Обрабатываем title - используем display_name как fallback
        title = work.get("title") or work.get("display_name")
        
        # Обрабатываем publication_year - проверяем разные поля
        pub_year = work.get("publication_year") or work.get("year")
        if pub_year is not None:
            try:
                pub_year = int(pub_year)
            except (ValueError, TypeError):
                pub_year = None
        
        # Если publication_year не найден, попробуем извлечь из published-print
        if pub_year is None and "published-print" in work:
            published_print = work["published-print"]
            if "date-parts" in published_print and published_print["date-parts"]:
                try:
                    pub_year = int(published_print["date-parts"][0][0])
                except (ValueError, TypeError, IndexError):
                    pub_year = None
        
        # Извлекаем type_crossref - проверяем разные возможные поля
        type_crossref = work.get("type_crossref")
        if type_crossref is None:
            # Попробуем найти в других местах
            if "biblio" in work and isinstance(work["biblio"], dict):
                type_crossref = work["biblio"].get("type_crossref")
            if type_crossref is None and "primary_location" in work:
                primary_location = work["primary_location"]
                if primary_location and "source" in primary_location:
                    source = primary_location["source"]
                    if source and "type_crossref" in source:
                        type_crossref = source["type_crossref"]
        
        record: dict[str, Any | None] = {
            "source": "openalex",
            "openalex_doi_key": doi_value,
            "openalex_title": title,
            "openalex_doc_type": work.get("type"),
            "openalex_type_crossref": type_crossref,
            "openalex_publication_year": pub_year,
            "openalex_error": None,  # Will be set if there's an error
        }
        
        # Debug logging for the final record
        self.logger.debug("openalex_parsed_record", 
                         openalex_type_crossref=type_crossref,
                         openalex_doc_type=work.get("type"))
        
        # Return all fields, including None values, to maintain schema consistency
        return record

    def _create_empty_record(self, identifier: str, error_msg: str) -> dict[str, Any]:
        """Создает пустую запись для случая ошибки."""
        return {
            "source": "openalex",
            "openalex_doi_key": None,
            "openalex_title": None,
            "openalex_doc_type": None,
            "openalex_type_crossref": None,
            "openalex_publication_year": None,
            "openalex_error": error_msg,
        }
