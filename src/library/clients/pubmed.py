"""Client for the PubMed API."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from library.clients.base import ApiClientError, BaseApiClient
from library.config import APIClientConfig


class PubMedClient(BaseApiClient):
    """HTTP client for PubMed E-utilities."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        headers = dict(config.headers)
        headers.setdefault("Accept", "application/json")
        enhanced = config.model_copy(update={"headers": headers})
        super().__init__(enhanced, **kwargs)

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any]:
        # Используем esummary.fcgi для получения метаданных статьи
        params = {
            "db": "pubmed",
            "id": pmid,
            "retmode": "json"
        }
        
        # Добавляем API ключ если он настроен
        api_key = getattr(self.config, 'api_key', None)
        if api_key:
            params["api_key"] = api_key
            
        payload = self._request("GET", "esummary.fcgi", params=params)
        record = self._extract_record(payload, pmid)
        if record is None:
            raise ApiClientError(f"No PubMed record found for PMID {pmid}")
        return record

    def fetch_by_pmids(self, pmids: Iterable[str]) -> dict[str, dict[str, Any]]:
        pmid_list = list(pmids)
        if not pmid_list:
            return {}
        
        # NCBI E-utilities поддерживает множественные ID через запятую
        ids_param = ",".join(pmid_list)
        params = {
            "db": "pubmed",
            "id": ids_param,
            "retmode": "json"
        }
        
        # Добавляем API ключ если он настроен
        api_key = getattr(self.config, 'api_key', None)
        if api_key:
            params["api_key"] = api_key
            
        payload = self._request("GET", "esummary.fcgi", params=params)
        
        result: dict[str, dict[str, Any]] = {}
        for pmid in pmid_list:
            record = self._extract_record(payload, pmid)
            if record is not None:
                result[str(pmid)] = record
        return result

    def _extract_record(self, payload: dict[str, Any], pmid: str) -> dict[str, Any] | None:
        # Проверяем наличие ошибок в ответе
        if "error" in payload:
            error_msg = payload.get("error", "Unknown error")
            self.logger.warning("pubmed_api_error", pmid=pmid, error=error_msg)
            return None
            
        # NCBI E-utilities возвращает данные в формате {"result": {"uids": [...], "pmid": {...}}}
        if "result" in payload and isinstance(payload["result"], dict):
            result = payload["result"]
            
            # Проверяем, есть ли запрашиваемый PMID в списке UIDs
            uids = result.get("uids", [])
            if str(pmid) not in uids:
                self.logger.warning("pmid_not_found", pmid=pmid, available_uids=uids)
                return None
                
            # Получаем данные для конкретного PMID
            data = result.get(pmid)
            if data is not None:
                return self._normalise_record(data)
            else:
                self.logger.warning("no_data_for_pmid", pmid=pmid)
                return None

        # Fallback для других форматов (если API изменится)
        if "records" in payload and isinstance(payload["records"], list):
            for item in payload["records"]:
                if str(item.get("pmid")) == str(pmid):
                    return self._normalise_record(item)

        if payload.get("pmid") and str(payload.get("pmid")) == str(pmid):
            return self._normalise_record(payload)
            
        self.logger.warning("unexpected_payload_format", pmid=pmid, payload_keys=list(payload.keys()))
        return None

    def _normalise_record(self, record: dict[str, Any]) -> dict[str, Any]:
        # Обработка авторов из NCBI E-utilities
        authors = record.get("authors") or record.get("authorList")
        if isinstance(authors, dict):
            authors = authors.get("authors")
        if isinstance(authors, list):
            formatted_authors = [self._format_author(author) for author in authors]
        else:
            formatted_authors = None

        # Обработка DOI
        doi_value: str | None
        doi_list = record.get("doiList")
        if isinstance(doi_list, list) and doi_list:
            doi_value = doi_list[0]
        else:
            doi_value = record.get("doi")

        # Обработка дат публикации
        pub_date = record.get("pubdate")
        if isinstance(pub_date, str):
            # Парсим дату в формате "2023 Dec 15" или "2023"
            try:
                from datetime import datetime
                if len(pub_date.split()) >= 3:
                    parsed_date = datetime.strptime(pub_date, "%Y %b %d")
                elif len(pub_date.split()) == 2:
                    parsed_date = datetime.strptime(pub_date, "%Y %b")
                else:
                    parsed_date = datetime.strptime(pub_date, "%Y")
                pub_year = parsed_date.year
                pub_month = parsed_date.month
                pub_day = parsed_date.day
            except (ValueError, AttributeError):
                pub_year = pub_month = pub_day = None
        else:
            pub_year = record.get("pubdate") or record.get("year")
            pub_month = record.get("month")
            pub_day = record.get("day")

        parsed: dict[str, Any | None] = {
            "source": "pubmed",
            "pubmed_pmid": record.get("uid") or record.get("pmid") or record.get("PMID"),
            "pubmed_doi": doi_value,
            "pubmed_article_title": record.get("title") or record.get("articleTitle"),
            "pubmed_abstract": record.get("abstract"),
            "pubmed_journal_title": record.get("source") or record.get("journalTitle"),
            "pubmed_volume": record.get("volume"),
            "pubmed_issue": record.get("issue"),
            "pubmed_start_page": record.get("pages", "").split("-")[0] if record.get("pages") else None,
            "pubmed_end_page": record.get("pages", "").split("-")[1] if record.get("pages") and "-" in record.get("pages", "") else None,
            "pubmed_publication_type": record.get("pubtype"),
            "pubmed_mesh_descriptors": record.get("meshdescriptors"),
            "pubmed_mesh_qualifiers": record.get("meshqualifiers"),
            "pubmed_chemical_list": record.get("chemicals"),
            "pubmed_year_completed": pub_year,
            "pubmed_month_completed": pub_month,
            "pubmed_day_completed": pub_day,
            "pubmed_year_revised": record.get("lastauthor"),
            "pubmed_month_revised": record.get("lastauthor"),
            "pubmed_day_revised": record.get("lastauthor"),
            "pubmed_issn": record.get("issn"),
            "pubmed_error": None,  # Will be set if there's an error
            # Legacy fields for backward compatibility
            "title": record.get("title") or record.get("articleTitle"),
            "abstract": record.get("abstract"),
            "doi": doi_value,
            "authors": formatted_authors,
        }
        # Return all fields, including None values, to maintain schema consistency
        return parsed

    def _format_author(self, author: Any) -> str:
        if isinstance(author, str):
            return author
        if isinstance(author, dict):
            last = author.get("lastName") or author.get("lastname")
            fore = author.get("foreName") or author.get("forename")
            if last and fore:
                return f"{fore} {last}"
            return author.get("name") or " ".join(filter(None, [fore, last]))
        return str(author)