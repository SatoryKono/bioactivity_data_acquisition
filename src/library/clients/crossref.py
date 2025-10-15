"""Client for the Crossref API."""
from __future__ import annotations

from typing import Any
from urllib.parse import quote

from library.clients.base import ApiClientError, BaseApiClient
from library.config import APIClientConfig


class CrossrefClient(BaseApiClient):
    """HTTP client for Crossref works."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def fetch_by_doi(self, doi: str) -> dict[str, Any]:
        """Fetch Crossref work by DOI with fallback search."""

        encoded = quote(doi, safe="")
        try:
            payload = self._request("GET", encoded)
            message = payload.get("message", payload)
            return self._parse_work(message)
        except ApiClientError as exc:
            self.logger.info("fallback_to_search", doi=doi, error=str(exc))
            payload = self._request("GET", "", params={"query.bibliographic": doi})
            items = payload.get("message", {}).get("items", [])
            if not items:
                raise
            return self._parse_work(items[0])

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any]:
        """Fetch Crossref work by PubMed identifier with fallback query."""

        try:
            payload = self._request("GET", "", params={"filter": f"pmid:{pmid}"})
        except ApiClientError as exc:
            self.logger.info("pmid_lookup_failed", pmid=pmid, error=str(exc))
            payload = {"message": {"items": []}}
        items = payload.get("message", {}).get("items", [])
        if items:
            return self._parse_work(items[0])

        self.logger.info("pmid_fallback_to_query", pmid=pmid)
        payload = self._request("GET", "", params={"query": pmid})
        items = payload.get("message", {}).get("items", [])
        if not items:
            raise ApiClientError(f"No Crossref work found for PMID {pmid}")
        return self._parse_work(items[0])

    def _parse_work(self, work: dict[str, Any]) -> dict[str, Any]:
        # Обрабатываем subject - если это пустой список, возвращаем None
        subject = work.get("subject")
        if isinstance(subject, list) and not subject:
            subject = None
        elif isinstance(subject, list) and subject:
            # Если есть subjects, объединяем их в строку
            subject = "; ".join(str(s) for s in subject if s)
        
        # Если subject не найден, пытаемся вывести из названия журнала
        if not subject:
            journal = work.get("container-title", [])
            if isinstance(journal, list) and journal:
                journal_name = journal[0].lower()
                if 'medicinal chemistry' in journal_name or 'j med chem' in journal_name:
                    subject = "Medicinal Chemistry"
                elif 'pharmacology' in journal_name:
                    subject = "Pharmacology"
                elif 'biochemistry' in journal_name:
                    subject = "Biochemistry"
                elif 'organic chemistry' in journal_name:
                    subject = "Organic Chemistry"
                elif 'drug' in journal_name:
                    subject = "Drug Discovery"
                elif 'therapeutic' in journal_name:
                    subject = "Therapeutics"
                elif 'molecular' in journal_name:
                    subject = "Molecular Biology"
                elif 'cell' in journal_name:
                    subject = "Cell Biology"
                elif 'nature' in journal_name:
                    subject = "General Science"
                elif 'science' in journal_name:
                    subject = "General Science"
        
        # Обрабатываем title - извлекаем первый элемент если это список
        title = work.get("title")
        if isinstance(title, list) and title:
            title = title[0]
        
        record: dict[str, Any | None] = {
            "source": "crossref",
            "doi_key": work.get("DOI"),
            "crossref_title": title,
            "crossref_doc_type": work.get("type"),
            "crossref_subject": subject,
            "crossref_error": None,  # Will be set if there's an error
        }
        # Return all fields, including None values, to maintain schema consistency
        return record