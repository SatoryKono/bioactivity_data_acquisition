"""Client for the Crossref API."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote

from library.clients.base import ApiClientError, BaseApiClient
from library.config import APIClientConfig

logger = logging.getLogger(__name__)


class CrossrefClient(BaseApiClient):
    """HTTP client for Crossref works."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        # Добавляем email в User-Agent для polite pool согласно документации Crossref
        headers = dict(config.headers)

        # Используем реальный email из конфигурации
        email = getattr(config, "mailto", None) or "821311@gmail.com"
        if "User-Agent" not in headers:
            headers["User-Agent"] = f"bioactivity-data-acquisition/0.1.0 (mailto:{email})"

        enhanced = config.model_copy(update={"headers": headers})
        super().__init__(enhanced, **kwargs)
        self.logger = logger

    def fetch_by_doi(self, doi: str) -> dict[str, Any]:
        """Fetch Crossref work by DOI with fallback search."""

        encoded = quote(doi, safe="")
        
        # Добавляем mailto параметр для доступа к polite pool
        mailto_param = ""
        if hasattr(self.config, "mailto") and self.config.mailto:
            mailto_param = f"?mailto={quote(self.config.mailto)}"

        try:
            # Crossref API requires /works/ prefix for DOI lookups
            response = self._request("GET", f"works/{encoded}{mailto_param}")
            payload = response.json()
            message = payload.get("message", payload)
            return self._parse_work(message)
        except ApiClientError as exc:
            # Try graceful degradation first
            if self.degradation_manager.should_degrade(self.config.name, exc):
                self.logger.warning(
                    f"Crossref API failed, using graceful degradation: {exc}"
                )
                return self.degradation_manager.get_fallback_data(
                    self.config.name,
                    {"doi": doi},
                    exc
                )
            
            # Try fallback search
            self.logger.info("fallback_to_search doi=%s error=%s", doi, str(exc))
            try:
                # Crossref search endpoint requires /works prefix
                response = self._request("GET", "works", params={"query.bibliographic": doi})
                payload = response.json()
                items = payload.get("message", {}).get("items", [])
                if not items:
                    raise ApiClientError(f"No Crossref work found for DOI {doi}")
                return self._parse_work(items[0])
            except ApiClientError:
                # If fallback also fails, raise the exception
                raise

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any]:
        """Fetch Crossref work by PubMed identifier with fallback query."""

        # Добавляем mailto параметр
        mailto_param = ""
        if hasattr(self.config, "mailto") and self.config.mailto:
            mailto_param = f"&mailto={quote(self.config.mailto)}"

        try:
            response = self._request("GET", "works", params={"filter": f"pmid:{pmid}{mailto_param}"})
            payload = response.json()
        except ApiClientError as exc:
            # Try graceful degradation first
            if self.degradation_manager.should_degrade(self.config.name, exc):
                self.logger.warning(
                    f"Crossref PMID lookup failed, using graceful degradation: {exc}"
                )
                return self.degradation_manager.get_fallback_data(
                    self.config.name,
                    {"pmid": pmid},
                    exc
                )
            
            self.logger.info("pmid_lookup_failed pmid=%s error=%s", pmid, str(exc))
            payload = {"message": {"items": []}}

        items = payload.get("message", {}).get("items", [])
        if items:
            return self._parse_work(items[0])

        self.logger.info("pmid_fallback_to_query", pmid=pmid)
        try:
            response = self._request("GET", "works", params={"query": pmid})
            payload = response.json()
            items = payload.get("message", {}).get("items", [])
            if not items:
                raise ApiClientError(f"No Crossref work found for PMID {pmid}")
            return self._parse_work(items[0])
        except ApiClientError:
            # If fallback also fails, raise the exception
            raise

    def _parse_work(self, work: dict[str, Any]) -> dict[str, Any]:
        self.logger.debug(f"crossref_parse_start work_keys={list(work.keys())}")
        
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
                if "medicinal chemistry" in journal_name or "j med chem" in journal_name:
                    subject = "Medicinal Chemistry"
                elif "pharmacology" in journal_name:
                    subject = "Pharmacology"
                elif "biochemistry" in journal_name:
                    subject = "Biochemistry"
                elif "organic chemistry" in journal_name:
                    subject = "Organic Chemistry"
                elif "drug" in journal_name:
                    subject = "Drug Discovery"
                elif "therapeutic" in journal_name:
                    subject = "Therapeutics"
                elif "molecular" in journal_name:
                    subject = "Molecular Biology"
                elif "cell" in journal_name:
                    subject = "Cell Biology"
                elif "nature" in journal_name:
                    subject = "General Science"
                elif "science" in journal_name:
                    subject = "General Science"

        # Обрабатываем title - извлекаем первый элемент если это список
        title = work.get("title")
        if isinstance(title, list) and title:
            title = title[0]
        

        # Извлекаем дополнительные библиографические данные
        work.get("published-print") or work.get("published-online")

        # Извлекаем volume, issue, page
        work.get("page")

        # Извлекаем journal из container-title
        journal_name = self._extract_journal(work)
        
        # Извлекаем ключевые поля с логированием
        pmid = self._extract_pmid(work)
        abstract = work.get("abstract")
        issn = self._extract_issn(work)
        
        self.logger.info(f"crossref_fields_extracted pmid={pmid} abstract_present={abstract is not None} issn={issn} subject={subject} journal={journal_name}")

        record: dict[str, Any | None] = {
            "source": "crossref",
            "doi_key": work.get("DOI"),
            "crossref_doi": work.get("DOI"),
            "crossref_title": title,
            "crossref_doc_type": work.get("type"),
            "crossref_subject": subject,
            "crossref_pmid": pmid,
            "crossref_abstract": abstract,
            "crossref_issn": issn,
            "crossref_authors": self._extract_authors(work),
            "crossref_journal": journal_name,
            "crossref_year": self._extract_year(work),
            "crossref_volume": work.get("volume"),
            "crossref_issue": work.get("issue"),
            "crossref_first_page": self._extract_first_page(work),
            "crossref_last_page": self._extract_last_page(work),
            "crossref_error": None,  # Will be set if there's an error
        }
        
        self.logger.debug(f"crossref_parse_complete record_keys={list(record.keys())}")
        # Return all fields, including None values, to maintain schema consistency
        return record

    def _extract_pmid(self, work: dict[str, Any]) -> str | None:
        """Извлекает PMID из Crossref work."""
        # Crossref может содержать PMID в разных местах
        pmid = work.get("pmid")
        if pmid:
            return str(pmid)

        # Проверяем в link
        links = work.get("link", [])
        if isinstance(links, list):
            for link in links:
                if isinstance(link, dict) and link.get("intended-application") == "text-mining":
                    url = link.get("URL", "")
                    if "pubmed.ncbi.nlm.nih.gov" in url and "pmid=" in url:
                        # Извлекаем PMID из URL
                        import re

                        match = re.search(r"pmid=(\d+)", url)
                        if match:
                            return match.group(1)

        return None

    def _extract_authors(self, work: dict[str, Any]) -> list[str] | None:
        """Извлекает авторов из Crossref work."""
        authors = work.get("author")
        if isinstance(authors, list):
            author_names = []
            for author in authors:
                if isinstance(author, dict):
                    # Crossref обычно имеет структуру: {"given": "John", "family": "Doe"}
                    given = author.get("given", "")
                    family = author.get("family", "")
                    if given or family:
                        full_name = f"{given} {family}".strip()
                        author_names.append(full_name)
            return author_names if author_names else None

        return None

    def _extract_issn(self, work: dict[str, Any]) -> str | None:
        """Извлекает ISSN из Crossref work."""
        # Crossref может содержать ISSN в разных местах
        issn = work.get("issn")
        if issn:
            if isinstance(issn, list) and issn:
                return issn[0]  # Берем первый ISSN если их несколько
            return str(issn)

        # Проверяем в container-title
        container_title = work.get("container-title", [])
        if isinstance(container_title, list) and container_title:
            # В Crossref ISSN может быть в метаданных контейнера
            # Но обычно он находится в отдельном поле issn
            pass

        return None


    def _extract_journal(self, work: dict[str, Any]) -> str | None:
        """Extract journal name from Crossref work."""
        # Extract from container-title
        container_title = work.get("container-title", [])
        if isinstance(container_title, list) and container_title:
            journal = container_title[0]
            if journal:
                return str(journal)

        # Fallback на short-container-title
        short_container = work.get("short-container-title", [])
        if isinstance(short_container, list) and short_container:
            journal = short_container[0]
            if journal:
                return str(journal)

        return None

    def _extract_year(self, work: dict[str, Any]) -> int | None:
        """Извлекает год публикации из Crossref work."""
        # Проверяем published-print сначала
        published_print = work.get("published-print")
        if published_print and isinstance(published_print, dict):
            date_parts = published_print.get("date-parts")
            if date_parts and isinstance(date_parts, list) and date_parts[0]:
                try:
                    return int(date_parts[0][0])
                except (ValueError, TypeError, IndexError):
                    pass
        
        # Fallback на published-online
        published_online = work.get("published-online")
        if published_online and isinstance(published_online, dict):
            date_parts = published_online.get("date-parts")
            if date_parts and isinstance(date_parts, list) and date_parts[0]:
                try:
                    return int(date_parts[0][0])
                except (ValueError, TypeError, IndexError):
                    pass
        
        return None

    def _extract_first_page(self, work: dict[str, Any]) -> str | None:
        """Извлекает первую страницу из Crossref work."""
        page = work.get("page")
        if not page:
            return None
        
        # Crossref может возвращать страницы в формате "123-456" или просто "123"
        if isinstance(page, str):
            if "-" in page:
                return page.split("-")[0].strip()
            return page.strip()
        
        return None

    def _extract_last_page(self, work: dict[str, Any]) -> str | None:
        """Извлекает последнюю страницу из Crossref work."""
        page = work.get("page")
        if not page:
            return None
        
        # Crossref может возвращать страницы в формате "123-456" или просто "123"
        if isinstance(page, str) and "-" in page:
            parts = page.split("-")
            if len(parts) > 1:
                return parts[1].strip()
        
        return None

    def _create_empty_record(self, identifier: str, error_msg: str) -> dict[str, Any]:
        """Создает пустую запись для случая ошибки."""
        return {
            "source": "crossref",
            "doi_key": None,
            "crossref_doi": identifier if identifier.startswith("10.") else None,
            "crossref_pmid": identifier if not identifier.startswith("10.") else None,
            "crossref_title": None,
            "crossref_doc_type": None,
            "crossref_subject": None,
            "crossref_abstract": None,
            "crossref_issn": None,
            "crossref_authors": None,
            "crossref_journal": None,
            "crossref_year": None,
            "crossref_volume": None,
            "crossref_issue": None,
            "crossref_first_page": None,
            "crossref_last_page": None,
            "crossref_error": error_msg,
        }

    def fetch_by_dois_batch(self, dois: list[str], batch_size: int = 50) -> dict[str, dict[str, Any]]:
        """Fetch multiple DOIs in batches.

        Crossref doesn't have a true batch endpoint, so we use
        individual requests with controlled concurrency.
        """
        if not dois:
            return {}
        
        results = {}
        
        # Process in chunks to avoid overwhelming the API
        for i in range(0, len(dois), batch_size):
            chunk = dois[i:i + batch_size]
            
            for doi in chunk:
                try:
                    result = self.fetch_by_doi(doi)
                    results[doi] = result
                except Exception as e:
                    logger.warning(f"Failed to fetch DOI {doi} in batch: {e}")
                    results[doi] = {
                        "crossref_doi": doi,
                        "crossref_error": str(e),
                        "source": "crossref"
                    }
        
        return results

        result: dict[str, dict[str, Any]] = {}

        # Обрабатываем DOI батчами
        for i in range(0, len(dois), batch_size):
            batch = dois[i : i + batch_size]
            try:
                self.logger.debug(f"Processing Crossref batch {i // batch_size + 1} with {len(batch)} DOIs")
                batch_result = self.fetch_by_dois(batch)
                result.update(batch_result)
            except Exception as e:
                self.logger.error(f"Failed to process Crossref batch {i // batch_size + 1}: {e}")
                # Добавляем ошибки для всех DOI в батче
                for doi in batch:
                    result[doi] = self._create_empty_record(doi, f"Batch processing failed: {str(e)}")

        return result

    def fetch_by_pmids_batch(self, pmids: list[str], batch_size: int = 50) -> dict[str, dict[str, Any]]:
        """Fetch multiple works by PMIDs.

        Crossref doesn't have a true batch endpoint, so we use
        individual requests with controlled concurrency.
        """
        if not pmids:
            return {}

        results = {}

        # Process in chunks to avoid overwhelming the API
        for i in range(0, len(pmids), batch_size):
            chunk = pmids[i : i + batch_size]

            for pmid in chunk:
                try:
                    result = self.fetch_by_pmid(pmid)
                    results[pmid] = result
                except Exception as e:
                    logger.warning(f"Failed to fetch PMID {pmid} in batch: {e}")
                    results[pmid] = {
                        "crossref_pmid": pmid,
                        "crossref_error": str(e),
                        "source": "crossref"
                    }
        
        return results

    def fetch_by_dois(self, dois: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch multiple DOIs using individual requests."""
        result: dict[str, dict[str, Any]] = {}

        for doi in dois:
            try:
                record = self.fetch_by_doi(doi)
                result[doi] = record
            except Exception as e:
                self.logger.warning(f"Failed to fetch Crossref data for DOI {doi}: {e}")
                result[doi] = self._create_empty_record(doi, str(e))

        return result
