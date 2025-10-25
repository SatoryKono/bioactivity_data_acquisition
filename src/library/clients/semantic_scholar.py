"""Client for the Semantic Scholar API."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from typing import Any

from library.clients.base import BaseApiClient
<<<<<<< Updated upstream
from library.config import APIClientConfig
=======
from library.settings import APIClientConfig
from library.utils.list_converter import convert_authors_list
>>>>>>> Stashed changes

logger = logging.getLogger(__name__)


class SemanticScholarClient(BaseApiClient):
    """HTTP client for Semantic Scholar."""

    # Минимальный набор полей для оптимизации производительности
    _DEFAULT_FIELDS = [
        "title",      
        "externalIds",
        "year",
        "authors",
        "publicationVenue",  # Для получения ISSN и названия журнала
        "publicationTypes",  # Для получения типа документа
<<<<<<< Updated upstream
=======
        "externalIds",  # Для получения DOI и других ID
        "paperId",  # Semantic Scholar ID (как в референсе)
        "venue",  # Для получения данных о журнале (как в референсе)
        "title",  # Дополнительно для полноты
        "year",  # Дополнительно для полноты
        "authors",  # Дополнительно для полноты
>>>>>>> Stashed changes
    ]

    # Альтернативный минимальный набор для максимальной производительности
    _MINIMAL_FIELDS = ["title", "externalIds", "year", "authors"]

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        headers = dict(config.headers)
        headers.setdefault("Accept", "application/json")
        enhanced = config.model_copy(update={"headers": headers})
<<<<<<< Updated upstream
        
        # Создаем специальную fallback стратегию для Semantic Scholar
        from library.clients.fallback import (
            FallbackConfig,
            FallbackManager,
            SemanticScholarFallbackStrategy,
        )
        fallback_config = FallbackConfig(
            max_retries=1,  # Минимальное количество попыток для SemanticScholar
            base_delay=10.0,  # Базовая задержка 10 секунд
            max_delay=30.0,  # Максимальная задержка 30 секунд
            backoff_multiplier=1.5,  # Меньший множитель
            jitter=True
        )
        fallback_strategy = SemanticScholarFallbackStrategy(fallback_config)
        fallback_manager = FallbackManager(fallback_strategy)
        
        super().__init__(enhanced, fallback_manager=fallback_manager, **kwargs)

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any]:
        identifier = f"PMID:{pmid}"
        
=======

        # Fallback strategy removed - using standard request handling

        super().__init__(enhanced, **kwargs)
        self.logger = logger

    def fetch_by_pmid(self, pmid: str, title: str | None = None) -> dict[str, Any]:
        # Semantic Scholar API endpoint for papers by PMID
        identifier = f"paper/PMID:{pmid}"

>>>>>>> Stashed changes
        try:
            # Use standard request method
            response = self._request("GET", identifier, params={"fields": ",".join(self._DEFAULT_FIELDS)})
            payload = response.json()

            # Check if we got fallback data
            if payload.get("source") == "fallback" or payload.get("fallback_used"):
                # Экранируем символы % в сообщениях об ошибках для безопасного логирования
                error_msg = str(payload.get("error", "")).replace("%", "%%")
                fallback_reason_msg = str(payload.get("fallback_reason", "")).replace("%", "%%")
<<<<<<< Updated upstream
                self.logger.warning(
                    "semantic_scholar_fallback_used",
                    pmid=pmid,
                    error=error_msg,
                    fallback_reason=fallback_reason_msg
                )
                return self._create_empty_record(pmid, str(payload.get("error", "Unknown error")).replace('%', '%%'))
                
=======
                self.logger.warning(f"semantic_scholar_fallback_used pmid={pmid} error={error_msg} fallback_reason={fallback_reason_msg}")
                # Fallback к поиску по заголовку если есть
                if title:
                    self.logger.info(f"semantic_scholar_title_fallback pmid={pmid} title={title}")
                    return self._search_by_title(title, pmid)
                return self._create_empty_record(pmid, str(payload.get("error", "Unknown error")).replace("%", "%%"))

>>>>>>> Stashed changes
            return self._parse_paper(payload)

        except Exception as exc:
            # Специальная обработка для rate limiting ошибок
            if "429" in str(exc) or "rate limited" in str(exc).lower():
                # Экранируем символы % в сообщениях об ошибках для безопасного логирования
                error_msg = str(exc).replace("%", "%%")
                self.logger.warning(
                    f"semantic_scholar_rate_limited pmid={pmid} error={error_msg} message=Rate limited by Semantic Scholar API. Consider getting an API key for higher limits."
                )
                exc_msg = str(exc).replace("%", "%%")
                return self._create_empty_record(pmid, f"Rate limited: {exc_msg}. Consider getting an API key for higher limits.")
            else:
<<<<<<< Updated upstream
=======
                # Fallback к поиску по заголовку если есть
                if title:
                    self.logger.info(f"semantic_scholar_pmid_fallback pmid={pmid} title={title}")
                    return self._search_by_title(title, pmid)

>>>>>>> Stashed changes
                # Экранируем символы % в сообщениях об ошибках для безопасного логирования
                error_msg = str(exc).replace("%", "%%")
                self.logger.error(f"semantic_scholar_request_failed pmid={pmid} error={error_msg} error_type={type(exc).__name__}")
                exc_msg = str(exc).replace("%", "%%")
                return self._create_empty_record(pmid, f"Request failed: {exc_msg}")

<<<<<<< Updated upstream
    def fetch_by_pmids(self, pmids: Iterable[str]) -> dict[str, dict[str, Any]]:
        ids = [f"PMID:{pmid}" for pmid in pmids]
        if not ids:
            return {}
        
        try:
            payload = self._request_with_fallback(
                "POST",
                "batch",
                json={"ids": ids, "fields": ",".join(self._DEFAULT_FIELDS)},
            )
        except Exception as exc:
            # Экранируем символы % в сообщениях об ошибках для безопасного логирования
            error_msg = str(exc).replace("%", "%%")
            self.logger.error(f"Batch request failed: {error_msg}")
            # Возвращаем пустые записи для всех PMID
            result: dict[str, dict[str, Any]] = {}
            for pmid in pmids:
                exc_msg = str(exc).replace('%', '%%')
                result[pmid] = self._create_empty_record(pmid, exc_msg)
            return result
        
        papers = payload.get("data") or payload.get("papers") or []
        result: dict[str, dict[str, Any]] = {}
        for paper in papers:
            pmid_value = self._extract_pmid(paper)
            if pmid_value:
                result[pmid_value] = self._parse_paper(paper)
        
        # Добавляем пустые записи для PMID, которые не были найдены
        for pmid in pmids:
            if pmid not in result:
                result[pmid] = self._create_empty_record(pmid, "Paper not found")
        
        return result

=======
    def _search_by_title(self, title: str, pmid: str) -> dict[str, Any]:
        """Search for paper by title as fallback when PMID lookup fails."""
        try:
            # Очищаем заголовок от HTML тегов и лишних символов
            clean_title = self._clean_title_for_search(title)
            if not clean_title:
                return self._create_empty_record(pmid, "Empty title for search")

            # Используем поиск по заголовку
            search_params = {
                "query": clean_title,
                "fields": ",".join(self._DEFAULT_FIELDS),
                "limit": 5,  # Ограничиваем результаты
            }

            response = self._request("GET", "paper/search", params=search_params)
            payload = response.json()

            # Проверяем результаты поиска
            if isinstance(payload, dict) and "data" in payload:
                papers = payload.get("data", [])
                if papers:
                    # Берем первый результат (наиболее релевантный)
                    best_match = papers[0]
                    self.logger.info(f"semantic_scholar_title_search_success pmid={pmid} title={clean_title} found_paper_id={best_match.get('paperId', 'unknown')}")
                    return self._parse_paper(best_match)

            # Если поиск не дал результатов
            self.logger.warning(f"semantic_scholar_title_search_no_results pmid={pmid} title={clean_title}")
            return self._create_empty_record(pmid, f"Not found by title search: {clean_title}")

        except Exception as exc:
            self.logger.error(f"semantic_scholar_title_search_failed pmid={pmid} title={title} error={str(exc)}")
            return self._create_empty_record(pmid, f"Title search failed: {str(exc)}")

    def _clean_title_for_search(self, title: str) -> str:
        """Clean title for search by removing HTML tags and special characters."""
        if not title:
            return ""

        import re

        # Удаляем HTML теги
        clean = re.sub(r"<[^>]+>", "", title)

        # Удаляем лишние пробелы
        clean = re.sub(r"\s+", " ", clean).strip()

        # Ограничиваем длину для поиска (Semantic Scholar имеет лимиты)
        if len(clean) > 200:
            clean = clean[:200]

        return clean

    def fetch_by_pmids(self, pmids: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Fetch multiple papers by PMIDs using individual requests.

        Semantic Scholar API doesn't have a batch endpoint, so we use individual requests.
        """
        pmid_list = list(pmids)
        if not pmid_list:
            return {}

        result: dict[str, dict[str, Any]] = {}

        # Process each PMID individually
        for pmid in pmid_list:
            try:
                # Use the individual fetch method
                paper_data = self.fetch_by_pmid(pmid)
                result[pmid] = paper_data
            except Exception as exc:
                # Экранируем символы % в сообщениях об ошибках для безопасного логирования
                error_msg = str(exc).replace("%", "%%")
                self.logger.warning("Failed to fetch PMID %s: %s", pmid, error_msg)
                result[pmid] = self._create_empty_record(pmid, str(exc))

        return result

>>>>>>> Stashed changes
    def _parse_paper(self, payload: dict[str, Any]) -> dict[str, Any]:
        external_ids = payload.get("externalIds") or {}
        authors = payload.get("authors")
        if isinstance(authors, list) and authors:
            author_names = [author.get("name") for author in authors if isinstance(author, dict) and author.get("name")]
            # Убираем None значения и пустые строки
            author_names = [name for name in author_names if name and name.strip()]
            author_names = author_names if author_names else None
        else:
            author_names = None

        record: dict[str, Any | None] = {
            "source": "semantic_scholar",
<<<<<<< Updated upstream
            "semantic_scholar_pmid": self._extract_pmid(payload),
            "semantic_scholar_doi": external_ids.get("DOI"),
            "semantic_scholar_semantic_scholar_id": payload.get("paperId"),
            "semantic_scholar_title": payload.get("title"),
            "semantic_scholar_doc_type": self._extract_doc_type(payload),
            "semantic_scholar_journal": self._extract_journal(payload),
            "semantic_scholar_external_ids": (
                json.dumps(external_ids) if external_ids else None
            ),
            
            "semantic_scholar_issn": self._extract_issn(payload),
            "semantic_scholar_authors": author_names,
            "semantic_scholar_error": None,  # Will be set if there's an error
            # Legacy fields for backward compatibility
            "title": payload.get("title"),
            
            "year": payload.get("year"),
            "pubmed_authors": author_names,
=======
            "semantic_scholar_pmid": self._extract_pmid(payload) or "",
            "semantic_scholar_doi": external_ids.get("DOI") or "",
            "semantic_scholar_semantic_scholar_id": payload.get("paperId") or "",
            "semantic_scholar_title": payload.get("title") or "",
            "semantic_scholar_doc_type": self._extract_doc_type(payload) or "",
            "semantic_scholar_journal": self._extract_journal(payload) or "",
            "semantic_scholar_external_ids": (json.dumps(external_ids) if external_ids else ""),
            "semantic_scholar_issn": self._extract_issn(payload) or "",
            "semantic_scholar_authors": convert_authors_list(author_names) or "",
            "semantic_scholar_year": payload.get("year") or "",
            "semantic_scholar_error": "",  # Will be set if there's an error
            # Legacy fields for backward compatibility
            "title": payload.get("title") or "",
            "year": payload.get("year") or "",
            "pubmed_authors": convert_authors_list(author_names) or "",
>>>>>>> Stashed changes
        }
        # Return all fields, including None values, to maintain schema consistency
        return record

    def _extract_issn(self, payload: dict[str, Any]) -> str | None:
        """Извлекает ISSN из Semantic Scholar payload."""
<<<<<<< Updated upstream
        # Сначала проверяем в publicationVenue
=======
        # Сначала проверяем в venue (как в референсе)
        venue = payload.get("venue", {})
        if isinstance(venue, dict):
            issn = venue.get("issn")
            if issn:
                return str(issn)

        # Fallback к publicationVenue
>>>>>>> Stashed changes
        publication_venue = payload.get("publicationVenue", {})
        if isinstance(publication_venue, dict):
            issn = publication_venue.get("issn")
            if issn:
                return str(issn)

        # Затем проверяем в externalIds
        external_ids = payload.get("externalIds", {})
        issn = external_ids.get("issn")
        if issn:
            return str(issn)

        return None

    def _extract_journal(self, payload: dict[str, Any]) -> str | None:
        """Извлекает название журнала из Semantic Scholar payload."""
<<<<<<< Updated upstream
        # Проверяем в publicationVenue
=======
        # Сначала проверяем в venue (как в референсе)
        venue = payload.get("venue", {})
        if isinstance(venue, dict):
            # Пробуем разные поля для названия журнала
            journal = venue.get("name") or venue.get("alternateName") or venue.get("displayName")
            if journal:
                return str(journal)

        # Fallback к publicationVenue
>>>>>>> Stashed changes
        publication_venue = payload.get("publicationVenue", {})
        if isinstance(publication_venue, dict):
            # Пробуем разные поля для названия журнала
            journal = publication_venue.get("name") or publication_venue.get("alternateName") or publication_venue.get("displayName")
            if journal:
                return str(journal)

        return None

    def _extract_doc_type(self, payload: dict[str, Any]) -> str | None:
        """Извлекает тип документа из Semantic Scholar payload."""
        # Проверяем в publicationTypes
        publication_types = payload.get("publicationTypes", [])
        if isinstance(publication_types, list) and publication_types:
            # Берем первый тип документа
            doc_type = publication_types[0]
            if isinstance(doc_type, str):
                return doc_type
            elif isinstance(doc_type, dict):
                # Если это объект, берем поле name или type
                return doc_type.get("name") or doc_type.get("type")

        return None

    def _create_empty_record(self, pmid: str, error_msg: str) -> dict[str, Any]:
        """Создает пустую запись для случая ошибки."""
        return {
            "source": "semantic_scholar",
            "semantic_scholar_pmid": pmid if pmid else None,
            "semantic_scholar_doi": None,
            "semantic_scholar_semantic_scholar_id": None,
            "semantic_scholar_title": None,
            "semantic_scholar_doc_type": None,
            "semantic_scholar_journal": None,
            "semantic_scholar_external_ids": None,
            "semantic_scholar_issn": None,
            "semantic_scholar_authors": None,
            "semantic_scholar_error": error_msg,
            # Legacy fields
            "title": None,
         
            "year": None,
            "pubmed_authors": None,
        }

    def _extract_pmid(self, payload: dict[str, Any]) -> str | None:
        external_ids = payload.get("externalIds") or {}
        pmid = external_ids.get("PubMed") or external_ids.get("PMID")
        if isinstance(pmid, int | float):
            return str(int(pmid))
        if isinstance(pmid, str):
            return pmid.split(":")[-1]
        return None

<<<<<<< Updated upstream
    def fetch_by_pmids_batch(
        self,
        pmids: list[str],
        batch_size: int = 500
    ) -> dict[str, dict[str, Any]]:
        """Fetch multiple papers by PMIDs using batch endpoint.
        
        Semantic Scholar supports POST /paper/batch with up to 500 IDs.
        """
        if not pmids:
            return {}
        
        results = {}
        
        # Process in chunks to respect API limits
        for i in range(0, len(pmids), batch_size):
            chunk = pmids[i:i + batch_size]
            
            try:
                # Prepare batch request payload
                identifiers = [f"PMID:{pmid}" for pmid in chunk]
                batch_payload = {"ids": identifiers}
                
                # Make batch request
                payload = self._request("POST", "batch", json=batch_payload)
                
                # Parse batch results
                papers = payload.get("results", [])
                
                for paper in papers:
                    if paper:  # Skip None results
                        parsed_paper = self._parse_paper(paper)
                        pmid = self._extract_pmid(paper)
                        if pmid:
                            results[pmid] = parsed_paper
                
                # Add empty records for missing PMIDs
                for pmid in chunk:
                    if pmid not in results:
                        results[pmid] = self._create_empty_record(pmid, "Not found in batch response")
                        
            except Exception as e:
                logger.warning(f"Failed to fetch PMIDs batch {chunk}: {e}")
                # Add empty records for failed batch
                for pmid in chunk:
                    results[pmid] = self._create_empty_record(pmid, str(e))
        
        return results
=======
    def fetch_by_pmids_batch(self, pmids: list[str], batch_size: int = 100) -> dict[str, dict[str, Any]]:
        """Fetch multiple PMIDs in batches.

        Semantic Scholar API doesn't have a working batch endpoint, so we use individual requests.
        """
        if not pmids:
            return {}

        result: dict[str, dict[str, Any]] = {}

        # Обрабатываем PMID батчами
        for i in range(0, len(pmids), batch_size):
            batch = pmids[i : i + batch_size]
            try:
                self.logger.debug(f"Processing Semantic Scholar batch {i // batch_size + 1} with {len(batch)} PMIDs")
                batch_result = self.fetch_by_pmids(batch)
                result.update(batch_result)
            except Exception as e:
                self.logger.error(f"Failed to process Semantic Scholar batch {i // batch_size + 1}: {e}")
                # Добавляем ошибки для всех PMID в батче
                for pmid in batch:
                    result[pmid] = self._create_empty_record(pmid, f"Batch processing failed: {str(e)}")

        return result
>>>>>>> Stashed changes
