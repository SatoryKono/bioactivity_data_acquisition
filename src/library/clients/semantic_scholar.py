"""Client for the Semantic Scholar API."""
from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from library.clients.base import BaseApiClient
from library.config import APIClientConfig


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
    ]
    
    # Альтернативный минимальный набор для максимальной производительности
    _MINIMAL_FIELDS = [
        "title",
        "externalIds", 
        "year",
        "authors"
    ]

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        headers = dict(config.headers)
        headers.setdefault("Accept", "application/json")
        enhanced = config.model_copy(update={"headers": headers})
        
        # Создаем специальную fallback стратегию для Semantic Scholar
        # Учитываем строгий rate limit: 100 req/5min для анонимных
        from library.clients.fallback import (
            FallbackConfig,
            FallbackManager,
            SemanticScholarFallbackStrategy,
        )
        fallback_config = FallbackConfig(
            max_retries=2,  # Увеличиваем попытки для Semantic Scholar
            base_delay=15.0,  # Увеличиваем базовую задержку до 15 секунд
            max_delay=60.0,  # Увеличиваем максимальную задержку до 60 секунд
            backoff_multiplier=2.0,  # Увеличиваем множитель
            jitter=True
        )
        fallback_strategy = SemanticScholarFallbackStrategy(fallback_config)
        fallback_manager = FallbackManager(fallback_strategy)
        
        super().__init__(enhanced, fallback_manager=fallback_manager, **kwargs)

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any]:
        # Semantic Scholar API endpoint for papers by PMID
        identifier = f"paper/PMID:{pmid}"
        
        try:
            # Use fallback strategy for handling rate limiting and other errors
            payload = self._request_with_fallback(
                "GET", identifier, params={"fields": ",".join(self._DEFAULT_FIELDS)}
            )
            
            # Check if we got fallback data
            if payload.get("source") == "fallback" or payload.get("fallback_used"):
                # Экранируем символы % в сообщениях об ошибках для безопасного логирования
                error_msg = str(payload.get("error", "")).replace("%", "%%")
                fallback_reason_msg = str(payload.get("fallback_reason", "")).replace("%", "%%")
                self.logger.warning(
                    "semantic_scholar_fallback_used",
                    pmid=pmid,
                    error=error_msg,
                    fallback_reason=fallback_reason_msg
                )
                return self._create_empty_record(pmid, str(payload.get("error", "Unknown error")).replace('%', '%%'))
                
            return self._parse_paper(payload)
            
        except Exception as exc:
            # Специальная обработка для rate limiting ошибок
            if "429" in str(exc) or "rate limited" in str(exc).lower():
                # Экранируем символы % в сообщениях об ошибках для безопасного логирования
                error_msg = str(exc).replace("%", "%%")
                self.logger.warning(
                    "semantic_scholar_rate_limited",
                    pmid=pmid,
                    error=error_msg,
                    message=(
                        "Rate limited by Semantic Scholar API. "
                        "Consider getting an API key for higher limits."
                    )
                )
                exc_msg = str(exc).replace('%', '%%')
                return self._create_empty_record(
                    pmid, 
                    f"Rate limited: {exc_msg}. "
                    f"Consider getting an API key for higher limits."
                )
            else:
                # Экранируем символы % в сообщениях об ошибках для безопасного логирования
                error_msg = str(exc).replace("%", "%%")
                self.logger.error(
                    "semantic_scholar_request_failed",
                    pmid=pmid,
                    error=error_msg,
                    error_type=type(exc).__name__
                )
                exc_msg = str(exc).replace('%', '%%')
                return self._create_empty_record(pmid, f"Request failed: {exc_msg}")

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

    def _parse_paper(self, payload: dict[str, Any]) -> dict[str, Any]:
        external_ids = payload.get("externalIds") or {}
        authors = payload.get("authors")
        if isinstance(authors, list) and authors:
            author_names = [
                author.get("name") for author in authors 
                if isinstance(author, dict) and author.get("name")
            ]
            # Убираем None значения и пустые строки
            author_names = [name for name in author_names if name and name.strip()]
            author_names = author_names if author_names else None
        else:
            author_names = None

        record: dict[str, Any | None] = {
            "source": "semantic_scholar",
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
        }
        # Return all fields, including None values, to maintain schema consistency
        return record

    def _extract_issn(self, payload: dict[str, Any]) -> str | None:
        """Извлекает ISSN из Semantic Scholar payload."""
        # Сначала проверяем в publicationVenue
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
        # Проверяем в publicationVenue
        publication_venue = payload.get("publicationVenue", {})
        if isinstance(publication_venue, dict):
            # Пробуем разные поля для названия журнала
            journal = (
                publication_venue.get("name") or 
                publication_venue.get("alternateName") or
                publication_venue.get("displayName")
            )
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

    def fetch_by_pmids_batch(
        self,
        pmids: list[str],
        batch_size: int = 500
    ) -> dict[str, dict[str, Any]]:
        """Fetch multiple papers by PMIDs using individual requests.
        
        Semantic Scholar API doesn't have a working batch endpoint, so we use individual requests.
        """
        if not pmids:
            return {}
        
        # Use the individual fetch method for each PMID
        return self.fetch_by_pmids(pmids)