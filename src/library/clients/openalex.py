"""Client for the OpenAlex API."""

from __future__ import annotations

import logging
from typing import Any

from library.clients.base import ApiClientError, BaseApiClient
from library.config import APIClientConfig

logger = logging.getLogger(__name__)


class OpenAlexClient(BaseApiClient):
    """HTTP client for OpenAlex works."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        # Добавляем email в User-Agent для polite pool согласно документации OpenAlex
        headers = dict(config.headers)
        if "User-Agent" not in headers:
            headers["User-Agent"] = "bioactivity-data-acquisition/0.1.0 (mailto:your-email@example.com)"
        enhanced = config.model_copy(update={"headers": headers})
        
        # Создаем специализированную fallback стратегию для OpenAlex
        from library.clients.fallback import (
            FallbackConfig,
            FallbackManager,
            OpenAlexFallbackStrategy,
        )
        fallback_config = FallbackConfig(
            max_retries=1,  # Минимальное количество попыток для OpenAlex
            base_delay=5.0,  # Базовая задержка 5 секунд
            max_delay=30.0,  # Максимальная задержка 30 секунд
            backoff_multiplier=1.5,  # Меньший множитель
            jitter=True
        )
        fallback_strategy = OpenAlexFallbackStrategy(fallback_config)
        fallback_manager = FallbackManager(fallback_strategy)
        
        super().__init__(enhanced, fallback_manager=fallback_manager, **kwargs)
        self.logger = logger

    def fetch_by_doi(self, doi: str, title: str | None = None) -> dict[str, Any]:
        """Fetch a work by DOI with fallback to a filter query."""

        # Use OpenAlex API format: https://api.openalex.org/works/https://doi.org/{doi}
        path = f"https://api.openalex.org/works/https://doi.org/{doi}"
        self.logger.info(f"openalex_fetch_by_doi doi={doi} url={path}")
        try:
            response = self._request("GET", path)
            payload = response.json()
            self.logger.info(f"openalex_fetch_by_doi_success doi={doi} status={response.status_code}")
            return self._parse_work(payload)
        except ApiClientError as exc:
            # Специальная обработка для ошибок 429 от OpenAlex
            status_code = exc.context.details.get("status_code") if exc.context and exc.context.details else None
            if status_code == 429:
                self.logger.warning(f"openalex_rate_limited doi={doi} error={str(exc)} message=OpenAlex API rate limit exceeded. Consider getting an API key.")
                return self._create_empty_record(doi, f"Rate limited: {str(exc)}")
            
            self.logger.warning(f"openalex_doi_fallback doi={doi} error={str(exc)}")
            try:
                # Fallback to OpenAlex search API
                fallback_url = f"https://api.openalex.org/works?filter=doi:{doi}"
                self.logger.info(f"openalex_doi_fallback_filter doi={doi} url={fallback_url}")
                response = self._request("GET", "", params={"filter": f"doi:{doi}"})
                payload = response.json()
                results = payload.get("results", [])
                self.logger.info(f"openalex_doi_fallback_filter_result doi={doi} results_count={len(results)}")
                if not results:
                    raise
                return self._parse_work(results[0])
            except ApiClientError as fallback_exc:
                fallback_status_code = fallback_exc.context.details.get("status_code") if fallback_exc.context and fallback_exc.context.details else None
                if fallback_status_code == 429:
                    return self._create_empty_record(doi, f"Rate limited: {str(fallback_exc)}")
                
                # Fallback к поиску по заголовку если есть
                if title:
                    self.logger.info(f"openalex_doi_title_fallback doi={doi} title={title}")
                    return self._search_by_title(title, doi)
                
                raise

    def fetch_by_pmid(self, pmid: str, title: str | None = None) -> dict[str, Any]:
        """Fetch a work by PMID with fallback search."""

        try:
            # Используем прямой URL как в референсном проекте: /works/pmid:{pmid}
            path = f"works/pmid:{pmid}"
            full_url = f"https://api.openalex.org/{path}"
            self.logger.info(f"openalex_fetch_by_pmid pmid={pmid} url={full_url}")
            response = self._request("GET", path)
            payload = response.json()
            self.logger.info(f"openalex_fetch_by_pmid_success pmid={pmid} status={response.status_code}")
            return self._parse_work(payload)
        except ApiClientError as exc:
            # Специальная обработка для ошибок 429 от OpenAlex
            status_code = exc.context.details.get("status_code") if exc.context and exc.context.details else None
            if status_code == 429:
                self.logger.warning(f"openalex_rate_limited pmid={pmid} error={str(exc)} message=OpenAlex API rate limit exceeded. Consider getting an API key.")
                return self._create_empty_record(pmid, f"Rate limited: {str(exc)}")
            
            # Fallback к поиску если прямой запрос не сработал
            self.logger.warning(f"openalex_pmid_fallback pmid={pmid} error={str(exc)}")
            try:
                # Попробуем filter API
                filter_url = f"https://api.openalex.org/works?filter=pmid:{pmid}"
                self.logger.info(f"openalex_pmid_fallback_filter pmid={pmid} url={filter_url}")
                payload = self._request("GET", "", params={"filter": f"pmid:{pmid}"})
                results = payload.get("results", [])
                self.logger.info(f"openalex_pmid_fallback_filter_result pmid={pmid} results_count={len(results)}")
                if results:
                    return self._parse_work(results[0])

                # Попробуем search API
                search_url = f"https://api.openalex.org/works?search={pmid}"
                self.logger.info(f"openalex_pmid_search_fallback pmid={pmid} url={search_url}")
                payload = self._request("GET", "", params={"search": pmid})
                results = payload.get("results", [])
                self.logger.info(f"openalex_pmid_search_fallback_result pmid={pmid} results_count={len(results)}")
                if not results:
                    raise ApiClientError(f"No OpenAlex work found for PMID {pmid}")
                return self._parse_work(results[0])
            except ApiClientError as fallback_exc:
                fallback_status_code = fallback_exc.context.details.get("status_code") if fallback_exc.context and fallback_exc.context.details else None
                if fallback_status_code == 429:
                    return self._create_empty_record(pmid, f"Rate limited: {str(fallback_exc)}")
                
                # Fallback к поиску по заголовку если есть
                if title:
                    self.logger.info(f"openalex_pmid_title_fallback pmid={pmid} title={title}")
                    return self._search_by_title(title, pmid)
                
                raise

    def _parse_work(self, work: dict[str, Any]) -> dict[str, Any]:
        ids = work.get("ids") or {}

        # Debug logging - добавляем больше информации для диагностики
        self.logger.debug(f"openalex_parse_work work_type={work.get('type')} work_type_crossref={work.get('type_crossref')} work_keys={list(work.keys())}")
        
        # Логируем ключевые поля для диагностики
        title = work.get("display_name")
        doi_value = ids.get("doi")
        pmid_value = self._extract_pmid(work)
        self.logger.info(
            f"openalex_parse_work_fields title_length={len(title) if title else 0} "
            f"doi={doi_value} pmid={pmid_value} has_abstract={bool(work.get('abstract_inverted_index'))}"
        )

        # Извлекаем DOI - согласно OpenAlex API, DOI находится в ids.doi
        doi_value = ids.get("doi")
        if doi_value and doi_value.startswith("https://doi.org/"):
            doi_value = doi_value.replace("https://doi.org/", "")

        # OpenAlex НЕ возвращает title напрямую, только display_name
        # display_name может быть заголовком статьи или описанием работы
        title = work.get("display_name")

        # Извлекаем publication_year - основное поле в OpenAlex API
        pub_year = work.get("publication_year")
        if pub_year is not None:
            try:
                pub_year = int(pub_year)
            except (ValueError, TypeError):
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
        

        record: dict[str, Any] = {
            "source": "openalex",
            "openalex_doi": doi_value,
            "openalex_title": title,
            "openalex_doc_type": work.get("type"),
            "openalex_crossref_doc_type": type_crossref,
            "openalex_year": pub_year,
            "openalex_pmid": self._extract_pmid(work),
            "openalex_abstract": self._reconstruct_abstract(work.get("abstract_inverted_index")),
            "openalex_issn": self._extract_issn(work),
            "openalex_authors": self._extract_authors(work),
            "openalex_journal": self._extract_journal(work),
            "openalex_volume": self._extract_volume(work),
            "openalex_issue": self._extract_issue(work),
            "openalex_first_page": self._extract_first_page(work),
            "openalex_last_page": self._extract_last_page(work),
            "openalex_error": None,  # Will be set if there's an error
        }

        # Debug logging for the final record
        self.logger.debug(f"openalex_parsed_record openalex_crossref_doc_type={type_crossref} openalex_doc_type={work.get('type')}")

        # Return all fields, including None values, to maintain schema consistency
        return record

    def _extract_pmid(self, work: dict[str, Any]) -> str | None:
        """Извлекает PMID из OpenAlex work."""
        # OpenAlex может содержать PMID в ids
        ids = work.get("ids", {})
        pmid = ids.get("pmid") or ids.get("pubmed")
        if pmid:
            # Убираем префикс если есть
            if pmid.startswith("https://pubmed.ncbi.nlm.nih.gov/"):
                pmid = pmid.split("/")[-1]
            return str(pmid)

        # Проверяем в external_ids
        external_ids = work.get("external_ids", {})
        pmid = external_ids.get("pmid") or external_ids.get("pubmed")
        if pmid:
            return str(pmid)

        return None

    def _extract_authors(self, work: dict[str, Any]) -> list[str] | None:
        """Извлекает авторов из OpenAlex work."""
        authors = work.get("authorships")
        if isinstance(authors, list):
            author_names = []
            for authorship in authors:
                if isinstance(authorship, dict):
                    author = authorship.get("author")
                    if isinstance(author, dict):
                        # OpenAlex имеет структуру: {"display_name": "John Doe"}
                        display_name = author.get("display_name")
                        if display_name:
                            author_names.append(display_name)
            return author_names if author_names else None

        return None

    def _reconstruct_abstract(self, inverted_index: dict[str, list[int]] | None) -> str | None:
        """Реконструирует абстракт из inverted index формата OpenAlex.

        OpenAlex хранит абстракты в формате inverted index:
        {"word1": [0, 5], "word2": [1, 6], ...}
        где числа - позиции слов в тексте.
        """
        if not inverted_index or not isinstance(inverted_index, dict):
            return None

        try:
            # Создаем список (позиция, слово)
            word_positions = []
            for word, positions in inverted_index.items():
                if isinstance(positions, list):
                    for pos in positions:
                        if isinstance(pos, int):
                            word_positions.append((pos, word))

            if not word_positions:
                return None

            # Сортируем по позиции
            word_positions.sort(key=lambda x: x[0])

            # Собираем текст
            abstract = " ".join(word for _, word in word_positions)
            return abstract if abstract else None

        except Exception as e:
            self.logger.warning(f"Failed to reconstruct abstract: {e}")
            return None

    def _extract_journal(self, work: dict[str, Any]) -> str | None:
        """Извлекает название журнала из OpenAlex work."""
        # Проверяем primary_location -> source -> display_name
        primary_location = work.get("primary_location")
        if isinstance(primary_location, dict):
            source = primary_location.get("source")
            if isinstance(source, dict):
                display_name = source.get("display_name")
                if display_name:
                    return str(display_name)

        # Fallback на host_venue (старый формат API)
        host_venue = work.get("host_venue")
        if isinstance(host_venue, dict):
            display_name = host_venue.get("display_name")
            if display_name:
                return str(display_name)

        return None

    def _extract_issn(self, work: dict[str, Any]) -> str | None:
        """Извлекает ISSN из OpenAlex work."""
        # OpenAlex может содержать ISSN в разных местах
        issn = work.get("issn")
        if issn:
            if isinstance(issn, list) and issn:
                return issn[0]  # Берем первый ISSN если их несколько
            return str(issn)

        # Проверяем в primary_location.source
        primary_location = work.get("primary_location")
        if primary_location and isinstance(primary_location, dict):
            source = primary_location.get("source")
            if source and isinstance(source, dict):
                issn = source.get("issn")
                if issn:
                    return str(issn)

        # Проверяем в locations
        locations = work.get("locations", [])
        if isinstance(locations, list):
            for location in locations:
                if isinstance(location, dict):
                    source = location.get("source")
                    if source and isinstance(source, dict):
                        issn = source.get("issn")
                        if issn:
                            return str(issn)

        return None

    def _extract_volume(self, work: dict[str, Any]) -> str | None:
        """Извлекает том из OpenAlex work."""
        # OpenAlex хранит библиографические данные в biblio
        biblio = work.get("biblio", {})
        if isinstance(biblio, dict):
            volume = biblio.get("volume")
            if volume:
                return str(volume)
        
        # Проверяем в primary_location
        primary_location = work.get("primary_location")
        if isinstance(primary_location, dict):
            biblio = primary_location.get("biblio", {})
            if isinstance(biblio, dict):
                volume = biblio.get("volume")
                if volume:
                    return str(volume)
        
        return None

    def _extract_issue(self, work: dict[str, Any]) -> str | None:
        """Извлекает номер выпуска из OpenAlex work."""
        # OpenAlex хранит библиографические данные в biblio
        biblio = work.get("biblio", {})
        if isinstance(biblio, dict):
            issue = biblio.get("issue")
            if issue:
                return str(issue)
        
        # Проверяем в primary_location
        primary_location = work.get("primary_location")
        if isinstance(primary_location, dict):
            biblio = primary_location.get("biblio", {})
            if isinstance(biblio, dict):
                issue = biblio.get("issue")
                if issue:
                    return str(issue)
        
        return None

    def _extract_first_page(self, work: dict[str, Any]) -> str | None:
        """Извлекает первую страницу из OpenAlex work."""
        # OpenAlex хранит библиографические данные в biblio
        biblio = work.get("biblio", {})
        if isinstance(biblio, dict):
            first_page = biblio.get("first_page")
            if first_page:
                return str(first_page)
            
            # Проверяем page_range
            page_range = biblio.get("page_range")
            if page_range and isinstance(page_range, str) and "-" in page_range:
                return page_range.split("-")[0].strip()
        
        # Проверяем в primary_location
        primary_location = work.get("primary_location")
        if isinstance(primary_location, dict):
            biblio = primary_location.get("biblio", {})
            if isinstance(biblio, dict):
                first_page = biblio.get("first_page")
                if first_page:
                    return str(first_page)
        
        return None

    def _extract_last_page(self, work: dict[str, Any]) -> str | None:
        """Извлекает последнюю страницу из OpenAlex work."""
        # OpenAlex хранит библиографические данные в biblio
        biblio = work.get("biblio", {})
        if isinstance(biblio, dict):
            last_page = biblio.get("last_page")
            if last_page:
                return str(last_page)
            
            # Проверяем page_range
            page_range = biblio.get("page_range")
            if page_range and isinstance(page_range, str) and "-" in page_range:
                parts = page_range.split("-")
                if len(parts) > 1:
                    return parts[1].strip()
        
        # Проверяем в primary_location
        primary_location = work.get("primary_location")
        if isinstance(primary_location, dict):
            biblio = primary_location.get("biblio", {})
            if isinstance(biblio, dict):
                last_page = biblio.get("last_page")
                if last_page:
                    return str(last_page)
        
        return None

    def _create_empty_record(self, identifier: str, error_msg: str) -> dict[str, Any]:
        """Создает пустую запись для случая ошибки."""
        return {
            "source": "openalex",
            "openalex_doi": None,
            "openalex_title": None,
            "openalex_doc_type": None,
            "openalex_crossref_doc_type": None,
            "openalex_year": None,
            "openalex_pmid": None,
            "openalex_abstract": None,
            "openalex_issn": None,
            "openalex_authors": None,
            "openalex_journal": None,
            "openalex_volume": None,
            "openalex_issue": None,
            "openalex_first_page": None,
            "openalex_last_page": None,
            "openalex_error": error_msg,
        }

    def fetch_by_dois_batch(self, dois: list[str], batch_size: int = 50) -> dict[str, dict[str, Any]]:
        """Fetch multiple works by DOIs using filter.

        OpenAlex supports batch queries using filter with OR operator.
        """
        if not dois:
            return {}

        results = {}
        
        success_count = 0
        error_count = 0
        
        self.logger.info(f"openalex_fetch_by_dois_batch starting dois_count={len(dois)} batch_size={batch_size}")

        # Process in chunks to avoid URL length limits
        for i in range(0, len(dois), batch_size):
            chunk = dois[i : i + batch_size]
            self.logger.info(f"openalex_fetch_by_dois_batch_chunk chunk_index={i//batch_size + 1} chunk_size={len(chunk)}")

            try:
                # Create filter string with OR operator
                filter_parts = [f"doi:{doi}" for doi in chunk]
                filter_str = "|".join(filter_parts)
                batch_url = f"https://api.openalex.org/works?filter={filter_str}"
                self.logger.info(f"openalex_fetch_by_dois_batch_url url={batch_url}")

                response = self._request("GET", "", params={"filter": filter_str})
                payload = response.json()
                works = payload.get("results", [])
                
                self.logger.info(f"openalex_fetch_by_dois_batch_response chunk_size={len(chunk)} works_found={len(works)}")

                # Map results back to DOIs
                for work in works:
                    doi_value = work.get("doi")
                    if doi_value and doi_value.startswith("https://doi.org/"):
                        doi_value = doi_value.replace("https://doi.org/", "")

                    if doi_value in chunk:
                        results[doi_value] = self._parse_work(work)
                        success_count += 1

                # Add empty records for missing DOIs
                for doi in chunk:
                    if doi not in results:
                        results[doi] = self._create_empty_record(doi, "Not found in batch response")
                        error_count += 1

            except Exception as e:
                self.logger.warning(f"openalex_fetch_by_dois_batch_error chunk={chunk} error={str(e)}")
                # Add empty records for failed batch
                for doi in chunk:
                    results[doi] = self._create_empty_record(doi, str(e))
        
        self.logger.info(f"openalex_fetch_by_dois_batch_complete total_dois={len(dois)} success_count={success_count} error_count={error_count}")
        return results

    def fetch_by_pmids_batch(
        self,
        pmids: list[str],
        batch_size: int = 50
    ) -> dict[str, dict[str, Any]]:
        """Fetch multiple works by PMIDs using individual requests with fallback.

        OpenAlex batch queries may not find all documents, so we use individual requests
        with the same fallback logic as fetch_by_pmid.
        """
        if not pmids:
            return {}

        results = {}
        success_count = 0
        error_count = 0
        
        self.logger.info(f"openalex_fetch_by_pmids_batch starting pmids_count={len(pmids)}")
        
        # Use individual requests with fallback logic like fetch_by_pmid
        for pmid in pmids:
            try:
                # Try direct URL first (like in reference project)
                path = f"works/pmid:{pmid}"
                full_url = f"https://api.openalex.org/{path}"
                self.logger.info(f"openalex_fetch_by_pmid_batch pmid={pmid} url={full_url}")
                response = self._request("GET", path)
                payload = response.json()
                self.logger.info(f"openalex_fetch_by_pmid_batch_success pmid={pmid} status={response.status_code}")
                results[pmid] = self._parse_work(payload)
                success_count += 1
            except ApiClientError as exc:
                status_code = exc.context.details.get("status_code") if exc.context and exc.context.details else None
                if status_code == 429:
                    self.logger.warning(f"openalex_rate_limited pmid={pmid} error={str(exc)} message=OpenAlex API rate limit exceeded. Consider getting an API key.")
                    results[pmid] = self._create_empty_record(pmid, f"Rate limited: {str(exc)}")
                    error_count += 1
                else:
                    # Fallback to filter API first
                    self.logger.warning(f"openalex_pmid_batch_fallback pmid={pmid} error={str(exc)}")
                    try:
                        filter_url = f"https://api.openalex.org/works?filter=pmid:{pmid}"
                        self.logger.info(f"openalex_pmid_batch_fallback_filter pmid={pmid} url={filter_url}")
                        response = self._request("GET", "", params={"filter": f"pmid:{pmid}"})
                        payload = response.json()
                        filter_results = payload.get("results", [])
                        self.logger.info(f"openalex_pmid_batch_fallback_filter_result pmid={pmid} results_count={len(filter_results)}")
                        if filter_results:
                            results[pmid] = self._parse_work(filter_results[0])
                            success_count += 1
                        else:
                            # Try search API as last resort
                            search_url = f"https://api.openalex.org/works?search={pmid}"
                            self.logger.info(f"openalex_pmid_batch_search_fallback pmid={pmid} url={search_url}")
                            response = self._request("GET", "", params={"search": pmid})
                            payload = response.json()
                            search_results = payload.get("results", [])
                            self.logger.info(f"openalex_pmid_batch_search_fallback_result pmid={pmid} results_count={len(search_results)}")
                            if search_results:
                                results[pmid] = self._parse_work(search_results[0])
                                success_count += 1
                            else:
                                results[pmid] = self._create_empty_record(pmid, "Not found in OpenAlex database")
                                error_count += 1
                    except ApiClientError as fallback_exc:
                        fallback_status_code = fallback_exc.context.details.get("status_code") if fallback_exc.context and fallback_exc.context.details else None
                        if fallback_status_code == 429:
                            results[pmid] = self._create_empty_record(pmid, f"Rate limited: {str(fallback_exc)}")
                        else:
                            results[pmid] = self._create_empty_record(pmid, f"Not found: {str(fallback_exc)}")
                        error_count += 1
            except Exception as e:
                self.logger.error(f"openalex_pmid_batch_error pmid={pmid} error={str(e)}")
                results[pmid] = self._create_empty_record(pmid, f"Error: {str(e)}")
                error_count += 1
        
        self.logger.info(f"openalex_fetch_by_pmids_batch_complete total_pmids={len(pmids)} success_count={success_count} error_count={error_count}")
        
    def _search_by_title(self, title: str, identifier: str) -> dict[str, Any]:
        """Search for work by title as fallback when DOI/PMID lookup fails."""
        try:
            # Очищаем заголовок от HTML тегов и лишних символов
            clean_title = self._clean_title_for_search(title)
            if not clean_title:
                return self._create_empty_record(identifier, "Empty title for search")

            # Используем поиск по заголовку через search API
            search_params = {
                "search": clean_title,
                "per_page": 5,  # Ограничиваем результаты
            }

            response = self._request("GET", "", params=search_params)
            payload = response.json()

            # Проверяем результаты поиска
            if isinstance(payload, dict) and "results" in payload:
                works = payload.get("results", [])
                if works:
                    # Берем первый результат (наиболее релевантный)
                    best_match = works[0]
                    self.logger.info(f"openalex_title_search_success identifier={identifier} title={clean_title} found_work_id={best_match.get('id', 'unknown')}")
                    return self._parse_work(best_match)

            # Если поиск не дал результатов
            self.logger.warning(f"openalex_title_search_no_results identifier={identifier} title={clean_title}")
            return self._create_empty_record(identifier, f"Not found by title search: {clean_title}")

        except Exception as exc:
            self.logger.error(f"openalex_title_search_failed identifier={identifier} title={title} error={str(exc)}")
            return self._create_empty_record(identifier, f"Title search failed: {str(exc)}")

    def _clean_title_for_search(self, title: str) -> str:
        """Clean title for search by removing HTML tags and special characters."""
        if not title:
            return ""

        import re

        # Удаляем HTML теги
        clean = re.sub(r"<[^>]+>", "", title)

        # Удаляем лишние пробелы
        clean = re.sub(r"\s+", " ", clean).strip()

        # Ограничиваем длину для поиска (OpenAlex имеет лимиты)
        if len(clean) > 200:
            clean = clean[:200]

        return clean
        
