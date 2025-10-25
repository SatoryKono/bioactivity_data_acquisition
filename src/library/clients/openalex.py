"""Client for the OpenAlex API."""

from __future__ import annotations

import logging
from typing import Any

<<<<<<< Updated upstream
from library.clients.base import ApiClientError, BaseApiClient
from library.config import APIClientConfig
=======
from library.clients.base import BaseApiClient
from library.common.exceptions import ApiClientError
from library.settings import APIClientConfig
from library.utils.list_converter import convert_authors_list, convert_issn_list
>>>>>>> Stashed changes

logger = logging.getLogger(__name__)


class OpenAlexClient(BaseApiClient):
    """HTTP client for OpenAlex works."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
<<<<<<< Updated upstream
        super().__init__(config, **kwargs)
=======
        # Добавляем email в User-Agent для polite pool согласно документации OpenAlex
        headers = dict(config.headers)
        if "User-Agent" not in headers:
            headers["User-Agent"] = "bioactivity-data-acquisition/0.1.0 (mailto:your-email@example.com)"
        enhanced = config.model_copy(update={"headers": headers})
        super().__init__(enhanced, **kwargs)
        self.logger = logger
>>>>>>> Stashed changes

    def fetch_by_doi(self, doi: str) -> dict[str, Any]:
        """Fetch a work by DOI with fallback to a filter query."""

        # Use OpenAlex API format: https://api.openalex.org/works/https://doi.org/{doi}
        path = f"https://api.openalex.org/works/https://doi.org/{doi}"
        try:
            response = self._request("GET", path)
            payload = response.json()
            return self._parse_work(payload)
        except ApiClientError as exc:
            # Специальная обработка для ошибок 429 от OpenAlex
            status_code = exc.context.details.get("status_code") if exc.context and exc.context.details else None
            if status_code == 429:
                self.logger.warning(f"openalex_rate_limited doi={doi} error={str(exc)} message=OpenAlex API rate limit exceeded. Consider getting an API key.")
                return self._create_empty_record(doi, f"Rate limited: {str(exc)}")
<<<<<<< Updated upstream
            
            self.logger.info("openalex_doi_fallback", doi=doi, error=str(exc))
=======

            self.logger.info("openalex_doi_fallback doi=%s error=%s", doi, str(exc))
>>>>>>> Stashed changes
            try:
                # Fallback to OpenAlex search API
                response = self._request("GET", "", params={"filter": f"doi:{doi}"})
                payload = response.json()
                results = payload.get("results", [])
                if not results:
                    raise
                return self._parse_work(results[0])
            except ApiClientError as fallback_exc:
                fallback_status_code = fallback_exc.context.details.get("status_code") if fallback_exc.context and fallback_exc.context.details else None
                if fallback_status_code == 429:
                    return self._create_empty_record(doi, f"Rate limited: {str(fallback_exc)}")
                raise

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any]:
        """Fetch a work by PMID with fallback search."""

        try:
<<<<<<< Updated upstream
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
=======
            # Используем прямой URL как в референсном проекте: /works/pmid:{pmid}
            path = f"works/pmid:{pmid}"
            response = self._request("GET", path)
            payload = response.json()
            return self._parse_work(payload)
>>>>>>> Stashed changes
        except ApiClientError as exc:
            # Специальная обработка для ошибок 429 от OpenAlex
            status_code = exc.context.details.get("status_code") if exc.context and exc.context.details else None
            if status_code == 429:
                self.logger.warning(f"openalex_rate_limited pmid={pmid} error={str(exc)} message=OpenAlex API rate limit exceeded. Consider getting an API key.")
                return self._create_empty_record(pmid, f"Rate limited: {str(exc)}")
<<<<<<< Updated upstream
            raise
=======

            # Fallback к поиску если прямой запрос не сработал
            self.logger.info("openalex_pmid_fallback pmid=%s", pmid)
            try:
                response = self._request("GET", "", params={"search": pmid})
                payload = response.json()
                results = payload.get("results", [])
                if not results:
                    raise ApiClientError(f"No OpenAlex work found for PMID {pmid}")
                return self._parse_work(results[0])
            except ApiClientError as fallback_exc:
                fallback_status_code = fallback_exc.context.details.get("status_code") if fallback_exc.context and fallback_exc.context.details else None
                if fallback_status_code == 429:
                    return self._create_empty_record(pmid, f"Rate limited: {str(fallback_exc)}")
                raise
>>>>>>> Stashed changes

    def _parse_work(self, work: dict[str, Any]) -> dict[str, Any]:
        ids = work.get("ids") or {}

        # Debug logging
        self.logger.debug(f"openalex_parse_work work_type={work.get('type')} work_type_crossref={work.get('type_crossref')} work_keys={list(work.keys())}")

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
<<<<<<< Updated upstream
        
        record: dict[str, Any | None] = {
=======

        # Извлекаем библиографические данные из biblio
        biblio = work.get("biblio", {})
        openalex_volume = None
        openalex_issue = None
        openalex_first_page = None
        openalex_last_page = None

        if biblio:
            openalex_volume = biblio.get("volume")
            openalex_issue = biblio.get("issue")
            openalex_first_page = biblio.get("first_page")
            openalex_last_page = biblio.get("last_page")

        # Реконструируем abstract из inverted index
        abstract = self._reconstruct_abstract(work.get("abstract_inverted_index"))

        # Извлекаем journal из host_venue
        journal = self._extract_journal(work)

        record: dict[str, Any] = {
>>>>>>> Stashed changes
            "source": "openalex",
            "openalex_doi": doi_value,
            "openalex_title": title,
            "openalex_doc_type": work.get("type"),
            "openalex_crossref_doc_type": type_crossref,
            "openalex_year": pub_year,
            "openalex_pmid": self._extract_pmid(work),
            "openalex_abstract": work.get("abstract"),
            "openalex_issn": self._extract_issn(work),
            "openalex_authors": self._extract_authors(work),
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
<<<<<<< Updated upstream
=======

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
>>>>>>> Stashed changes

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
            "openalex_error": error_msg,
        }

    def fetch_by_dois_batch(self, dois: list[str], batch_size: int = 50) -> dict[str, dict[str, Any]]:
        """Fetch multiple works by DOIs using filter.

        OpenAlex supports batch queries using filter with OR operator.
        """
        if not dois:
            return {}

        results = {}
<<<<<<< Updated upstream
        
=======
        success_count = 0
        error_count = 0

>>>>>>> Stashed changes
        # Process in chunks to avoid URL length limits
        for i in range(0, len(dois), batch_size):
            chunk = dois[i : i + batch_size]

            try:
                # Create filter string with OR operator
                filter_parts = [f"doi:{doi}" for doi in chunk]
                filter_str = "|".join(filter_parts)

                response = self._request("GET", "", params={"filter": filter_str})
                payload = response.json()
                works = payload.get("results", [])

                # Map results back to DOIs
                for work in works:
                    doi_value = work.get("doi")
                    if doi_value and doi_value.startswith("https://doi.org/"):
                        doi_value = doi_value.replace("https://doi.org/", "")

                    if doi_value in chunk:
                        results[doi_value] = self._parse_work(work)
<<<<<<< Updated upstream
                
=======
                        success_count += 1

>>>>>>> Stashed changes
                # Add empty records for missing DOIs
                for doi in chunk:
                    if doi not in results:
                        results[doi] = self._create_empty_record(doi, "Not found in batch response")
<<<<<<< Updated upstream
                        
=======
                        error_count += 1

>>>>>>> Stashed changes
            except Exception as e:
                logger.warning(f"Failed to fetch DOIs batch {chunk}: {e}")
                # Add empty records for failed batch
                for doi in chunk:
                    results[doi] = self._create_empty_record(doi, str(e))
<<<<<<< Updated upstream
        
        return results

    def fetch_by_pmids_batch(
        self,
        pmids: list[str],
        batch_size: int = 50
    ) -> dict[str, dict[str, Any]]:
        """Fetch multiple works by PMIDs using filter.
        
        OpenAlex supports batch queries using filter with OR operator.
=======
                    error_count += 1

        self.logger.info(f"OpenAlex batch completed: {success_count} successful, {error_count} errors out of {len(dois)} DOIs")
        return results

    def fetch_by_pmids_batch(self, pmids: list[str], batch_size: int = 50) -> dict[str, dict[str, Any]]:
        """Fetch multiple works by PMIDs using individual requests with fallback.

        OpenAlex batch queries may not find all documents, so we use individual requests
        with the same fallback logic as fetch_by_pmid.
>>>>>>> Stashed changes
        """
        if not pmids:
            return {}

        results = {}
<<<<<<< Updated upstream
        
        # Process in chunks to avoid URL length limits
        for i in range(0, len(pmids), batch_size):
            chunk = pmids[i:i + batch_size]
            
            try:
                # Create filter string with OR operator
                filter_parts = [f"pmid:{pmid}" for pmid in chunk]
                filter_str = "|".join(filter_parts)
                
                payload = self._request("GET", "", params={"filter": filter_str})
                works = payload.get("results", [])
                
                # Map results back to PMIDs
                for work in works:
                    ids = work.get("ids", {})
                    pmid_value = ids.get("pmid")
                    if pmid_value and pmid_value.startswith("https://pubmed.ncbi.nlm.nih.gov/"):
                        pmid_value = pmid_value.replace("https://pubmed.ncbi.nlm.nih.gov/", "")
                    
                    if pmid_value in chunk:
                        results[pmid_value] = self._parse_work(work)
                
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

        # Use individual requests with fallback logic like fetch_by_pmid
        for pmid in pmids:
            try:
                # Try direct URL first (like in reference project)
                path = f"works/pmid:{pmid}"
                response = self._request("GET", path)
                payload = response.json()
                results[pmid] = self._parse_work(payload)
            except ApiClientError as exc:
                status_code = exc.context.details.get("status_code") if exc.context and exc.context.details else None
                if status_code == 429:
                    self.logger.warning(f"openalex_rate_limited pmid={pmid} error={str(exc)} message=OpenAlex API rate limit exceeded. Consider getting an API key.")
                    results[pmid] = self._create_empty_record(pmid, f"Rate limited: {str(exc)}")
                else:
                    # Fallback to search if direct request fails
                    self.logger.info("openalex_pmid_fallback pmid=%s", pmid)
                    try:
                        response = self._request("GET", "", params={"search": pmid})
                        payload = response.json()
                        search_results = payload.get("results", [])
                        if search_results:
                            results[pmid] = self._parse_work(search_results[0])
                        else:
                            results[pmid] = self._create_empty_record(pmid, "Not found in OpenAlex database")
                    except ApiClientError as fallback_exc:
                        fallback_status_code = fallback_exc.context.details.get("status_code") if fallback_exc.context and fallback_exc.context.details else None
                        if fallback_status_code == 429:
                            results[pmid] = self._create_empty_record(pmid, f"Rate limited: {str(fallback_exc)}")
                        else:
                            results[pmid] = self._create_empty_record(pmid, f"Not found: {str(fallback_exc)}")
            except Exception as e:
                self.logger.warning("Failed to fetch PMID %s: %s", pmid, e)
                results[pmid] = self._create_empty_record(pmid, str(e))

        return results
>>>>>>> Stashed changes
