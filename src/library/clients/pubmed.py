"""Client for the PubMed API."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from library.clients.base import ApiClientError, BaseApiClient
from library.config import APIClientConfig
from library.utils.list_converter import convert_authors_list, safe_str_convert


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
        
        # Use fallback strategy for handling rate limiting and other errors
        payload = self._request_with_fallback("GET", "esummary.fcgi", params=params)
        
        # Check if we got fallback data
        if payload.get("source") == "fallback":
            self.logger.warning(
                "pubmed_fallback_used",
                pmid=pmid,
                error=payload.get("error"),
                fallback_reason=payload.get("fallback_reason")
            )
            return self._create_empty_record(pmid, payload.get("error", "Unknown error"))
            
        record = self._extract_record(payload, pmid)
        if record is None:
            raise ApiClientError(f"No PubMed record found for PMID {pmid}")
        
        # Пытаемся получить дополнительную информацию через efetch
        try:
            enhanced_record = self._enhance_with_efetch(record, pmid)
            return enhanced_record
        except Exception as e:
            # Если efetch не работает, возвращаем базовую запись
            self.logger.warning("efetch_failed pmid=%s error=%s", pmid, str(e))
            return record

    def fetch_by_pmids(self, pmids: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Fetch multiple PMIDs in batch using esummary.fcgi."""
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
            
        try:
            payload = self._request("GET", "esummary.fcgi", params=params)
        except Exception as e:
            self.logger.error("pubmed_batch_request_failed pmids=%s error=%s", pmid_list, str(e))
            # Возвращаем пустые записи с ошибками для всех PMID
            result = {}
            for pmid in pmid_list:
                result[str(pmid)] = self._create_empty_record(pmid, f"Batch request failed: {str(e)}")
            return result
        
        result: dict[str, dict[str, Any]] = {}
        for pmid in pmid_list:
            record = self._extract_record(payload, pmid)
            if record is not None:
                result[str(pmid)] = record
            else:
                # Создаем пустую запись с ошибкой если не удалось извлечь
                result[str(pmid)] = self._create_empty_record(pmid, "No data found")
        return result

    def fetch_by_pmids_batch(self, pmids: list[str], batch_size: int = 200) -> dict[str, dict[str, Any]]:
        """Fetch multiple PMIDs in batches to avoid rate limits."""
        if not pmids:
            return {}
        
        result: dict[str, dict[str, Any]] = {}
        
        # Обрабатываем PMID батчами
        for i in range(0, len(pmids), batch_size):
            batch = pmids[i:i + batch_size]
            try:
                self.logger.debug(f"Processing PubMed batch {i//batch_size + 1} with {len(batch)} PMIDs")
                batch_result = self.fetch_by_pmids(batch)
                result.update(batch_result)
            except Exception as e:
                self.logger.error(f"Failed to process PubMed batch {i//batch_size + 1}: {e}")
                # Добавляем ошибки для всех PMID в батче
                for pmid in batch:
                    result[str(pmid)] = self._create_empty_record(pmid, f"Batch processing failed: {str(e)}")
        
        return result

    def _extract_record(self, payload: dict[str, Any], pmid: str) -> dict[str, Any] | None:
        # Проверяем наличие ошибок в ответе
        if "error" in payload:
            error_msg = payload.get("error", "Unknown error")
            self.logger.warning("pubmed_api_error pmid=%s error=%s", pmid, error_msg)
            return None
            
        # NCBI E-utilities возвращает данные в формате {"result": {"uids": [...], "pmid": {...}}}
        if "result" in payload and isinstance(payload["result"], dict):
            result = payload["result"]
            
            # Проверяем, есть ли запрашиваемый PMID в списке UIDs
            uids = result.get("uids", [])
            if str(pmid) not in uids:
                self.logger.warning("pmid_not_found pmid=%s available_uids=%s", pmid, uids)
                return None
                
            # Получаем данные для конкретного PMID
            data = result.get(pmid)
            if data is not None:
                return self._normalise_record(data)
            else:
                self.logger.warning("no_data_for_pmid pmid=%s", pmid)
                return None

        # Fallback для других форматов (если API изменится)
        if "records" in payload and isinstance(payload["records"], list):
            for item in payload["records"]:
                if str(item.get("pmid")) == str(pmid):
                    return self._normalise_record(item)

        if payload.get("pmid") and str(payload.get("pmid")) == str(pmid):
            return self._normalise_record(payload)
            
        self.logger.warning("unexpected_payload_format pmid=%s payload_keys=%s", pmid, list(payload.keys()))
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

        # Обрабатываем MeSH descriptors и qualifiers
        mesh_descriptors = record.get("meshdescriptors")
        if isinstance(mesh_descriptors, list):
            mesh_descriptors = "; ".join(str(d) for d in mesh_descriptors if d)
        elif mesh_descriptors is None:
            mesh_descriptors = None
            
        mesh_qualifiers = record.get("meshqualifiers")
        if isinstance(mesh_qualifiers, list):
            mesh_qualifiers = "; ".join(str(q) for q in mesh_qualifiers if q)
        elif mesh_qualifiers is None:
            mesh_qualifiers = None
            
        chemical_list = record.get("chemicals")
        if isinstance(chemical_list, list):
            chemical_list = "; ".join(str(c) for c in chemical_list if c)
        elif chemical_list is None:
            chemical_list = None
        
        # Обрабатываем publication type
        pub_type = record.get("pubtype")
        if isinstance(pub_type, list):
            pub_type = "; ".join(str(t) for t in pub_type if t)
        elif pub_type is None:
            pub_type = None

        parsed: dict[str, Any | None] = {
            "source": "pubmed",
            "pubmed_pmid": record.get("uid") or record.get("pmid") or record.get("PMID"),
            "pubmed_doi": doi_value,
            "pubmed_article_title": record.get("title") or record.get("articleTitle"),
            "pubmed_abstract": record.get("abstract"),
            "pubmed_journal": record.get("source") or record.get("journalTitle"),
            "pubmed_volume": record.get("volume"),
            "pubmed_issue": record.get("issue"),
            "pubmed_first_page": record.get("pages", "").split("-")[0] if record.get("pages") else None,
            "pubmed_last_page": record.get("pages", "").split("-")[1] if record.get("pages") and "-" in record.get("pages", "") else None,
            "pubmed_doc_type": pub_type,
            "pubmed_mesh_descriptors": mesh_descriptors,
            "pubmed_mesh_qualifiers": mesh_qualifiers,
            "pubmed_chemical_list": chemical_list,
            "pubmed_year_completed": pub_year,
            "pubmed_month_completed": pub_month,
            "pubmed_day_completed": pub_day,
            "pubmed_year_revised": self._extract_revised_date(record, "year"),
            "pubmed_month_revised": self._extract_revised_date(record, "month"),
            "pubmed_day_revised": self._extract_revised_date(record, "day"),
            "pubmed_issn": record.get("issn"),
            "pubmed_error": None,  # Will be set if there's an error
            # Legacy fields for backward compatibility
            "title": record.get("title") or record.get("articleTitle"),
            "abstract": record.get("abstract"),
            "doi": doi_value,
            "pubmed_authors": convert_authors_list(formatted_authors),
        }
        
        # Debug logging для проверки MeSH данных
        pmid = record.get("pmid", "unknown")
        self.logger.debug("PubMed record for %s: mesh_descriptors=%s, "
                         "mesh_qualifiers=%s, chemical_list=%s", pmid, mesh_descriptors, 
                         mesh_qualifiers, chemical_list)
        
        # Return all fields, including None values, to maintain schema consistency
        return parsed

    def _extract_revised_date(self, record: dict[str, Any], date_part: str) -> str | None:
        """Извлекает revised дату из поля history."""
        history = record.get("history", [])
        if not history:
            return None
        
        # Берем последнюю дату из history (обычно это дата последнего обновления)
        last_entry = history[-1]
        date_str = last_entry.get("date", "")
        
        if not date_str:
            return None
        
        try:
            # Парсим дату в формате "1980/04/01 00:00"
            date_part_only = date_str.split(" ")[0]  # "1980/04/01"
            parts = date_part_only.split("/")
            
            if len(parts) >= 3:
                year, month, day = parts[0], parts[1], parts[2]
                
                if date_part == "year":
                    return year
                elif date_part == "month":
                    return month
                elif date_part == "day":
                    return day
        except (ValueError, IndexError):
            pass
        
        return None

    def _create_empty_record(self, pmid: str, error_msg: str) -> dict[str, Any]:
        """Создает пустую запись для случая ошибки."""
        return {
            "source": "pubmed",
            "pubmed_pmid": pmid,
            "pubmed_doi": None,
            "pubmed_article_title": None,
            "pubmed_abstract": None,
            "pubmed_journal": None,
            "pubmed_volume": None,
            "pubmed_issue": None,
            "pubmed_first_page": None,
            "pubmed_last_page": None,
            "pubmed_doc_type": None,
            "pubmed_mesh_descriptors": None,
            "pubmed_mesh_qualifiers": None,
            "pubmed_chemical_list": None,
            "pubmed_year_completed": None,
            "pubmed_month_completed": None,
            "pubmed_day_completed": None,
            "pubmed_year_revised": None,
            "pubmed_month_revised": None,
            "pubmed_day_revised": None,
            "pubmed_issn": None,
            "pubmed_error": error_msg,
            # Legacy fields
            "title": None,
            "abstract": None,
            "doi": None,
            "authors": None,
        }

    def _enhance_with_efetch(self, record: dict[str, Any], pmid: str) -> dict[str, Any]:
        """Улучшает запись данными из efetch для получения DOI и abstract."""
        try:
            # Получаем полную информацию через efetch напрямую через requests
            import requests
            
            fetch_params = {
                "db": "pubmed",
                "id": pmid,
                "retmode": "xml",
                "rettype": "abstract"
            }
            
            # Добавляем API ключ если он настроен
            api_key = getattr(self.config, 'api_key', None)
            if api_key:
                fetch_params["api_key"] = api_key
                
            # Делаем прямой запрос к efetch
            response = requests.get(
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
                params=fetch_params,
                headers={"Accept": "application/xml"},
                timeout=30.0
            )
            
            if response.status_code == 200:
                xml_content = response.text
                
                # Простой парсинг XML для извлечения DOI и abstract
                import re
                
                # Ищем DOI в XML
                doi_match = re.search(r'<ArticleId IdType="doi">([^<]+)</ArticleId>', xml_content)
                if doi_match:
                    record["pubmed_doi"] = doi_match.group(1)
                
                # Ищем abstract в XML - улучшенный поиск
                # Сначала ищем все AbstractText теги
                abstract_matches = re.findall(r'<AbstractText[^>]*>(.*?)</AbstractText>', xml_content, re.DOTALL)
                if abstract_matches:
                    # Объединяем все части abstract
                    abstract_parts = []
                    for match in abstract_matches:
                        # Очищаем от HTML тегов внутри abstract
                        clean_abstract = re.sub(r'<[^>]+>', '', match).strip()
                        if clean_abstract:
                            abstract_parts.append(clean_abstract)
                    
                    if abstract_parts:
                        record["pubmed_abstract"] = ' '.join(abstract_parts)
                
                # Если не нашли через AbstractText, попробуем другие варианты
                if not record.get("pubmed_abstract"):
                    # Ищем в других возможных местах
                    abstract_alt = re.search(r'<Abstract[^>]*>(.*?)</Abstract>', xml_content, re.DOTALL)
                    if abstract_alt:
                        clean_abstract = re.sub(r'<[^>]+>', '', abstract_alt.group(1)).strip()
                        if clean_abstract:
                            record["pubmed_abstract"] = clean_abstract
                
                # Извлекаем MeSH descriptors
                mesh_descriptors = re.findall(r'<MeshHeadingList[^>]*>.*?<DescriptorName[^>]*>([^<]+)</DescriptorName>.*?</MeshHeadingList>', xml_content, re.DOTALL)
                if mesh_descriptors:
                    record["pubmed_mesh_descriptors"] = "; ".join(mesh_descriptors)
                
                # Извлекаем MeSH qualifiers
                mesh_qualifiers = re.findall(r'<QualifierName[^>]*>([^<]+)</QualifierName>', xml_content)
                if mesh_qualifiers:
                    record["pubmed_mesh_qualifiers"] = "; ".join(mesh_qualifiers)
                
                # Извлекаем Chemical List
                chemical_list = re.findall(r'<ChemicalList[^>]*>.*?<NameOfSubstance[^>]*>([^<]+)</NameOfSubstance>.*?</ChemicalList>', xml_content, re.DOTALL)
                if chemical_list:
                    record["pubmed_chemical_list"] = "; ".join(chemical_list)
            
            return record
            
        except Exception as e:
            self.logger.warning("efetch_parsing_failed pmid=%s error=%s", pmid, str(e))
            return record

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