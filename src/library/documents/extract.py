"""Модуль для извлечения данных документов из внешних источников."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

# from library.documents.column_mapping import apply_field_mapping  # Удалено: клиенты уже возвращают данные с правильными префиксами

logger = logging.getLogger(__name__)


def extract_from_pubmed(client: Any, pmids: list[str], batch_size: int = 200) -> pd.DataFrame:
    """Извлечь данные из PubMed по списку PMID.
    
    Args:
        client: PubMedClient для запросов к API
        pmids: Список PubMed идентификаторов
        batch_size: Размер батча для запросов
        
    Returns:
        DataFrame с данными из PubMed
    """
    if not pmids:
        return pd.DataFrame()
    
    logger.info(f"Extracting PubMed data for {len(pmids)} PMIDs")
    
    # Статистика для диагностики
    total_requested = len(pmids)
    successful_records = 0
    error_records = 0
    error_categories = {"404": 0, "timeout": 0, "rate_limit": 0, "parse_error": 0, "other": 0}
    
    try:
        # Используем батч-метод если доступен
        if hasattr(client, 'fetch_by_pmids_batch'):
            # Используем новый batch метод
            try:
                logger.debug(f"Fetching PubMed data for {len(pmids)} PMIDs using batch method")
                records = client.fetch_by_pmids_batch(pmids, batch_size)
                logger.info(f"Successfully fetched {len(records)} records from PubMed using batch method")
            except Exception as e:
                logger.error(f"Failed to fetch PubMed data using batch method: {e}")
                records = {}
                for pmid in pmids:
                    records[pmid] = {"pmid": pmid, "error": str(e)}
        elif hasattr(client, 'fetch_by_pmids'):
            # Fallback к старому методу
            try:
                logger.debug(f"Fetching PubMed data for {len(pmids)} PMIDs using individual requests")
                records = client.fetch_by_pmids(pmids)
                logger.info(f"Successfully fetched {len(records)} records from PubMed using individual requests")
            except Exception as e:
                logger.error(f"Failed to fetch PubMed data using individual requests: {e}")
                records = {}
                for pmid in pmids:
                    records[pmid] = {"pmid": pmid, "error": str(e)}
        else:
            # Fallback к одиночным запросам
            records = {}
            for pmid in pmids:
                try:
                    record = client.fetch_by_pmid(pmid)
                    records[pmid] = record
                except Exception as e:
                    logger.warning(f"Failed to fetch PubMed data for PMID {pmid}: {e}")
                    records[pmid] = {"pmid": pmid, "error": str(e)}
        
        # Преобразуем в список записей с правильным маппингом
        record_list = []
        for pmid, record in records.items():
            if isinstance(record, dict):
                # Клиент уже возвращает данные с правильными префиксами
                mapped_record = record
                
                # Добавляем join key для объединения
                mapped_record["document_pubmed_id"] = pmid
                
                # Категоризируем ошибки для диагностики
                if "error" in mapped_record:
                    error_msg = str(mapped_record["error"]).lower()
                    if "404" in error_msg or "not found" in error_msg:
                        error_categories["404"] += 1
                    elif "timeout" in error_msg:
                        error_categories["timeout"] += 1
                    elif "rate limit" in error_msg or "429" in error_msg:
                        error_categories["rate_limit"] += 1
                    elif "parse" in error_msg or "json" in error_msg:
                        error_categories["parse_error"] += 1
                    else:
                        error_categories["other"] += 1
                    error_records += 1
                else:
                    successful_records += 1
                
                # Диагностическое логирование
                logger.debug(f"Mapped PubMed record columns: {list(mapped_record.keys())}")
                logger.debug(f"Sample values - doi: {mapped_record.get('pubmed_doi')}, "
                            f"pmid: {mapped_record.get('pubmed_pmid')}, "
                            f"year_completed: {mapped_record.get('pubmed_year_completed')}")
                
                record_list.append(mapped_record)
        
        # Создаем DataFrame
        if record_list:
            df = pd.DataFrame(record_list)
            # Удаляем дублированные колонки
            df = df.loc[:, ~df.columns.duplicated()]
            
            # Логируем детальную статистику
            logger.info(f"PubMed extraction completed: {successful_records} successful, {error_records} errors out of {total_requested} PMIDs")
            logger.info(f"Error breakdown: {error_categories}")
            return df
        else:
            logger.warning("No PubMed records extracted")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Failed to extract PubMed data: {e}")
        # Возвращаем DataFrame с ошибками
        error_records = []
        for pmid in pmids:
            error_record = {"document_pubmed_id": pmid, "pubmed_error": str(e)}
            error_records.append(error_record)
        return pd.DataFrame(error_records)


def extract_from_crossref(client: Any, dois: list[str], batch_size: int = 100) -> pd.DataFrame:
    """Извлечь данные из Crossref по списку DOI.
    
    Args:
        client: CrossrefClient для запросов к API
        dois: Список DOI идентификаторов
        batch_size: Размер батча для запросов
        
    Returns:
        DataFrame с данными из Crossref
    """
    if not dois:
        return pd.DataFrame()
    
    logger.info(f"Extracting Crossref data for {len(dois)} DOIs")
    
    try:
        # Используем батч-метод если доступен
        if hasattr(client, 'fetch_by_dois_batch'):
            # Используем новый batch метод
            try:
                logger.debug(f"Fetching Crossref data for {len(dois)} DOIs using batch method")
                records = client.fetch_by_dois_batch(dois, batch_size)
                logger.info(f"Successfully fetched {len(records)} records from Crossref using batch method")
            except Exception as e:
                logger.error(f"Failed to fetch Crossref data using batch method: {e}")
                records = {}
                for doi in dois:
                    records[doi] = {"doi": doi, "error": str(e)}
        elif hasattr(client, 'fetch_by_dois'):
            # Fallback к методу для нескольких DOI
            try:
                logger.debug(f"Fetching Crossref data for {len(dois)} DOIs using individual requests")
                records = client.fetch_by_dois(dois)
                logger.info(f"Successfully fetched {len(records)} records from Crossref using examine requests")
            except Exception as e:
                logger.error(f"Failed to fetch Crossref data using individual requests: {e}")
                records = {}
                for doi in dois:
                    records[doi] = {"doi": doi, "error": str(e)}
        else:
            # Fallback к одиночным запросам
            records = {}
            for doi in dois:
                try:
                    record = client.fetch_by_doi(doi)
                    records[doi] = record
                except Exception as e:
                    logger.warning(f"Failed to fetch Crossref data for DOI {doi}: {e}")
                    records[doi] = {"doi": doi, "error": str(e)}
        
        # Преобразуем в список записей с правильным маппингом
        record_list = []
        for doi, record in records.items():
            if isinstance(record, dict):
                # Клиент уже возвращает данные с правильными префиксами
                mapped_record = record
                
                # Добавляем join key для объединения
                mapped_record["doi"] = doi
                
                # Диагностическое логирование
                logger.debug(f"Mapped Crossref record columns: {list(mapped_record.keys())}")
                logger.debug(f"Sample values - doi: {mapped_record.get('crossref_doi')}, title: {mapped_record.get('crossref_title')}")
                
                record_list.append(mapped_record)
        
        # Создаем DataFrame
        if record_list:
            df = pd.DataFrame(record_list)
            # Удаляем дублированные колонки
            df = df.loc[:, ~df.columns.duplicated()]
            logger.info(f"Successfully extracted {len(df)} Crossref records")
            return df
        else:
            logger.warning("No Crossref records extracted")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Failed to extract Crossref data: {e}")
        # Возвращаем DataFrame с ошибками
        error_records = []
        for doi in dois:
            error_record = {"doi": doi, "crossref_error": str(e)}
            error_records.append(error_record)
        return pd.DataFrame(error_records)


def extract_from_openalex(client: Any, pmids: list[str], batch_size: int = 50) -> pd.DataFrame:
    """Извлечь данные из OpenAlex по списку PMID.
    
    Args:
        client: OpenAlexClient для запросов к API
        pmids: Список PubMed идентификаторов
        batch_size: Размер батча для запросов
        
    Returns:
        DataFrame с данными из OpenAlex
    """
    if not pmids:
        return pd.DataFrame()
    
    logger.info(f"Extracting OpenAlex data for {len(pmids)} PMIDs")
    
    try:
        # Используем батч-метод если доступен
        if hasattr(client, 'fetch_by_pmids_batch'):
            # Используем новый batch метод
            try:
                logger.debug(f"Fetching OpenAlex data for {len(pmids)} PMIDs using batch method")
                records = client.fetch_by_pmids_batch(pmids, batch_size)
                logger.info(f"Successfully fetched {len(records)} records from OpenAlex using batch method")
            except Exception as e:
                logger.error(f"Failed to fetch OpenAlex data using batch method: {e}")
                records = {}
                for pmid in pmids:
                    records[pmid] = {"pmid": pmid, "error": str(e)}
        else:
            # Fallback к одиночным запросам
            records = {}
            for pmid in pmids:
                try:
                    record = client.fetch_by_pmid(pmid)
                    records[pmid] = record
                except Exception as e:
                    logger.warning(f"Failed to fetch OpenAlex data for PMID {pmid}: {e}")
                    records[pmid] = {"pmid": pmid, "error": str(e)}
        
        # Преобразуем в список записей с правильным маппингом
        record_list = []
        for pmid, record in records.items():
            if isinstance(record, dict):
                # Клиент уже возвращает данные с правильными префиксами
                mapped_record = record
                
                # Добавляем join key для объединения
                mapped_record["document_pubmed_id"] = pmid
                
                # Диагностическое логирование
                logger.debug(f"Mapped OpenAlex record columns: {list(mapped_record.keys())}")
                logger.debug(f"Sample values - doi: {mapped_record.get('openalex_doi')}, pmid: {mapped_record.get('openalex_pmid')}")
                
                record_list.append(mapped_record)
        
        # Создаем DataFrame
        if record_list:
            df = pd.DataFrame(record_list)
            # Удаляем дублированные колонки
            df = df.loc[:, ~df.columns.duplicated()]
            logger.info(f"Successfully extracted {len(df)} OpenAlex records")
            return df
        else:
            logger.warning("No OpenAlex records extracted")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Failed to extract OpenAlex data: {e}")
        # Возвращаем DataFrame с ошибками
        error_records = []
        for pmid in pmids:
            error_record = {"document_pubmed_id": pmid, "openalex_error": str(e)}
            error_records.append(error_record)
        return pd.DataFrame(error_records)


def extract_from_semantic_scholar(client: Any, pmids: list[str], batch_size: int = 100, titles: dict[str, str] = None) -> pd.DataFrame:
    """Извлечь данные из Semantic Scholar по списку PMID с fallback поиском по заголовку.
    
    Args:
        client: SemanticScholarClient для запросов к API
        pmids: Список PubMed идентификаторов
        batch_size: Размер батча для запросов
        titles: Словарь маппинга PMID -> заголовок для fallback поиска
        
    Returns:
        DataFrame с данными из Semantic Scholar
    """
    if not pmids:
        return pd.DataFrame()
    
    logger.info(f"Extracting Semantic Scholar data for {len(pmids)} PMIDs")
    
    try:
        # Используем батч-метод если доступен
        if hasattr(client, 'fetch_by_pmids_batch'):
            # Используем новый batch метод
            try:
                logger.debug(f"Fetching Semantic Scholar data for {len(pmids)} PMIDs using batch method")
                records = client.fetch_by_pmids_batch(pmids, batch_size)
                logger.info(f"Successfully fetched {len(records)} records from Semantic Scholar using batch method")
            except Exception as e:
                logger.error(f"Failed to fetch Semantic Scholar data using batch method: {e}")
                records = {}
                for pmid in pmids:
                    records[pmid] = {"pmid": pmid, "error": str(e)}
        else:
            # Fallback к одиночным запросам с поддержкой поиска по заголовку
            records = {}
            for pmid in pmids:
                try:
                    # Получаем заголовок для fallback поиска
                    title = titles.get(pmid) if titles else None
                    record = client.fetch_by_pmid(pmid, title)
                    records[pmid] = record
                except Exception as e:
                    logger.warning(f"Failed to fetch Semantic Scholar data for PMID {pmid}: {e}")
                    records[pmid] = {"pmid": pmid, "error": str(e)}
        
        # Преобразуем в список записей с правильным маппингом
        record_list = []
        for pmid, record in records.items():
            if isinstance(record, dict):
                # Клиент уже возвращает данные с правильными префиксами
                mapped_record = record
                
                # Добавляем join key для объединения
                mapped_record["document_pubmed_id"] = pmid
                
                # Диагностическое логирование
                logger.debug(f"Mapped Semantic Scholar record columns: {list(mapped_record.keys())}")
                logger.debug(f"Sample values - doi: {mapped_record.get('semantic_scholar_doi')}, pmid: {mapped_record.get('semantic_scholar_pmid')}")
                
                record_list.append(mapped_record)
        
        # Создаем DataFrame
        if record_list:
            df = pd.DataFrame(record_list)
            # Удаляем дублированные колонки
            df = df.loc[:, ~df.columns.duplicated()]
            logger.info(f"Successfully extracted {len(df)} Semantic Scholar records")
            return df
        else:
            logger.warning("No Semantic Scholar records extracted")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Failed to extract Semantic Scholar data: {e}")
        # Возвращаем DataFrame с ошибками
        error_records = []
        for pmid in pmids:
            error_record = {"document_pubmed_id": pmid, "semantic_scholar_error": str(e)}
            error_records.append(error_record)
        return pd.DataFrame(error_records)


def extract_from_chembl(client: Any, chembl_ids: list[str], batch_size: int = 100) -> pd.DataFrame:
    """Извлечь данные из ChEMBL по списку document_chembl_id.
    
    Args:
        client: ChEMBLClient для запросов к API
        chembl_ids: Список ChEMBL идентификаторов документов
        batch_size: Размер батча для запросов
        
    Returns:
        DataFrame с данными из ChEMBL
    """
    if not chembl_ids:
        return pd.DataFrame()
    
    logger.info(f"Extracting ChEMBL data for {len(chembl_ids)} document IDs")
    
    try:
        # Используем батч-метод если доступен
        if hasattr(client, 'fetch_documents_batch'):
            # Обрабатываем батчами для соблюдения rate limits
            records = {}
            for i in range(0, len(chembl_ids), batch_size):
                batch = chembl_ids[i:i + batch_size]
                try:
                    batch_records = client.fetch_documents_batch(batch)
                    records.update(batch_records)
                except Exception as e:
                    logger.warning(f"Failed to fetch ChEMBL batch {i//batch_size + 1}: {e}")
                    # Создаем записи с ошибками для этого батча
                    for chembl_id in batch:
                        records[chembl_id] = {"document_chembl_id": chembl_id, "error": str(e)}
        else:
            # Fallback к одиночным запросам
            records = {}
            for chembl_id in chembl_ids:
                try:
                    record = client.fetch_by_doc_id(chembl_id)
                    records[chembl_id] = record
                except Exception as e:
                    logger.warning(f"Failed to fetch ChEMBL data for document {chembl_id}: {e}")
                    records[chembl_id] = {"document_chembl_id": chembl_id, "error": str(e)}
        
        # Преобразуем в список записей с правильным маппингом
        record_list = []
        for chembl_id, record in records.items():
            if isinstance(record, dict):
                # Клиент уже возвращает данные с правильными префиксами
                mapped_record = record
                
                # Добавляем join key для объединения
                mapped_record["document_chembl_id"] = chembl_id
                
                record_list.append(mapped_record)
        
        # Создаем DataFrame
        if record_list:
            df = pd.DataFrame(record_list)
            # Удаляем дублированные колонки
            df = df.loc[:, ~df.columns.duplicated()]
            logger.info(f"Successfully extracted {len(df)} ChEMBL records")
            return df
        else:
            logger.warning("No ChEMBL records extracted")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Failed to extract ChEMBL data: {e}")
        # Возвращаем DataFrame с ошибками
        error_records = []
        for chembl_id in chembl_ids:
            error_record = {"document_chembl_id": chembl_id, "chembl_error": str(e)}
            error_records.append(error_record)
        return pd.DataFrame(error_records)