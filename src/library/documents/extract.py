"""Модуль для извлечения данных документов из внешних источников."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from library.documents.column_mapping import apply_field_mapping

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
    
    try:
        # Используем батч-метод если доступен
        if hasattr(client, 'fetch_by_pmids'):
            # Обрабатываем батчами для соблюдения rate limits
            records = {}
            for i in range(0, len(pmids), batch_size):
                batch = pmids[i:i + batch_size]
                try:
                    batch_records = client.fetch_by_pmids(batch)
                    records.update(batch_records)
                except Exception as e:
                    logger.warning(f"Failed to fetch PubMed batch {i//batch_size + 1}: {e}")
                    # Создаем записи с ошибками для этого батча
                    for pmid in batch:
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
                # Применяем маппинг полей
                mapped_record = apply_field_mapping(record, "pubmed")
                
                # Добавляем join key для объединения
                mapped_record["document_pubmed_id"] = pmid
                
                record_list.append(mapped_record)
        
        # Создаем DataFrame
        if record_list:
            df = pd.DataFrame(record_list)
            # Удаляем дублированные колонки
            df = df.loc[:, ~df.columns.duplicated()]
            logger.info(f"Successfully extracted {len(df)} PubMed records")
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
            # Обрабатываем батчами для соблюдения rate limits
            records = {}
            for i in range(0, len(dois), batch_size):
                batch = dois[i:i + batch_size]
                try:
                    batch_records = client.fetch_by_dois_batch(batch)
                    records.update(batch_records)
                except Exception as e:
                    logger.warning(f"Failed to fetch Crossref batch {i//batch_size + 1}: {e}")
                    # Создаем записи с ошибками для этого батча
                    for doi in batch:
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
                # Применяем маппинг полей
                mapped_record = apply_field_mapping(record, "crossref")
                
                # Добавляем join key для объединения
                mapped_record["doi"] = doi
                
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
            # Обрабатываем батчами для соблюдения rate limits
            records = {}
            for i in range(0, len(pmids), batch_size):
                batch = pmids[i:i + batch_size]
                try:
                    batch_records = client.fetch_by_pmids_batch(batch)
                    records.update(batch_records)
                except Exception as e:
                    logger.warning(f"Failed to fetch OpenAlex batch {i//batch_size + 1}: {e}")
                    # Создаем записи с ошибками для этого батча
                    for pmid in batch:
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
                # Применяем маппинг полей
                mapped_record = apply_field_mapping(record, "openalex")
                
                # Добавляем join key для объединения
                mapped_record["document_pubmed_id"] = pmid
                
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


def extract_from_semantic_scholar(client: Any, pmids: list[str], batch_size: int = 100) -> pd.DataFrame:
    """Извлечь данные из Semantic Scholar по списку PMID.
    
    Args:
        client: SemanticScholarClient для запросов к API
        pmids: Список PubMed идентификаторов
        batch_size: Размер батча для запросов
        
    Returns:
        DataFrame с данными из Semantic Scholar
    """
    if not pmids:
        return pd.DataFrame()
    
    logger.info(f"Extracting Semantic Scholar data for {len(pmids)} PMIDs")
    
    try:
        # Используем батч-метод если доступен
        if hasattr(client, 'fetch_by_pmids_batch'):
            # Обрабатываем батчами для соблюдения rate limits
            records = {}
            for i in range(0, len(pmids), batch_size):
                batch = pmids[i:i + batch_size]
                try:
                    batch_records = client.fetch_by_pmids_batch(batch)
                    records.update(batch_records)
                except Exception as e:
                    logger.warning(f"Failed to fetch Semantic Scholar batch {i//batch_size + 1}: {e}")
                    # Создаем записи с ошибками для этого батча
                    for pmid in batch:
                        records[pmid] = {"pmid": pmid, "error": str(e)}
        else:
            # Fallback к одиночным запросам
            records = {}
            for pmid in pmids:
                try:
                    record = client.fetch_by_pmid(pmid)
                    records[pmid] = record
                except Exception as e:
                    logger.warning(f"Failed to fetch Semantic Scholar data for PMID {pmid}: {e}")
                    records[pmid] = {"pmid": pmid, "error": str(e)}
        
        # Преобразуем в список записей с правильным маппингом
        record_list = []
        for pmid, record in records.items():
            if isinstance(record, dict):
                # Применяем маппинг полей
                mapped_record = apply_field_mapping(record, "semantic_scholar")
                
                # Добавляем join key для объединения
                mapped_record["document_pubmed_id"] = pmid
                
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
                # Применяем маппинг полей
                mapped_record = apply_field_mapping(record, "chembl")
                
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