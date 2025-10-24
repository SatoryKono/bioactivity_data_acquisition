"""Data normalization for document records."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from library.normalizers import get_normalizer
from library.schemas.document_schema_normalized import DocumentNormalizedSchema
from library.utils.empty_value_handler import (
    is_empty_value,
    normalize_dict_field,
    normalize_list_field,
    normalize_numeric_field,
    normalize_string_field,
)

logger = logging.getLogger(__name__)


class DocumentNormalizer:
    """Normalizes document data according to business rules."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize normalizer with configuration."""
        self.config = config or {}

    def normalize_documents(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализация данных документов согласно бизнес-правилам.
        
        Преобразует сырые данные документов в нормализованный формат,
        добавляя вычисляемые поля и нормализуя значения.
        
        Args:
            df: DataFrame с валидированными сырыми данными документов
            
        Returns:
            pd.DataFrame: Нормализованный DataFrame с дополнительными полями:
                - publication_date: нормализованная дата публикации
                - document_sortorder: поле для сортировки документов
                - нормализованные поля: journal, DOI, PMID, title, authors
                
        Example:
            >>> normalizer = DocumentNormalizer(config)
            >>> normalized_df = normalizer.normalize_documents(validated_df)
        """
        logger.info(f"Normalizing {len(df)} document records")
        
        # Create a copy to avoid modifying original
        normalized_df = df.copy()
        
        # Apply schema-based normalization first
        normalized_df = self._apply_schema_normalizations(normalized_df)
        
        # Step 1: Initialize all possible columns with default values
        normalized_df = self._initialize_all_columns(normalized_df)
        
        # Step 2: Normalize field types and values
        normalized_df = self._normalize_field_types(normalized_df)
        
        # Step 3: Add computed fields
        normalized_df = self._add_publication_date_column(normalized_df)
        normalized_df = self._add_document_sortorder_column(normalized_df)
        
        logger.info(f"Normalization completed. Output: {len(normalized_df)} records")
        return normalized_df

    def _initialize_all_columns(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Initialize all possible output columns with default values."""
        
        # Define all possible columns that should exist in the output
        all_columns = {
            # Original ChEMBL fields
            "document_chembl_id", "title", "doi", "document_pubmed_id", "chembl_doc_type", "journal", "year",
            # Legacy ChEMBL fields
            "abstract", "pubmed_authors", "document_classification", "referenses_on_previous_experiments",
            "first_page", "original_experimental_document", "issue", "last_page", "month", "volume",
            # Enriched fields from external sources
            "crossref_doi", "crossref_title", "crossref_doc_type", "crossref_subject", "crossref_error",
            "openalex_doi", "openalex_title", "openalex_type", "openalex_concepts", "openalex_error",
            "pubmed_doi", "pubmed_title", "pubmed_abstract", "pubmed_journal", 
            "pubmed_issn", "pubmed_volume", "pubmed_issue", "pubmed_pages", "pubmed_year", "pubmed_month",
            "pubmed_day", "pubmed_pmcid", "pubmed_error",
            "semantic_scholar_doi", "semantic_scholar_title", "semantic_scholar_abstract", 
            "semantic_scholar_authors", "semantic_scholar_venue", "semantic_scholar_year", 
            "semantic_scholar_citation_count", "semantic_scholar_error",
            # ChEMBL enriched fields
            "chembl_title", "chembl_doi", "chembl_pmid", "chembl_journal", "chembl_year", 
            "chembl_volume", "chembl_issue",
            # Computed fields
            "publication_date", "document_sortorder", "citation"
        }
        
        # Add missing columns with default values
        for column in all_columns:
            if column not in frame.columns:
                frame[column] = pd.NA
        
        return frame

    def _normalize_field_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize field types and values using empty_value_handler utilities."""
        logger.debug("Normalizing field types and values")
        
        normalized_df = df.copy()
        
        # String fields
        string_fields = [
            "document_chembl_id", "title", "doi", "document_pubmed_id", "chembl_doc_type", 
            "journal", "abstract", "pubmed_authors", "document_classification",
            "first_page", "last_page", "issue", "volume", "month",
            "crossref_doi", "crossref_title", "crossref_doc_type", "crossref_subject", "crossref_error",
            "openalex_doi", "openalex_title", "openalex_type", "openalex_error",
            "pubmed_doi", "pubmed_title", "pubmed_abstract", "pubmed_authors", "pubmed_journal",
            "pubmed_issn", "pubmed_volume", "pubmed_issue", "pubmed_pages", "pubmed_pmcid", "pubmed_error",
            "semantic_scholar_doi", "semantic_scholar_title", "semantic_scholar_abstract",
            "semantic_scholar_authors", "semantic_scholar_venue", "semantic_scholar_error",
            "chembl_title", "chembl_doi", "chembl_pmid", "chembl_journal", "chembl_volume", "chembl_issue",
            "citation", "document_sortorder"
        ]
        
        for field in string_fields:
            if field in normalized_df.columns:
                normalized_df[field] = normalized_df[field].apply(normalize_string_field)
        
        # Numeric fields
        numeric_fields = [
            "year", "pubmed_year", "pubmed_month", "pubmed_day", "semantic_scholar_year",
            "semantic_scholar_citation_count", "chembl_year"
        ]
        
        for field in numeric_fields:
            if field in normalized_df.columns:
                normalized_df[field] = normalized_df[field].apply(normalize_numeric_field)
        
        # List fields
        list_fields = [
            "pubmed_authors", "semantic_scholar_authors", "openalex_concepts"
        ]
        
        for field in list_fields:
            if field in normalized_df.columns:
                normalized_df[field] = normalized_df[field].apply(normalize_list_field)
        
        # Dict fields
        dict_fields = [
            "crossref_subject", "openalex_concepts"
        ]
        
        for field in dict_fields:
            if field in normalized_df.columns:
                normalized_df[field] = normalized_df[field].apply(normalize_dict_field)
        
        # Boolean fields
        boolean_fields = [
            "referenses_on_previous_experiments", "original_experimental_document"
        ]
        
        for field in boolean_fields:
            if field in normalized_df.columns:
                # Применяем normalize_boolean из normalizers
                from library.normalizers.boolean_normalizers import normalize_boolean
                normalized_df[field] = normalized_df[field].apply(normalize_boolean)
                # Убеждаемся, что колонка имеет правильный dtype
                # Заменяем None на False и приводим к bool для совместимости с pandera
                normalized_df[field] = normalized_df[field].fillna(False).astype('bool')
        
        # Map pubmed_year_completed to pubmed_year
        if "pubmed_year_completed" in normalized_df.columns:
            normalized_df["pubmed_year"] = normalized_df["pubmed_year_completed"]
        
        return normalized_df

    def _add_publication_date_column(self, frame: pd.DataFrame) -> pd.DataFrame:
        """
        Добавляет колонку publication_date в DataFrame на основе полей PubMed.
        
        Args:
            frame: DataFrame с данными документов
            
        Returns:
            pd.DataFrame: DataFrame с добавленной колонкой publication_date
        """
        frame = frame.copy()
        
        # Применяем функцию определения даты публикации к каждой строке
        frame["publication_date"] = frame.apply(self._determine_publication_date, axis=1)
        
        return frame

    def _determine_publication_date(self, row: pd.Series) -> str | None:
        """
        Определяет дату публикации на основе полей PubMed.
        
        Args:
            row: Строка DataFrame с данными документа
            
        Returns:
            str | None: Дата в формате YYYY-MM-DD или None если дата не определена
        """
        # Приоритет: pubmed_year, pubmed_month, pubmed_day
        year = row.get("pubmed_year")
        month = row.get("pubmed_month")
        day = row.get("pubmed_day")
        
        # Если нет данных PubMed, пробуем общие поля
        if is_empty_value(year):
            year = row.get("year")
        if is_empty_value(month):
            month = row.get("month")
        
        # Нормализуем значения
        year = normalize_numeric_field(year)
        month = normalize_numeric_field(month)
        day = normalize_numeric_field(day)
        
        if is_empty_value(year):
            return None
        
        # Формируем дату
        try:
            # Безопасное преобразование year в int
            if is_empty_value(year):
                return None
            year_val = year if not is_empty_value(year) else None
            if year_val is None:
                return None
            year_int = int(float(year_val))  # Сначала в float, потом в int
            if year_int < 1900 or year_int > 2030:  # Разумные границы
                return None
            
            # Если есть месяц и день
            if not is_empty_value(month) and not is_empty_value(day):
                try:
                    month_val = month if not is_empty_value(month) else None
                    day_val = day if not is_empty_value(day) else None
                    if month_val is not None and day_val is not None:
                        month_int = int(float(month_val))
                        day_int = int(float(day_val))
                        if 1 <= month_int <= 12 and 1 <= day_int <= 31:
                            return f"{year_int:04d}-{month_int:02d}-{day_int:02d}"
                except (ValueError, TypeError):
                    pass
            
            # Если есть только месяц
            if not is_empty_value(month):
                try:
                    month_val = month if not is_empty_value(month) else None
                    if month_val is not None:
                        month_int = int(float(month_val))
                        if 1 <= month_int <= 12:
                            return f"{year_int:04d}-{month_int:02d}-01"
                except (ValueError, TypeError):
                    pass
            
            # Если есть только год
            return f"{year_int:04d}-01-01"
            
        except (ValueError, TypeError):
            return None

    def _add_document_sortorder_column(self, frame: pd.DataFrame) -> pd.DataFrame:
        """
        Добавляет колонку document_sortorder в DataFrame на основе pubmed_issn, publication_date и index.
        
        Args:
            frame: DataFrame с данными документов
            
        Returns:
            pd.DataFrame: DataFrame с добавленной колонкой document_sortorder
        """
        frame = frame.copy()
        
        # Применяем функцию определения порядка сортировки к каждой строке
        frame["document_sortorder"] = frame.apply(self._determine_document_sortorder, axis=1)
        
        return frame

    def _determine_document_sortorder(self, row: pd.Series) -> str:
        """
        Определяет порядок сортировки документов на основе pubmed_issn, publication_date и index.
        
        Логика определения:
        1. Каждый параметр приводится к строковому типу
        2. index дополняется символом "0" до общей длины в 6 символов
        3. Полученные строки склеиваются используя ":" как разделитель
        
        Args:
            row: Строка DataFrame с данными документа
            
        Returns:
            str: Строка для сортировки в формате "issn:date:index"
        """
        # Получаем значения полей
        issn = row.get("pubmed_issn", "")
        date = row.get("publication_date", "")
        index = row.get("index", "")
        
        # Нормализуем значения
        issn_str = str(issn).strip() if not is_empty_value(issn) else ""
        date_str = str(date).strip() if not is_empty_value(date) else ""
        index_str = str(index).strip() if not is_empty_value(index) else ""
        
        # Дополняем index нулями до 6 символов
        if index_str:
            try:
                index_num = int(index_str)
                index_padded = f"{index_num:06d}"
            except (ValueError, TypeError):
                index_padded = index_str.zfill(6)
        else:
            index_padded = "000000"
        
        # Формируем строку сортировки
        sortorder_parts = [issn_str, date_str, index_padded]
        return ":".join(sortorder_parts)
    
    def _apply_schema_normalizations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Применяет функции нормализации из схемы к DataFrame.
        
        Args:
            df: DataFrame для нормализации
            
        Returns:
            DataFrame с примененными нормализациями
        """
        logger.info("Applying schema-based normalizations")
        
        # Получаем схему
        schema = DocumentNormalizedSchema.get_schema()
        
        # Применяем нормализацию к каждой колонке
        for column_name, column_schema in schema.columns.items():
            if column_name in df.columns:
                norm_funcs = column_schema.metadata.get("normalization_functions", [])
                if norm_funcs:
                    logger.debug(f"Normalizing column '{column_name}' with functions: {norm_funcs}")
                    
                    # Применяем функции нормализации в порядке
                    for func_name in norm_funcs:
                        try:
                            func = get_normalizer(func_name)
                            df[column_name] = df[column_name].apply(func)
                        except Exception as e:
                            logger.warning(f"Failed to apply normalizer '{func_name}' to column '{column_name}': {e}")
        
        # Добавляем системные метаданные
        df = self._add_system_metadata(df)
        
        return df
    
    def _add_system_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Добавить системные метаданные к DataFrame."""
        import hashlib
        from datetime import datetime
        
        logger.info("Adding system metadata to document records")
        
        # Index - порядковый номер записи
        df['index'] = range(len(df))
        
        # Pipeline version - из конфига
        df['pipeline_version'] = self.config.get('pipeline', {}).get('version', '2.0.0')
        
        # Source system
        df['source_system'] = 'ChEMBL'
        
        # ChEMBL release - из retrieved_at или текущее время
        df['chembl_release'] = None  # Будет заполнено из API если доступно
        
        # Extracted at - текущее время
        df['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
        
        # Hash row - SHA256 хеш всей строки
        df['hash_row'] = df.apply(lambda row: self._calculate_row_hash(row), axis=1)
        
        # Hash business key - SHA256 хеш бизнес-ключа
        df['hash_business_key'] = df['document_chembl_id'].apply(
            lambda x: hashlib.sha256(str(x).encode('utf-8')).hexdigest() if pd.notna(x) else None
        )
        
        return df
    
    def _calculate_row_hash(self, row: pd.Series) -> str:
        """Calculate SHA256 hash of a DataFrame row."""
        import hashlib
        
        # Создать строку из всех значений строки
        row_string = '|'.join([str(val) if pd.notna(val) else '' for val in row.values])
        
        # Вычислить SHA256 хеш
        return hashlib.sha256(row_string.encode('utf-8')).hexdigest()
