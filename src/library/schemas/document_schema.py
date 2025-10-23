"""
Pandera схемы для валидации данных документов.

Предоставляет схемы для входных, сырых и нормализованных данных документов.
"""

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema


class DocumentInputSchema:
    """Схемы для входных данных документов."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для входных данных документов."""
        return DataFrameSchema({
            "document_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL document ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID документа"
            ),
            "doi": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^10\.\d+/[^\s]+$', error="Invalid DOI format"),
                ],
                nullable=True,
                description="DOI документа",
                metadata={
                    "normalization_functions": [
                        "normalize_string_strip",
                        "normalize_string_lower",
                        "normalize_doi"
                    ]
                }
            ),
            "pmid": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^\d+$', error="Invalid PMID format"),
                ],
                nullable=True,
                description="PubMed ID",
                metadata={
                    "normalization_functions": [
                        "normalize_string_strip",
                        "normalize_pmid"
                    ]
                }
            ),
            "title": Column(
                pa.String,
                checks=[
                    Check.str_length(min_value=1, max_value=1000, error="Title length must be 1-1000 characters"),
                ],
                nullable=True,
                description="Название документа"
            )
        })


class DocumentRawSchema:
    """Схемы для сырых данных документов из API."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для сырых данных документов."""
        return DataFrameSchema({
            # Основные поля
            "document_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL document ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID документа"
            ),
            "document_pubmed_id": Column(pa.String, nullable=True, description="PubMed ID из ChEMBL"),
            "document_classification": Column(pa.String, nullable=True, description="Классификация документа"),
            "referenses_on_previous_experiments": Column(pa.Bool, nullable=True, description="Ссылки на предыдущие эксперименты"),
            "original_experimental_document": Column(pa.Bool, nullable=True, description="Оригинальный экспериментальный документ"),
            "retrieved_at": Column(
                pa.DateTime,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Время получения данных"
            ),
            
            # ChEMBL поля
            "chembl_pmid": Column(pa.String, nullable=True, description="PMID из ChEMBL"),
            "chembl_title": Column(pa.String, nullable=True, description="Название из ChEMBL"),
            "chembl_abstract": Column(pa.String, nullable=True, description="Аннотация из ChEMBL"),
            "chembl_authors": Column(pa.String, nullable=True, description="Авторы из ChEMBL"),
            "chembl_doi": Column(pa.String, nullable=True, description="DOI из ChEMBL"),
            "chembl_doc_type": Column(pa.String, nullable=True, description="Тип документа из ChEMBL"),
            "chembl_issn": Column(pa.String, nullable=True, description="ISSN из ChEMBL"),
            "chembl_journal": Column(pa.String, nullable=True, description="Журнал из ChEMBL"),
            "chembl_year": Column(pa.Int, nullable=True, description="Год из ChEMBL"),
            "chembl_volume": Column(pa.String, nullable=True, description="Том из ChEMBL"),
            "chembl_issue": Column(pa.String, nullable=True, description="Номер выпуска из ChEMBL"),
            "chembl_first_page": Column(pa.String, nullable=True, description="Первая страница из ChEMBL"),
            "chembl_last_page": Column(pa.String, nullable=True, description="Последняя страница из ChEMBL"),
            "chembl_error": Column(pa.String, nullable=True, description="Ошибка из ChEMBL"),
            
            # Crossref поля
            "crossref_pmid": Column(pa.String, nullable=True, description="PMID из Crossref"),
            "crossref_title": Column(pa.String, nullable=True, description="Название из Crossref"),
            "crossref_abstract": Column(pa.String, nullable=True, description="Аннотация из Crossref"),
            "crossref_authors": Column(pa.String, nullable=True, description="Авторы из Crossref"),
            "crossref_doi": Column(pa.String, nullable=True, description="DOI из Crossref"),
            "crossref_doc_type": Column(pa.String, nullable=True, description="Тип документа из Crossref"),
            "crossref_issn": Column(pa.String, nullable=True, description="ISSN из Crossref"),
            "crossref_journal": Column(pa.String, nullable=True, description="Журнал из Crossref"),
            "crossref_year": Column(pa.Int, nullable=True, description="Год из Crossref"),
            "crossref_volume": Column(pa.String, nullable=True, description="Том из Crossref"),
            "crossref_issue": Column(pa.String, nullable=True, description="Номер выпуска из Crossref"),
            "crossref_first_page": Column(pa.String, nullable=True, description="Первая страница из Crossref"),
            "crossref_last_page": Column(pa.String, nullable=True, description="Последняя страница из Crossref"),
            "crossref_subject": Column(pa.String, nullable=True, description="Предметная область из Crossref"),
            "crossref_error": Column(pa.String, nullable=True, description="Ошибка из Crossref"),
            
            # OpenAlex поля
            "openalex_pmid": Column(pa.String, nullable=True, description="PMID из OpenAlex"),
            "openalex_title": Column(pa.String, nullable=True, description="Название из OpenAlex"),
            "openalex_abstract": Column(pa.String, nullable=True, description="Аннотация из OpenAlex"),
            "openalex_authors": Column(pa.String, nullable=True, description="Авторы из OpenAlex"),
            "openalex_doi": Column(pa.String, nullable=True, description="DOI из OpenAlex"),
            "openalex_doc_type": Column(pa.String, nullable=True, description="Тип документа из OpenAlex"),
            "openalex_crossref_doc_type": Column(pa.String, nullable=True, description="Тип документа из OpenAlex Crossref"),
            "openalex_issn": Column(pa.String, nullable=True, description="ISSN из OpenAlex"),
            "openalex_journal": Column(pa.String, nullable=True, description="Журнал из OpenAlex"),
            "openalex_year": Column(pa.Int, nullable=True, description="Год из OpenAlex"),
            "openalex_volume": Column(pa.String, nullable=True, description="Том из OpenAlex"),
            "openalex_issue": Column(pa.String, nullable=True, description="Номер выпуска из OpenAlex"),
            "openalex_first_page": Column(pa.String, nullable=True, description="Первая страница из OpenAlex"),
            "openalex_last_page": Column(pa.String, nullable=True, description="Последняя страница из OpenAlex"),
            "openalex_concepts": Column(pa.String, nullable=True, description="Концепты из OpenAlex"),
            "openalex_error": Column(pa.String, nullable=True, description="Ошибка из OpenAlex"),
            
            # PubMed поля
            "pubmed_pmid": Column(pa.String, nullable=True, description="PMID из PubMed"),
            "pubmed_article_title": Column(pa.String, nullable=True, description="Название статьи из PubMed"),
            "pubmed_abstract": Column(pa.String, nullable=True, description="Аннотация из PubMed"),
            "pubmed_authors": Column(pa.String, nullable=True, description="Авторы из PubMed"),
            "pubmed_doi": Column(pa.String, nullable=True, description="DOI из PubMed"),
            "pubmed_doc_type": Column(pa.String, nullable=True, description="Тип публикации из PubMed"),
            "pubmed_issn": Column(pa.String, nullable=True, description="ISSN из PubMed"),
            "pubmed_journal": Column(pa.String, nullable=True, description="Журнал из PubMed"),
            "pubmed_year": Column(pa.Int, nullable=True, description="Год из PubMed"),
            "pubmed_volume": Column(pa.String, nullable=True, description="Том из PubMed"),
            "pubmed_issue": Column(pa.String, nullable=True, description="Номер выпуска из PubMed"),
            "pubmed_first_page": Column(pa.String, nullable=True, description="Начальная страница из PubMed"),
            "pubmed_last_page": Column(pa.String, nullable=True, description="Конечная страница из PubMed"),
            "pubmed_mesh_descriptors": Column(pa.String, nullable=True, description="MeSH дескрипторы из PubMed"),
            "pubmed_mesh_qualifiers": Column(pa.String, nullable=True, description="MeSH квалификаторы из PubMed"),
            "pubmed_chemical_list": Column(pa.String, nullable=True, description="Химические вещества из PubMed"),
            "pubmed_year_completed": Column(pa.Int, nullable=True, description="Год завершения из PubMed"),
            "pubmed_month_completed": Column(pa.Int, nullable=True, description="Месяц завершения из PubMed"),
            "pubmed_day_completed": Column(pa.Int, nullable=True, description="День завершения из PubMed"),
            "pubmed_year_revised": Column(pa.Int, nullable=True, description="Год пересмотра из PubMed"),
            "pubmed_month_revised": Column(pa.Int, nullable=True, description="Месяц пересмотра из PubMed"),
            "pubmed_day_revised": Column(pa.Int, nullable=True, description="День пересмотра из PubMed"),
            "pubmed_pages": Column(pa.String, nullable=True, description="Страницы из PubMed"),
            "pubmed_pmcid": Column(pa.String, nullable=True, description="PMC ID из PubMed"),
            "pubmed_day": Column(pa.Int, nullable=True, description="День из PubMed"),
            "pubmed_month": Column(pa.Int, nullable=True, description="Месяц из PubMed"),
            "pubmed_error": Column(pa.String, nullable=True, description="Ошибка из PubMed"),
            
            # Semantic Scholar поля
            "semantic_scholar_pmid": Column(pa.String, nullable=True, description="PMID из Semantic Scholar"),
            "semantic_scholar_title": Column(pa.String, nullable=True, description="Название из Semantic Scholar"),
            "semantic_scholar_authors": Column(pa.String, nullable=True, description="Авторы из Semantic Scholar"),
            "semantic_scholar_doi": Column(pa.String, nullable=True, description="DOI из Semantic Scholar"),
            "semantic_scholar_doc_type": Column(pa.String, nullable=True, description="Типы публикации из Semantic Scholar"),
            "semantic_scholar_issn": Column(pa.String, nullable=True, description="ISSN из Semantic Scholar"),
            "semantic_scholar_journal": Column(pa.String, nullable=True, description="Журнал из Semantic Scholar"),
            "semantic_scholar_abstract": Column(pa.String, nullable=True, description="Аннотация из Semantic Scholar"),
            "semantic_scholar_citation_count": Column(pa.Int, nullable=True, description="Количество цитирований из Semantic Scholar"),
            "semantic_scholar_venue": Column(pa.String, nullable=True, description="Площадка публикации из Semantic Scholar"),
            "semantic_scholar_year": Column(pa.Int, nullable=True, description="Год из Semantic Scholar"),
            "semantic_scholar_error": Column(pa.String, nullable=True, description="Ошибка из Semantic Scholar"),
        })


class DocumentNormalizedSchema:
    """Схемы для нормализованных данных документов."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных документов."""
        return DataFrameSchema({
            # Основные поля
            "document_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL document ID format"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="ChEMBL ID документа"
            ),
            "document_pubmed_id": Column(pa.String, nullable=True, description="PubMed ID из ChEMBL"),
            "document_classification": Column(pa.String, nullable=True, description="Классификация документа"),
            "referenses_on_previous_experiments": Column(pa.Bool, nullable=True, description="Ссылки на предыдущие эксперименты"),
            "original_experimental_document": Column(pa.Bool, nullable=True, description="Оригинальный экспериментальный документ"),
            "retrieved_at": Column(
                pa.DateTime,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Время получения данных"
            ),
            
            # Консолидированные поля
            "document_citation": Column(pa.String, nullable=True, description="Форматированная литературная ссылка"),
            "publication_date": Column(pa.Date, nullable=True, description="Дата публикации"),
            "document_sortorder": Column(
                pa.Int,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Sort order must be >= 0"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="Порядок сортировки"
            ),
            
            # Консолидированные поля из всех источников
            "doi": Column(pa.String, nullable=True, description="Консолидированный DOI"),
            "title": Column(pa.String, nullable=True, description="Консолидированное название"),
            "abstract": Column(pa.String, nullable=True, description="Консолидированная аннотация"),
            "journal": Column(pa.String, nullable=True, description="Консолидированный журнал"),
            "year": Column(pa.Int, nullable=True, description="Консолидированный год"),
            "volume": Column(pa.String, nullable=True, description="Консолидированный том"),
            "issue": Column(pa.String, nullable=True, description="Консолидированный номер"),
            "first_page": Column(pa.String, nullable=True, description="Консолидированная первая страница"),
            "last_page": Column(pa.String, nullable=True, description="Консолидированная последняя страница"),
            "month": Column(pa.Int, nullable=True, description="Месяц публикации"),
            
            # ChEMBL поля
            "chembl_pmid": Column(pa.String, nullable=True, description="PMID из ChEMBL"),
            "chembl_title": Column(pa.String, nullable=True, description="Название из ChEMBL"),
            "chembl_abstract": Column(pa.String, nullable=True, description="Аннотация из ChEMBL"),
            "chembl_authors": Column(pa.String, nullable=True, description="Авторы из ChEMBL"),
            "chembl_doi": Column(pa.String, nullable=True, description="DOI из ChEMBL"),
            "chembl_doc_type": Column(pa.String, nullable=True, description="Тип документа из ChEMBL"),
            "chembl_issn": Column(pa.String, nullable=True, description="ISSN из ChEMBL"),
            "chembl_journal": Column(pa.String, nullable=True, description="Журнал из ChEMBL"),
            "chembl_year": Column(pa.Int, nullable=True, description="Год из ChEMBL"),
            "chembl_volume": Column(pa.String, nullable=True, description="Том из ChEMBL"),
            "chembl_issue": Column(pa.String, nullable=True, description="Номер выпуска из ChEMBL"),
            "chembl_first_page": Column(pa.String, nullable=True, description="Первая страница из ChEMBL"),
            "chembl_last_page": Column(pa.String, nullable=True, description="Последняя страница из ChEMBL"),
            "chembl_error": Column(pa.String, nullable=True, description="Ошибка из ChEMBL"),
            
            # Crossref поля
            "crossref_pmid": Column(pa.String, nullable=True, description="PMID из Crossref"),
            "crossref_title": Column(pa.String, nullable=True, description="Название из Crossref"),
            "crossref_abstract": Column(pa.String, nullable=True, description="Аннотация из Crossref"),
            "crossref_authors": Column(pa.String, nullable=True, description="Авторы из Crossref"),
            "crossref_doi": Column(pa.String, nullable=True, description="DOI из Crossref"),
            "crossref_doc_type": Column(pa.String, nullable=True, description="Тип документа из Crossref"),
            "crossref_issn": Column(pa.String, nullable=True, description="ISSN из Crossref"),
            "crossref_journal": Column(pa.String, nullable=True, description="Журнал из Crossref"),
            "crossref_year": Column(pa.Int, nullable=True, description="Год из Crossref"),
            "crossref_volume": Column(pa.String, nullable=True, description="Том из Crossref"),
            "crossref_issue": Column(pa.String, nullable=True, description="Номер выпуска из Crossref"),
            "crossref_first_page": Column(pa.String, nullable=True, description="Первая страница из Crossref"),
            "crossref_last_page": Column(pa.String, nullable=True, description="Последняя страница из Crossref"),
            "crossref_subject": Column(pa.String, nullable=True, description="Предметная область из Crossref"),
            "crossref_error": Column(pa.String, nullable=True, description="Ошибка из Crossref"),
            
            # OpenAlex поля
            "openalex_pmid": Column(pa.String, nullable=True, description="PMID из OpenAlex"),
            "openalex_title": Column(pa.String, nullable=True, description="Название из OpenAlex"),
            "openalex_abstract": Column(pa.String, nullable=True, description="Аннотация из OpenAlex"),
            "openalex_authors": Column(pa.String, nullable=True, description="Авторы из OpenAlex"),
            "openalex_doi": Column(pa.String, nullable=True, description="DOI из OpenAlex"),
            "openalex_doc_type": Column(pa.String, nullable=True, description="Тип документа из OpenAlex"),
            "openalex_crossref_doc_type": Column(pa.String, nullable=True, description="Тип документа из OpenAlex Crossref"),
            "openalex_issn": Column(pa.String, nullable=True, description="ISSN из OpenAlex"),
            "openalex_journal": Column(pa.String, nullable=True, description="Журнал из OpenAlex"),
            "openalex_year": Column(pa.Int, nullable=True, description="Год из OpenAlex"),
            "openalex_volume": Column(pa.String, nullable=True, description="Том из OpenAlex"),
            "openalex_issue": Column(pa.String, nullable=True, description="Номер выпуска из OpenAlex"),
            "openalex_first_page": Column(pa.String, nullable=True, description="Первая страница из OpenAlex"),
            "openalex_last_page": Column(pa.String, nullable=True, description="Последняя страница из OpenAlex"),
            "openalex_concepts": Column(pa.String, nullable=True, description="Концепты из OpenAlex"),
            "openalex_error": Column(pa.String, nullable=True, description="Ошибка из OpenAlex"),
            
            # PubMed поля
            "pubmed_pmid": Column(pa.String, nullable=True, description="PMID из PubMed"),
            "pubmed_article_title": Column(pa.String, nullable=True, description="Название статьи из PubMed"),
            "pubmed_abstract": Column(pa.String, nullable=True, description="Аннотация из PubMed"),
            "pubmed_authors": Column(pa.String, nullable=True, description="Авторы из PubMed"),
            "pubmed_doi": Column(pa.String, nullable=True, description="DOI из PubMed"),
            "pubmed_doc_type": Column(pa.String, nullable=True, description="Тип публикации из PubMed"),
            "pubmed_issn": Column(pa.String, nullable=True, description="ISSN из PubMed"),
            "pubmed_journal": Column(pa.String, nullable=True, description="Журнал из PubMed"),
            "pubmed_year": Column(pa.Int, nullable=True, description="Год из PubMed"),
            "pubmed_volume": Column(pa.String, nullable=True, description="Том из PubMed"),
            "pubmed_issue": Column(pa.String, nullable=True, description="Номер выпуска из PubMed"),
            "pubmed_first_page": Column(pa.String, nullable=True, description="Начальная страница из PubMed"),
            "pubmed_last_page": Column(pa.String, nullable=True, description="Конечная страница из PubMed"),
            "pubmed_mesh_descriptors": Column(pa.String, nullable=True, description="MeSH дескрипторы из PubMed"),
            "pubmed_mesh_qualifiers": Column(pa.String, nullable=True, description="MeSH квалификаторы из PubMed"),
            "pubmed_chemical_list": Column(pa.String, nullable=True, description="Химические вещества из PubMed"),
            "pubmed_year_completed": Column(pa.Int, nullable=True, description="Год завершения из PubMed"),
            "pubmed_month_completed": Column(pa.Int, nullable=True, description="Месяц завершения из PubMed"),
            "pubmed_day_completed": Column(pa.Int, nullable=True, description="День завершения из PubMed"),
            "pubmed_year_revised": Column(pa.Int, nullable=True, description="Год пересмотра из PubMed"),
            "pubmed_month_revised": Column(pa.Int, nullable=True, description="Месяц пересмотра из PubMed"),
            "pubmed_day_revised": Column(pa.Int, nullable=True, description="День пересмотра из PubMed"),
            "pubmed_pages": Column(pa.String, nullable=True, description="Страницы из PubMed"),
            "pubmed_pmcid": Column(pa.String, nullable=True, description="PMC ID из PubMed"),
            "pubmed_day": Column(pa.Int, nullable=True, description="День из PubMed"),
            "pubmed_month": Column(pa.Int, nullable=True, description="Месяц из PubMed"),
            "pubmed_error": Column(pa.String, nullable=True, description="Ошибка из PubMed"),
            
            # Semantic Scholar поля
            "semantic_scholar_pmid": Column(pa.String, nullable=True, description="PMID из Semantic Scholar"),
            "semantic_scholar_title": Column(pa.String, nullable=True, description="Название из Semantic Scholar"),
            "semantic_scholar_authors": Column(pa.String, nullable=True, description="Авторы из Semantic Scholar"),
            "semantic_scholar_doi": Column(pa.String, nullable=True, description="DOI из Semantic Scholar"),
            "semantic_scholar_doc_type": Column(pa.String, nullable=True, description="Типы публикации из Semantic Scholar"),
            "semantic_scholar_issn": Column(pa.String, nullable=True, description="ISSN из Semantic Scholar"),
            "semantic_scholar_journal": Column(pa.String, nullable=True, description="Журнал из Semantic Scholar"),
            "semantic_scholar_abstract": Column(pa.String, nullable=True, description="Аннотация из Semantic Scholar"),
            "semantic_scholar_citation_count": Column(pa.Int, nullable=True, description="Количество цитирований из Semantic Scholar"),
            "semantic_scholar_venue": Column(pa.String, nullable=True, description="Площадка публикации из Semantic Scholar"),
            "semantic_scholar_year": Column(pa.Int, nullable=True, description="Год из Semantic Scholar"),
            "semantic_scholar_error": Column(pa.String, nullable=True, description="Ошибка из Semantic Scholar"),
            
            # Валидационные флаги
            "valid_doi": Column(pa.Bool, nullable=True, description="Валидное значение DOI"),
            "valid_journal": Column(pa.Bool, nullable=True, description="Валидное значение журнала"),
            "valid_year": Column(pa.Bool, nullable=True, description="Валидное значение года"),
            "valid_volume": Column(pa.Bool, nullable=True, description="Валидное значение тома"),
            "valid_issue": Column(pa.Bool, nullable=True, description="Валидное значение номера выпуска"),
            "invalid_doi": Column(pa.Bool, nullable=True, description="Флаг невалидности DOI"),
            "invalid_journal": Column(pa.Bool, nullable=True, description="Флаг невалидности журнала"),
            "invalid_year": Column(pa.Bool, nullable=True, description="Флаг невалидности года"),
            "invalid_volume": Column(pa.Bool, nullable=True, description="Флаг невалидности тома"),
            "invalid_issue": Column(pa.Bool, nullable=True, description="Флаг невалидности номера выпуска"),
            
            # Системные поля
            "index": Column(
                pa.Int,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Index must be >= 0"),
                    Check(lambda x: x.notna())
                ],
                nullable=False,
                description="Порядковый номер записи"
            ),
            "pipeline_version": Column(
                pa.String,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Версия пайплайна"
            ),
            "source_system": Column(
                pa.String,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Система-источник"
            ),
            "chembl_release": Column(pa.String, nullable=True, description="Версия ChEMBL"),
            "extracted_at": Column(
                pa.DateTime,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Время извлечения данных"
            ),
            "hash_row": Column(
                pa.String,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Хеш строки SHA256"
            ),
            "hash_business_key": Column(
                pa.String,
                checks=[Check(lambda x: x.notna())],
                nullable=False,
                description="Хеш бизнес-ключа SHA256"
            ),
            
            # Ошибки и статусы
            "extraction_errors": Column(pa.String, nullable=True, description="Ошибки извлечения (JSON)"),
            "validation_errors": Column(pa.String, nullable=True, description="Ошибки валидации (JSON)"),
            "extraction_status": Column(
                pa.String,
                checks=[
                    Check.isin(["success", "partial", "failed"], error="Invalid extraction status")
                ],
                nullable=True,
                description="Статус извлечения"
            ),
        })


class DocumentSchemaValidator:
    """Валидатор схем документов."""
    
    def __init__(self):
        self.input_schema = DocumentInputSchema.get_schema()
        self.raw_schema = DocumentRawSchema.get_schema()
        self.normalized_schema = DocumentNormalizedSchema.get_schema()
    
    def validate_input(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать входные данные документов."""
        return self.input_schema.validate(df)
    
    def validate_raw(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать сырые данные документов."""
        return self.raw_schema.validate(df)
    
    def validate_normalized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать нормализованные данные документов."""
        return self.normalized_schema.validate(df)
    
    def get_schema_errors(self, df: pd.DataFrame, schema_type: str = "normalized") -> list:
        """Получить ошибки схемы."""
        try:
            if schema_type == "input":
                self.input_schema.validate(df)
            elif schema_type == "raw":
                self.raw_schema.validate(df)
            elif schema_type == "normalized":
                self.normalized_schema.validate(df)
            else:
                raise ValueError(f"Неизвестный тип схемы: {schema_type}")
            return []
        except pa.errors.SchemaError as e:
            return [str(error) for error in e.failure_cases]
    
    def is_valid(self, df: pd.DataFrame, schema_type: str = "normalized") -> bool:
        """Проверить валидность данных."""
        try:
            if schema_type == "input":
                self.input_schema.validate(df)
            elif schema_type == "raw":
                self.raw_schema.validate(df)
            elif schema_type == "normalized":
                self.normalized_schema.validate(df)
            else:
                raise ValueError(f"Неизвестный тип схемы: {schema_type}")
            return True
        except pa.errors.SchemaError:
            return False


# Глобальный экземпляр валидатора
document_schema_validator = DocumentSchemaValidator()


def validate_document_input(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать входные данные документов."""
    return document_schema_validator.validate_input(df)


def validate_document_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать сырые данные документов."""
    return document_schema_validator.validate_raw(df)


def validate_document_normalized(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать нормализованные данные документов."""
    return document_schema_validator.validate_normalized(df)


def get_document_schema_errors(df: pd.DataFrame, schema_type: str = "normalized") -> list:
    """Получить ошибки схемы документов."""
    return document_schema_validator.get_schema_errors(df, schema_type)


def is_document_valid(df: pd.DataFrame, schema_type: str = "normalized") -> bool:
    """Проверить валидность данных документов."""
    return document_schema_validator.is_valid(df, schema_type)
