"""
Pandera схемы для валидации данных документов с нормализацией.

Предоставляет схемы для входных, сырых и нормализованных данных документов
с атрибутами нормализации для каждой колонки.
"""

import pandera as pa
from pandera import Check, Column, DataFrameSchema


def add_normalization_metadata(column: Column, functions: list[str]) -> Column:
    """Добавляет метаданные нормализации к колонке.
    
    Args:
        column: Колонка Pandera
        functions: Список функций нормализации
        
    Returns:
        Колонка с добавленными метаданными
    """
    metadata = column.metadata or {}
    metadata["normalization_functions"] = functions
    return Column(
        column.dtype,
        checks=column.checks,
        nullable=column.nullable,
        description=column.description,
        metadata=metadata
    )


class DocumentInputSchema:
    """Схемы для входных данных документов."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для входных данных документов."""
        return DataFrameSchema({
            "document_chembl_id": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL document ID format"),
                        Check(lambda x: x.notna())
                    ],
                    nullable=False,
                    description="ChEMBL ID документа"
                ),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "doi": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^10\.\d+/[^\s]+$', error="Invalid DOI format"),
                    ],
                    nullable=True,
                    description="DOI документа"
                ),
                ["normalize_string_strip", "normalize_string_lower", "normalize_doi"]
            ),
            "pmid": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^\d+$', error="Invalid PMID format"),
                    ],
                    nullable=True,
                    description="PubMed ID"
                ),
                ["normalize_string_strip", "normalize_pmid"]
            ),
            "title": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_length(min_value=1, max_value=1000, error="Title length must be 1-1000 characters"),
                    ],
                    nullable=True,
                    description="Название документа"
                ),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            )
        })


class DocumentRawSchema:
    """Схемы для сырых данных документов из API."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для сырых данных документов."""
        return DataFrameSchema({
            # Основные поля
            "document_chembl_id": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL document ID format"),
                        Check(lambda x: x.notna())
                    ],
                    nullable=False,
                    description="ChEMBL ID документа"
                ),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "document_pubmed_id": add_normalization_metadata(
                Column(pa.String, nullable=True, description="PubMed ID из ChEMBL"),
                ["normalize_string_strip", "normalize_pmid"]
            ),
            "document_classification": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Классификация документа"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "referenses_on_previous_experiments": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Ссылки на предыдущие эксперименты"),
                ["normalize_boolean"]
            ),
            "original_experimental_document": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Оригинальный экспериментальный документ"),
                ["normalize_boolean"]
            ),
            "retrieved_at": add_normalization_metadata(
                Column(
                    pa.DateTime,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="Время получения данных"
                ),
                ["normalize_datetime_iso8601"]
            ),
            
            # ChEMBL поля
            "chembl_pmid": add_normalization_metadata(
                Column(pa.String, nullable=True, description="PMID из ChEMBL"),
                ["normalize_string_strip", "normalize_pmid"]
            ),
            "chembl_title": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Название из ChEMBL"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "chembl_abstract": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Аннотация из ChEMBL"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "chembl_authors": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Авторы из ChEMBL"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "chembl_doi": add_normalization_metadata(
                Column(pa.String, nullable=True, description="DOI из ChEMBL"),
                ["normalize_string_strip", "normalize_string_lower", "normalize_doi"]
            ),
            "chembl_doc_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип документа из ChEMBL"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "chembl_issn": add_normalization_metadata(
                Column(pa.String, nullable=True, description="ISSN из ChEMBL"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "chembl_journal": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Журнал из ChEMBL"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "chembl_year": add_normalization_metadata(
                Column(pa.Int, nullable=True, description="Год из ChEMBL"),
                ["normalize_int", "normalize_year"]
            ),
            "chembl_volume": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Том из ChEMBL"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "chembl_issue": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Номер выпуска из ChEMBL"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "chembl_error": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ошибка из ChEMBL"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            
            # Crossref поля
            "crossref_pmid": add_normalization_metadata(
                Column(pa.String, nullable=True, description="PMID из Crossref"),
                ["normalize_string_strip", "normalize_pmid"]
            ),
            "crossref_title": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Название из Crossref"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "crossref_abstract": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Аннотация из Crossref"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "crossref_authors": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Авторы из Crossref"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "crossref_doi": add_normalization_metadata(
                Column(pa.String, nullable=True, description="DOI из Crossref"),
                ["normalize_string_strip", "normalize_string_lower", "normalize_doi"]
            ),
            "crossref_doc_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип документа из Crossref"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "crossref_issn": add_normalization_metadata(
                Column(pa.String, nullable=True, description="ISSN из Crossref"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "crossref_journal": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Журнал из Crossref"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "crossref_year": add_normalization_metadata(
                Column(pa.Int, nullable=True, description="Год из Crossref"),
                ["normalize_int", "normalize_year"]
            ),
            "crossref_volume": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Том из Crossref"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "crossref_issue": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Номер выпуска из Crossref"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "crossref_first_page": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Первая страница из Crossref"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "crossref_last_page": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Последняя страница из Crossref"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "crossref_subject": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Предметная область из Crossref"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "crossref_error": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ошибка из Crossref"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            
            # OpenAlex поля
            "openalex_pmid": add_normalization_metadata(
                Column(pa.String, nullable=True, description="PMID из OpenAlex"),
                ["normalize_string_strip", "normalize_pmid"]
            ),
            "openalex_title": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Название из OpenAlex"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "openalex_abstract": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Аннотация из OpenAlex"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "openalex_authors": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Авторы из OpenAlex"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "openalex_doi": add_normalization_metadata(
                Column(pa.String, nullable=True, description="DOI из OpenAlex"),
                ["normalize_string_strip", "normalize_string_lower", "normalize_doi"]
            ),
            "openalex_doc_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип документа из OpenAlex"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "openalex_crossref_doc_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип документа из OpenAlex Crossref"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "openalex_issn": add_normalization_metadata(
                Column(pa.String, nullable=True, description="ISSN из OpenAlex"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "openalex_journal": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Журнал из OpenAlex"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "openalex_year": add_normalization_metadata(
                Column(pa.Int, nullable=True, description="Год из OpenAlex"),
                ["normalize_int", "normalize_year"]
            ),
            "openalex_volume": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Том из OpenAlex"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "openalex_issue": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Номер выпуска из OpenAlex"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "openalex_first_page": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Первая страница из OpenAlex"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "openalex_last_page": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Последняя страница из OpenAlex"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "openalex_error": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ошибка из OpenAlex"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            
            # PubMed поля
            "pubmed_pmid": add_normalization_metadata(
                Column(pa.String, nullable=True, description="PMID из PubMed"),
                ["normalize_string_strip", "normalize_pmid"]
            ),
            "pubmed_article_title": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Название статьи из PubMed"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "pubmed_abstract": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Аннотация из PubMed"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "pubmed_authors": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Авторы из PubMed"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "pubmed_doi": add_normalization_metadata(
                Column(pa.String, nullable=True, description="DOI из PubMed"),
                ["normalize_string_strip", "normalize_string_lower", "normalize_doi"]
            ),
            "pubmed_doc_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип документа из PubMed"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "pubmed_issn": add_normalization_metadata(
                Column(pa.String, nullable=True, description="ISSN из PubMed"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "pubmed_journal": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Журнал из PubMed"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "pubmed_year": add_normalization_metadata(
                Column(pa.Int, nullable=True, description="Год из PubMed"),
                ["normalize_int", "normalize_year"]
            ),
            "pubmed_volume": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Том из PubMed"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "pubmed_issue": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Номер выпуска из PubMed"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "pubmed_first_page": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Первая страница из PubMed"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "pubmed_last_page": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Последняя страница из PubMed"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "pubmed_mesh_descriptors": add_normalization_metadata(
                Column(pa.String, nullable=True, description="MeSH дескрипторы из PubMed"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "pubmed_mesh_qualifiers": add_normalization_metadata(
                Column(pa.String, nullable=True, description="MeSH квалификаторы из PubMed"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "pubmed_chemical_list": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Список химических веществ из PubMed"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "pubmed_year_completed": add_normalization_metadata(
                Column(pa.Int, nullable=True, description="Год завершения из PubMed"),
                ["normalize_int", "normalize_year"]
            ),
            "pubmed_month_completed": add_normalization_metadata(
                Column(pa.Int, nullable=True, description="Месяц завершения из PubMed"),
                ["normalize_int", "normalize_month"]
            ),
            "pubmed_day_completed": add_normalization_metadata(
                Column(pa.Int, nullable=True, description="День завершения из PubMed"),
                ["normalize_int", "normalize_day"]
            ),
            "pubmed_year_revised": add_normalization_metadata(
                Column(pa.Int, nullable=True, description="Год ревизии из PubMed"),
                ["normalize_int", "normalize_year"]
            ),
            "pubmed_month_revised": add_normalization_metadata(
                Column(pa.Int, nullable=True, description="Месяц ревизии из PubMed"),
                ["normalize_int", "normalize_month"]
            ),
            "pubmed_day_revised": add_normalization_metadata(
                Column(pa.Int, nullable=True, description="День ревизии из PubMed"),
                ["normalize_int", "normalize_day"]
            ),
            "pubmed_error": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ошибка из PubMed"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            
            # Semantic Scholar поля
            "semantic_scholar_pmid": add_normalization_metadata(
                Column(pa.String, nullable=True, description="PMID из Semantic Scholar"),
                ["normalize_string_strip", "normalize_pmid"]
            ),
            "semantic_scholar_title": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Название из Semantic Scholar"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "semantic_scholar_abstract": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Аннотация из Semantic Scholar"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "semantic_scholar_authors": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Авторы из Semantic Scholar"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "semantic_scholar_doi": add_normalization_metadata(
                Column(pa.String, nullable=True, description="DOI из Semantic Scholar"),
                ["normalize_string_strip", "normalize_string_lower", "normalize_doi"]
            ),
            "semantic_scholar_doc_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип документа из Semantic Scholar"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "semantic_scholar_issn": add_normalization_metadata(
                Column(pa.String, nullable=True, description="ISSN из Semantic Scholar"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "semantic_scholar_journal": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Журнал из Semantic Scholar"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "semantic_scholar_error": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ошибка из Semantic Scholar"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
        })


class DocumentNormalizedSchema:
    """Схемы для нормализованных данных документов."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных документов."""
        return DataFrameSchema({
            # Основные поля
            "document_chembl_id": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL document ID format"),
                        Check(lambda x: x.notna())
                    ],
                    nullable=False,
                    description="ChEMBL ID документа"
                ),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "document_pubmed_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="PubMed ID из ChEMBL"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "document_classification": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Классификация документа"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "referenses_on_previous_experiments": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Ссылки на предыдущие эксперименты"),
                ["normalize_boolean"]
            ),
            "original_experimental_document": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Оригинальный экспериментальный документ"),
                ["normalize_boolean"]
            ),
            "retrieved_at": add_normalization_metadata(
                Column(
                    pa.DateTime,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="Время получения данных"
                ),
                ["normalize_datetime_iso8601"]
            ),
            
            # Вычисляемые поля
            "publication_date": add_normalization_metadata(
                Column(pa.DateTime, nullable=True, description="Дата публикации"),
                ["normalize_datetime_iso8601"]
            ),
            "document_sortorder": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Порядок сортировки документов"),
                ["normalize_int", "normalize_int_range"]
            ),
            
            # Валидационные флаги
            "valid_doi": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Валидный DOI"),
                ["normalize_boolean"]
            ),
            "valid_journal": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Валидный журнал"),
                ["normalize_boolean"]
            ),
            "valid_year": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Валидный год"),
                ["normalize_boolean"]
            ),
            "valid_volume": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Валидный том"),
                ["normalize_boolean"]
            ),
            "valid_issue": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Валидный номер выпуска"),
                ["normalize_boolean"]
            ),
            "invalid_doi": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Невалидный DOI"),
                ["normalize_boolean"]
            ),
            "invalid_journal": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Невалидный журнал"),
                ["normalize_boolean"]
            ),
            "invalid_year": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Невалидный год"),
                ["normalize_boolean"]
            ),
            "invalid_volume": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Невалидный том"),
                ["normalize_boolean"]
            ),
            "invalid_issue": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Невалидный номер выпуска"),
                ["normalize_boolean"]
            ),
            
            # Системные поля
            "index": add_normalization_metadata(
                Column(pa.Int64, nullable=False, description="Индекс записи"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "pipeline_version": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Версия пайплайна"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "source_system": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Исходная система"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "chembl_release": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Релиз ChEMBL"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "extracted_at": add_normalization_metadata(
                Column(
                    pa.DateTime,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="Время извлечения данных"
                ),
                ["normalize_datetime_iso8601"]
            ),
            "hash_row": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="SHA256 хеш строки"
                ),
                ["normalize_string_strip", "normalize_string_lower"]
            ),
            "hash_business_key": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="SHA256 хеш бизнес-ключа"
                ),
                ["normalize_string_strip", "normalize_string_lower"]
            ),
        })
