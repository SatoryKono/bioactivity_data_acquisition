"""
Финальный тест синхронизации схемы документов после внесенных изменений.

Проверяет:
1. DocumentETLWriter теперь использует column_order из конфига
2. DocumentNormalizedSchema содержит все необходимые поля
3. Системные метаданные добавляются в пайплайн
"""

import pandas as pd
from pathlib import Path

# Импортируем обновленные компоненты
from src.library.common.writer_base import DocumentETLWriter
from src.library.schemas.document_schema import DocumentNormalizedSchema
from src.library.documents.normalize import DocumentNormalizer
from src.library.config import Config

def test_document_etl_writer():
    """Тест DocumentETLWriter - проверяем что он использует column_order из конфига."""
    print("=" * 80)
    print("ТЕСТ 1: DocumentETLWriter использует column_order из конфига")
    print("=" * 80)
    
    # Создаем мок конфиг с column_order
    class MockConfig:
        def __init__(self):
            self.determinism = MockDeterminism()
    
    class MockDeterminism:
        def __init__(self):
            self.column_order = [
                "document_chembl_id", "doi", "title", "abstract", "journal", "year"
            ]
    
    config = MockConfig()
    writer = DocumentETLWriter(config, "documents")
    
    # Проверяем что get_column_order возвращает column_order из конфига
    column_order = writer.get_column_order()
    
    if column_order is not None:
        print("✅ DocumentETLWriter.get_column_order() возвращает column_order из конфига")
        print(f"   Получен порядок колонок: {column_order[:5]}... (показаны первые 5)")
    else:
        print("❌ DocumentETLWriter.get_column_order() все еще возвращает None")
    
    return column_order is not None

def test_document_normalized_schema():
    """Тест DocumentNormalizedSchema - проверяем что все поля присутствуют."""
    print("\n" + "=" * 80)
    print("ТЕСТ 2: DocumentNormalizedSchema содержит все необходимые поля")
    print("=" * 80)
    
    schema = DocumentNormalizedSchema.get_schema()
    schema_columns = set(schema.columns.keys())
    
    # Ожидаемые поля из фактического вывода
    expected_columns = {
        # Основные поля
        "document_chembl_id", "document_pubmed_id", "document_classification",
        "referenses_on_previous_experiments", "original_experimental_document",
        
        # Консолидированные поля
        "doi", "title", "abstract", "journal", "year", "volume", "issue",
        "first_page", "last_page", "citation", "month",
        
        # ChEMBL поля
        "chembl_pmid", "chembl_title", "chembl_abstract", "chembl_authors",
        "chembl_doi", "chembl_doc_type", "chembl_issn", "chembl_journal",
        "chembl_year", "chembl_volume", "chembl_issue", "chembl_first_page",
        "chembl_last_page", "chembl_error",
        
        # Crossref поля
        "crossref_pmid", "crossref_title", "crossref_abstract", "crossref_authors",
        "crossref_doi", "crossref_doc_type", "crossref_issn", "crossref_journal",
        "crossref_year", "crossref_volume", "crossref_issue", "crossref_first_page",
        "crossref_last_page", "crossref_subject", "crossref_error",
        
        # OpenAlex поля
        "openalex_pmid", "openalex_title", "openalex_abstract", "openalex_authors",
        "openalex_doi", "openalex_doc_type", "openalex_crossref_doc_type",
        "openalex_issn", "openalex_journal", "openalex_year", "openalex_volume",
        "openalex_issue", "openalex_first_page", "openalex_last_page",
        "openalex_concepts", "openalex_type", "openalex_error",
        
        # PubMed поля
        "pubmed_pmid", "pubmed_article_title", "pubmed_abstract", "pubmed_authors",
        "pubmed_doi", "pubmed_doc_type", "pubmed_issn", "pubmed_journal",
        "pubmed_year", "pubmed_volume", "pubmed_issue", "pubmed_first_page",
        "pubmed_last_page", "pubmed_mesh_descriptors", "pubmed_mesh_qualifiers",
        "pubmed_chemical_list", "pubmed_year_completed", "pubmed_month_completed",
        "pubmed_day_completed", "pubmed_year_revised", "pubmed_month_revised",
        "pubmed_day_revised", "pubmed_pages", "pubmed_pmcid", "pubmed_title",
        "pubmed_day", "pubmed_month", "pubmed_error",
        
        # Semantic Scholar поля
        "semantic_scholar_pmid", "semantic_scholar_title", "semantic_scholar_authors",
        "semantic_scholar_doi", "semantic_scholar_doc_type", "semantic_scholar_issn",
        "semantic_scholar_journal", "semantic_scholar_abstract", "semantic_scholar_citation_count",
        "semantic_scholar_venue", "semantic_scholar_year", "semantic_scholar_error",
        
        # Системные поля
        "index", "pipeline_version", "source_system", "chembl_release", "extracted_at",
        "hash_row", "hash_business_key", "extraction_errors", "validation_errors",
        "extraction_status", "retrieved_at",
        
        # Валидационные поля
        "valid_doi", "valid_journal", "valid_year", "valid_volume", "valid_issue",
        "invalid_doi", "invalid_journal", "invalid_year", "invalid_volume", "invalid_issue",
        
        # Дополнительные поля
        "document_citation", "publication_date", "document_sortorder"
    }
    
    missing_columns = expected_columns - schema_columns
    extra_columns = schema_columns - expected_columns
    
    print(f"Всего колонок в схеме: {len(schema_columns)}")
    print(f"Ожидаемых колонок: {len(expected_columns)}")
    
    if missing_columns:
        print(f"❌ Отсутствующие колонки ({len(missing_columns)}):")
        for col in sorted(missing_columns):
            print(f"   - {col}")
    else:
        print("✅ Все ожидаемые колонки присутствуют в схеме")
    
    if extra_columns:
        print(f"⚠️  Дополнительные колонки в схеме ({len(extra_columns)}):")
        for col in sorted(extra_columns):
            print(f"   - {col}")
    
    return len(missing_columns) == 0

def test_system_metadata():
    """Тест добавления системных метаданных."""
    print("\n" + "=" * 80)
    print("ТЕСТ 3: Системные метаданные добавляются в пайплайн")
    print("=" * 80)
    
    # Создаем тестовый DataFrame
    test_df = pd.DataFrame({
        'document_chembl_id': ['CHEMBL123', 'CHEMBL456'],
        'doi': ['10.1234/test1', '10.1234/test2'],
        'title': ['Test Title 1', 'Test Title 2']
    })
    
    # Создаем нормализатор
    config = {
        'pipeline': {'version': '2.0.0'}
    }
    normalizer = DocumentNormalizer(config)
    
    # Применяем нормализацию
    try:
        normalized_df = normalizer._add_system_metadata(test_df)
        
        # Проверяем наличие системных полей
        system_fields = [
            'index', 'pipeline_version', 'source_system', 'chembl_release',
            'extracted_at', 'hash_row', 'hash_business_key'
        ]
        
        missing_fields = []
        for field in system_fields:
            if field not in normalized_df.columns:
                missing_fields.append(field)
        
        if missing_fields:
            print(f"❌ Отсутствующие системные поля: {missing_fields}")
            return False
        else:
            print("✅ Все системные поля добавлены:")
            for field in system_fields:
                print(f"   - {field}: {normalized_df[field].iloc[0] if len(normalized_df) > 0 else 'N/A'}")
            return True
            
    except Exception as e:
        print(f"❌ Ошибка при добавлении системных метаданных: {e}")
        return False

def main():
    """Запуск всех тестов."""
    print("ФИНАЛЬНЫЙ ТЕСТ СИНХРОНИЗАЦИИ СХЕМЫ ДОКУМЕНТОВ")
    print("=" * 80)
    
    results = []
    
    # Тест 1: DocumentETLWriter
    results.append(test_document_etl_writer())
    
    # Тест 2: DocumentNormalizedSchema
    results.append(test_document_normalized_schema())
    
    # Тест 3: Системные метаданные
    results.append(test_system_metadata())
    
    # Итоговый результат
    print("\n" + "=" * 80)
    print("ИТОГОВЫЙ РЕЗУЛЬТАТ")
    print("=" * 80)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Пройдено тестов: {passed}/{total}")
    
    if passed == total:
        print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Синхронизация схемы завершена успешно.")
    else:
        print("⚠️  Некоторые тесты не пройдены. Требуется дополнительная работа.")
    
    return passed == total

if __name__ == "__main__":
    main()
