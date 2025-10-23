"""
Скрипт для проверки синхронизации схемы колонок в пайплайне documents.

Сравнивает:
1. Схему в document_schema.py (DocumentNormalizedSchema)
2. Порядок колонок в config_document.yaml (determinism.column_order)
3. Фактические колонки в выходном файле documents_20251023.csv
"""


# Колонки из фактического вывода (из documents_20251023.csv)
actual_output_columns = [
    "document_chembl_id", "doi", "document_pubmed_id", "document_classification",
    "referenses_on_previous_experiments", "original_experimental_document",
    "chembl_title", "chembl_journal", "chembl_volume", "chembl_issue",
    "chembl_year", "chembl_doi", "chembl_pmid", "chembl_doc_type",
    "chembl_abstract", "chembl_authors", "chembl_first_page", "chembl_last_page",
    "crossref_doi", "crossref_title", "crossref_doc_type", "crossref_subject",
    "crossref_authors", "pubmed_pmid", "pubmed_journal", "pubmed_volume",
    "pubmed_issue", "pubmed_first_page", "pubmed_last_page", "pubmed_doc_type",
    "pubmed_year_completed", "pubmed_month_completed", "pubmed_day_completed",
    "pubmed_year_revised", "pubmed_month_revised", "pubmed_day_revised",
    "pubmed_issn", "pubmed_authors", "semantic_scholar_pmid", "semantic_scholar_year",
    "semantic_scholar_error", "semantic_scholar_title", "pubmed_doi", "pubmed_pmcid",
    "first_page", "title", "semantic_scholar_authors", "pubmed_month", "month",
    "pubmed_error", "openalex_type", "semantic_scholar_citation_count",
    "pubmed_year", "volume", "pubmed_day", "semantic_scholar_venue",
    "pubmed_title", "citation", "openalex_doi", "crossref_error", "pubmed_abstract",
    "document_sortorder", "openalex_concepts", "last_page", "openalex_error",
    "journal", "year", "openalex_title", "semantic_scholar_abstract", "abstract",
    "pubmed_pages", "semantic_scholar_doi", "publication_date", "issue",
    "document_citation"
]

# Колонки из схемы DocumentNormalizedSchema
schema_columns = [
    "document_chembl_id", "document_pubmed_id", "document_classification",
    "referenses_on_previous_experiments", "original_experimental_document",
    "retrieved_at", "document_citation", "publication_date", "document_sortorder",
    "valid_doi", "valid_journal", "valid_year", "valid_volume", "valid_issue",
    "invalid_doi", "invalid_journal", "invalid_year", "invalid_volume", "invalid_issue",
    "index", "pipeline_version", "source_system", "chembl_release", "extracted_at",
    "hash_row", "hash_business_key", "extraction_errors", "validation_errors",
    "extraction_status"
]

# Колонки из config_document.yaml (determinism.column_order)
config_column_order = [
    "document_chembl_id", "document_pubmed_id", "document_classification",
    "referenses_on_previous_experiments", "original_experimental_document",
    "document_citation", "pubmed_mesh_descriptors", "pubmed_mesh_qualifiers",
    "pubmed_chemical_list", "crossref_subject", "chembl_pmid", "crossref_pmid",
    "openalex_pmid", "pubmed_pmid", "semantic_scholar_pmid", "chembl_title",
    "crossref_title", "openalex_title", "pubmed_article_title", "semantic_scholar_title",
    "chembl_abstract", "crossref_abstract", "openalex_abstract", "pubmed_abstract",
    "chembl_authors", "crossref_authors", "openalex_authors", "pubmed_authors",
    "semantic_scholar_authors", "chembl_doi", "crossref_doi", "openalex_doi",
    "pubmed_doi", "semantic_scholar_doi", "chembl_doc_type", "crossref_doc_type",
    "openalex_doc_type", "openalex_crossref_doc_type", "pubmed_doc_type",
    "semantic_scholar_doc_type", "chembl_issn", "crossref_issn", "openalex_issn",
    "pubmed_issn", "semantic_scholar_issn", "chembl_journal", "crossref_journal",
    "openalex_journal", "pubmed_journal", "semantic_scholar_journal", "chembl_year",
    "crossref_year", "openalex_year", "pubmed_year", "chembl_volume", "crossref_volume",
    "openalex_volume", "pubmed_volume", "chembl_issue", "crossref_issue",
    "openalex_issue", "pubmed_issue", "crossref_first_page", "openalex_first_page",
    "pubmed_first_page", "crossref_last_page", "openalex_last_page", "pubmed_last_page",
    "chembl_error", "crossref_error", "openalex_error", "pubmed_error",
    "semantic_scholar_error", "pubmed_year_completed", "pubmed_month_completed",
    "pubmed_day_completed", "pubmed_year_revised", "pubmed_month_revised",
    "pubmed_day_revised", "publication_date", "document_sortorder", "valid_doi",
    "valid_journal", "valid_year", "valid_volume", "valid_issue", "invalid_doi",
    "invalid_journal", "invalid_year", "invalid_volume", "invalid_issue", "index",
    "pipeline_version", "source_system", "chembl_release", "extracted_at",
    "hash_row", "hash_business_key"
]

print("=" * 80)
print("АНАЛИЗ СИНХРОНИЗАЦИИ СХЕМЫ ДОКУМЕНТОВ")
print("=" * 80)

print("\n1. СТАТИСТИКА КОЛОНОК")
print("-" * 80)
print(f"Фактический вывод (CSV):        {len(actual_output_columns)} колонок")
print(f"Схема (DocumentNormalizedSchema): {len(schema_columns)} колонок")
print(f"Конфиг (column_order):           {len(config_column_order)} колонок")

print("\n2. КОЛОНКИ В ВЫВОДЕ, НО НЕ В СХЕМЕ")
print("-" * 80)
in_output_not_in_schema = set(actual_output_columns) - set(schema_columns)
if in_output_not_in_schema:
    for col in sorted(in_output_not_in_schema):
        print(f"  - {col}")
else:
    print("  Нет несоответствий")

print("\n3. КОЛОНКИ В СХЕМЕ, НО НЕ В ВЫВОДЕ")
print("-" * 80)
in_schema_not_in_output = set(schema_columns) - set(actual_output_columns)
if in_schema_not_in_output:
    for col in sorted(in_schema_not_in_output):
        print(f"  - {col}")
else:
    print("  Нет несоответствий")

print("\n4. КОЛОНКИ В КОНФИГЕ, НО НЕ В ВЫВОДЕ")
print("-" * 80)
in_config_not_in_output = set(config_column_order) - set(actual_output_columns)
if in_config_not_in_output:
    for col in sorted(in_config_not_in_output):
        print(f"  - {col}")
else:
    print("  Нет несоответствий")

print("\n5. КОЛОНКИ В ВЫВОДЕ, НО НЕ В КОНФИГЕ")
print("-" * 80)
in_output_not_in_config = set(actual_output_columns) - set(config_column_order)
if in_output_not_in_config:
    for col in sorted(in_output_not_in_config):
        print(f"  - {col}")
else:
    print("  Нет несоответствий")

print("\n6. КОЛОНКИ В СХЕМЕ, НО НЕ В КОНФИГЕ")
print("-" * 80)
in_schema_not_in_config = set(schema_columns) - set(config_column_order)
if in_schema_not_in_config:
    for col in sorted(in_schema_not_in_config):
        print(f"  - {col}")
else:
    print("  Нет несоответствий")

print("\n7. КОЛОНКИ В КОНФИГЕ, НО НЕ В СХЕМЕ")
print("-" * 80)
in_config_not_in_schema = set(config_column_order) - set(schema_columns)
if in_config_not_in_schema:
    for col in sorted(in_config_not_in_schema):
        print(f"  - {col}")
else:
    print("  Нет несоответствий")

print("\n8. ПРОБЛЕМЫ С ПОРЯДКОМ КОЛОНОК")
print("-" * 80)
print("DocumentETLWriter.get_column_order() возвращает None")
print("Это означает, что порядок колонок из конфига игнорируется!")
print("\nРешение: Обновить DocumentETLWriter, чтобы он использовал column_order из конфига")

print("\n9. РЕКОМЕНДАЦИИ ПО СИНХРОНИЗАЦИИ")
print("-" * 80)
print("\nДействия для синхронизации:")
print("1. Добавить в DocumentNormalizedSchema все колонки из config_column_order")
print("2. Обновить DocumentETLWriter.get_column_order() для использования конфига")
print("3. Удалить лишние колонки из вывода, которых нет в схеме и конфиге")
print("4. Добавить отсутствующие системные колонки в вывод пайплайна")

print("\n" + "=" * 80)
