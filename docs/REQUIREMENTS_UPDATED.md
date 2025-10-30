# REQUIREMENTS UPDATED

| ID    | Обновление | Доказательство | Ссылки |
|-------|------------|----------------|--------|
| REQ-5 | QC отчёты фиксируют coverage, duplicates и fallback для всех пайплайнов | QC regression тесты, анализ CSV sidecars | [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11] |
| REQ-6 | Document mode=all агрегирует внешние источники с приоритетами и graceful degradation | Интеграционные тесты с моками PubMed/Crossref/OpenAlex/S2 | [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11] |
| REQ-7 | Target pipeline формирует четыре согласованные таблицы с PK/FK и логикой enrichment | Contract тесты parquet схем и referential integrity | [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11] |

