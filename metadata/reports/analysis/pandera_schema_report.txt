# Отчёт анализа Pandera схем

## Executive Summary

- **Entities проанализировано**: 5/5

## Таблица анализа схем

| Entity | Schema Loaded | Validation Passed | Dtype Issues | Check Issues | Coerce Needed |
|--------|---------------|-------------------|--------------|--------------|---------------|
| activities | ❌ | ❌ | ✅ | ✅ | ✅ |
| assays | ❌ | ❌ | ✅ | ✅ | ✅ |
| documents | ❌ | ❌ | ✅ | ✅ | ✅ |
| targets | ❌ | ❌ | ✅ | ✅ | ✅ |
| testitem | ✅ | ❌ | ✅ | ✅ | ✅ |

## Детальный анализ по сущностям

### Activities

- **Схема загружена**: Нет
- **Валидация прошла**: Нет
- **Размер выборки**: 2 строк

### Assays

- **Схема загружена**: Нет
- **Валидация прошла**: Нет
- **Размер выборки**: 2 строк

### Documents

- **Схема загружена**: Нет
- **Валидация прошла**: Нет
- **Размер выборки**: 2 строк
- **Проблемы с форматами**:
  - chembl_pmid: PMID формат (0.00%)
  - crossref_pmid: PMID формат (0.00%)
  - openalex_pmid: PMID формат (0.00%)
  - pubmed_pmid: PMID формат (0.00%)
  - semantic_scholar_pmid: PMID формат (0.00%)

### Targets

- **Схема загружена**: Нет
- **Валидация прошла**: Нет
- **Размер выборки**: 2 строк
- **Проблемы с форматами**:
  - uniprot_last_update: ISO 8601 формат (0.00%)
  - timestamp_utc: ISO 8601 формат (0.00%)
  - multi_source_validated: ISO 8601 формат (0.00%)
  - extracted_at: ISO 8601 формат (0.00%)

### Testitem

- **Схема загружена**: Да
- **Валидация прошла**: Нет
- **Размер выборки**: 2 строк
- **Ошибки валидации**: 1
  - column 'all_names' not in dataframe. Columns in dataframe: ['molecule_chembl_id', 'molregno', 'pref_...
- **Проблемы с форматами**:
  - drug_chembl_id: ChEMBL ID формат (0.00%)
  - salt_chembl_id: ChEMBL ID формат (0.00%)
