# Отчет о диагностике отсутствия данных в полях документов

## Выполненные действия

### 1. ✅ Исправлено: Добавлен маппинг pubmed_id → document_pubmed_id

**Проблема**: Пайплайн искал поле `document_pubmed_id`, а во входном CSV было только `pubmed_id`.

**Решение**: Добавлен код в функцию `_normalise_columns` (строки 448-451):
```python
# Маппинг pubmed_id на document_pubmed_id для совместимости с ETL
if "pubmed_id" in normalised.columns and "document_pubmed_id" not in normalised.columns:
    normalised["document_pubmed_id"] = normalised["pubmed_id"]
```

### 2. ✅ Результат: PubMed PMID теперь заполняется

- **pubmed_pmid**: 17827018 ✅
- Данные успешно извлекаются из PubMed API

### 3. ⚠️ Требует дополнительной проверки: OpenAlex PMID

**Проблема**: `openalex_pmid` всё ещё пустой (nan)

**Возможные причины**:
- OpenAlex использует batch processing вместо индивидуальных запросов
- OpenAlex сначала ищет по DOI, и только потом по PMID
- В коде есть альтернативная логика для batch запросов

**Следующие шаги**:
1. Проверить логи OpenAlex клиента
2. Убедиться что `_extract_data_from_source` правильно вызывает OpenAlex
3. Проверить что OpenAlex возвращает PMID в своих ответах

### 4. ℹ️ Crossref PMID: Ожидаемое поведение

Crossref редко возвращает PMID напрямую - это нормально. Поле остается пустым, что соответствует ожиданиям.

## Итоговый статус

- ✅ PubMed работает: pubmed_pmid заполняется
- ⚠️ OpenAlex: требуется дополнительная проверка
- ℹ️ Crossref: пуст как ожидалось

## Файлы изменены

- `src/library/documents/pipeline.py` - добавлен маппинг pubmed_id
- `diagnostic_report.md` - этот отчет
