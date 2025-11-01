# Семантические клоны (Type IV)

## Fallback паттерны

### Обнаруженные паттерны

1. **FallbackRecordBuilder** - унифицировано в `bioetl.utils.fallback`
   - Используется в: ActivityPipeline, TestItemPipeline
   - Статус: ✅ Унифицировано

2. **Fallback record creation**
   - ActivityPipeline: `_create_fallback_record`
   - DocumentPipeline: `_create_fallback_row`  
   - Сходство: Семантическое (~70%)
   - Статус: ⚠️ Частично унифицировано через `FallbackRecordBuilder`

## Enrichment паттерны

### Обнаруженные паттерны

1. **Target enrichment** - UniProt + IUPHAR
   - Уникальная специфика: многоуровневое обогащение с несколькими источниками
   - Статус: ✅ Оставить как есть (специфичная логика)

2. **Document enrichment** - PubMed/Crossref/OpenAlex/Semantic Scholar
   - Уникальная специфика: мульти-источник с мердж-логикой
   - Статус: ✅ Оставить как есть (специфичная логика)

3. **TestItem enrichment** - PubChem
   - Уникальная специфика: обогащение через InChI Key
   - Статус: ✅ Оставить как есть (специфичная логика)

## Выводы

Семантическое дублирование присутствует, но связано с разными источниками данных и разной спецификой обогащения. Унификация не требуется - каждое обогащение имеет свои особенности.

