Аннотация: Базовые метрики до рефакторинга (ветка test_refactoring_32). Дата: 2025-10-31.

- Исходники Python в `src/`: 265 файлов
- Пайплайны (`src/bioetl/pipelines/*.py`, без `__init__`): 8
- Схемы (`src/bioetl/schemas/*.py`, без `__init__`): 10
- Источники (`src/bioetl/sources/` провайдеров): 9 пакетов (chembl, crossref, document, iuphar, openalex, pubchem, pubmed, semantic_scholar, uniprot)
- Кластеры дубликатов, выявленные: 4 (см. DUPLICATES_REPORT.md)
- Выявленные циклы импорта: 0 (эвристический анализ; core не импортирует sources)
- Нестабильные I/O места: 4 (см. ARCH_RULES_VIOLATIONS.md)
- Оценка времени сборки артефактов: N/A (не измерялось)

Сводка по слоям (по структуре директорий)
- client/: `src/bioetl/core/api_client.py`, клиенты источников в `src/bioetl/sources/*/client*`
- request/: `src/bioetl/sources/*/request/*`, база в [ref: repo:src/bioetl/sources/common/request.py@test_refactoring_32]
- parser/: `src/bioetl/sources/*/parser/*`
- normalizer/: `src/bioetl/normalizers/*`, а также специфичные нормализаторы в sources
- schema/: `src/bioetl/schemas/*`
- pipeline.py: `src/bioetl/pipelines/*`
- output/: `src/bioetl/core/output_writer.py`

Замечания по покрытию
- Параллельные реализации пагинации и клиентов приводят к росту LOC и точек отказа.


