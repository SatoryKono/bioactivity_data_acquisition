# MIGRATION_PLAN

## Стратегия

1. Ввести модуль-прокладку импорта на один релиз: сохраняем старые пути, экспортируем из новых, логируем DeprecationWarning.
2. Переименования атомарными PR-бачами по группам правил: P1 → P2 → P3.
3. Включить линтер линк-чекинга и импорт-чекинга в CI; заблокировать PR при новых нарушениях.
4. Для `pipelines/` ввести иерархию `<provider>/<entity>/<stage>.py`; переместить текущие файлы согласно провайдеру/сущности/стадии.
5. Тесты переименовать зеркально по дереву модулей.
6. Конфиги привести к `config/<scope>/<scope>.yaml` и документировать ключи в `docs/config/CONFIG_KEYS.md`.

## Маппинг переименований (old → new)

| old_path | new_path | rule_id |
|---|---|---|
| src/bioetl/__init__.py | src/bioetl/__init__.py | F001 |
| src/bioetl/cli/__init__.py | src/bioetl/cli/__init__.py | F001 |
| src/bioetl/cli/tools/__init__.py | src/bioetl/cli/tools/__init__.py | F001 |
| src/bioetl/clients/__init__.py | src/bioetl/clients/__init__.py | F001 |
| src/bioetl/clients/activity/__init__.py | src/bioetl/clients/activity/__init__.py | F001 |
| src/bioetl/clients/assay/__init__.py | src/bioetl/clients/assay/__init__.py | F001 |
| src/bioetl/clients/document/__init__.py | src/bioetl/clients/document/__init__.py | F001 |
| src/bioetl/clients/target/__init__.py | src/bioetl/clients/target/__init__.py | F001 |
| src/bioetl/clients/testitem/__init__.py | src/bioetl/clients/testitem/__init__.py | F001 |
| src/bioetl/config/__init__.py | src/bioetl/config/__init__.py | F001 |
| src/bioetl/config/activity/__init__.py | src/bioetl/config/activity/__init__.py | F001 |
| src/bioetl/config/assay/__init__.py | src/bioetl/config/assay/__init__.py | F001 |
| src/bioetl/config/document/__init__.py | src/bioetl/config/document/__init__.py | F001 |
| src/bioetl/config/models/__init__.py | src/bioetl/config/models/__init__.py | F001 |
| src/bioetl/config/target/__init__.py | src/bioetl/config/target/__init__.py | F001 |
| src/bioetl/config/testitem/__init__.py | src/bioetl/config/testitem/__init__.py | F001 |
| src/bioetl/core/__init__.py | src/bioetl/core/__init__.py | F001 |
| src/bioetl/pipelines/__init__.py | src/bioetl/pipelines/__init__.py | F001 |
| src/bioetl/pipelines/activity/__init__.py | src/bioetl/pipelines/activity/__init__.py | F001 |
| src/bioetl/pipelines/assay/__init__.py | src/bioetl/pipelines/assay/__init__.py | F001 |
| src/bioetl/pipelines/document/__init__.py | src/bioetl/pipelines/document/__init__.py | F001 |
| src/bioetl/pipelines/target/__init__.py | src/bioetl/pipelines/target/__init__.py | F001 |
| src/bioetl/pipelines/testitem/__init__.py | src/bioetl/pipelines/testitem/__init__.py | F001 |
| src/bioetl/qc/__init__.py | src/bioetl/qc/__init__.py | F001 |
| src/bioetl/schemas/__init__.py | src/bioetl/schemas/__init__.py | F001 |
| src/bioetl/schemas/activity/__init__.py | src/bioetl/schemas/activity/__init__.py | F001 |
| src/bioetl/schemas/assay/__init__.py | src/bioetl/schemas/assay/__init__.py | F001 |
| src/bioetl/schemas/document/__init__.py | src/bioetl/schemas/document/__init__.py | F001 |
| src/bioetl/schemas/target/__init__.py | src/bioetl/schemas/target/__init__.py | F001 |
| src/bioetl/schemas/testitem/__init__.py | src/bioetl/schemas/testitem/__init__.py | F001 |
| src/bioetl/tools/__init__.py | src/bioetl/tools/__init__.py | F001 |

## Риски дублирования

- Кластеров точных дубликатов файлов: 0
- Кластеров идентичных функций: 30
- Пары near-duplicate (Jaccard≥0.85): 0