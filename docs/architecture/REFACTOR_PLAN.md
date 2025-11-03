# REFACTOR_PLAN.md

Дорожная карта фиксирует поэтапный переход пайплайнов к архитектуре `src/bioetl/sources/<source>/…` и сопровождение документации. План рассчитан на короткие независимые PR с автоматическими проверками.

## Входные артефакты

- [MODULE_RULES.md](../../refactoring/MODULE_RULES.md) — нормативная раскладка слоёв. [ref: repo:docs/architecture/refactoring/MODULE_RULES.md@test_refactoring_32]
- [PIPELINES.md](../../refactoring/PIPELINES.md) — контракт стадий и API пайплайнов. [ref: repo:docs/architecture/refactoring/PIPELINES.md@test_refactoring_32]
- [NORMALIZATION_RULES.md](NORMALIZATION_RULES.md) — единые правила нормализации.
- [VALIDATION_RULES.md](VALIDATION_RULES.md) — спецификация валидации и QC.

## Источники истины

- [SOURCES_AND_INTERFACES.md](SOURCES_AND_INTERFACES.md) — витрина спецификаций источников и интерфейсов.
- [PIPELINES.inventory.csv](PIPELINES.inventory.csv) — инвентаризация пайплайнов. [ref: repo:docs/pipelines/PIPELINES.inventory.csv@test_refactoring_32]
- [../configs/inventory.yaml](../../configs/inventory.yaml) — конфигурация генератора инвентаризации. [ref: repo:configs/inventory.yaml@test_refactoring_32]
- [../src/scripts/run_inventory.py](../../src/scripts/run_inventory.py) — CLI для генерации артефактов. [ref: repo:src/scripts/run_inventory.py@test_refactoring_32]

## Этапы

| Этап | Цель | Основные задачи | Контроль качества |
| --- | --- | --- | --- |
| E1 | Подготовка инфраструктуры | 1) Создать каталоги `src/bioetl/sources/<source>/` с подслоями и прокси-импортами из текущих `pipelines/`. 2) Перенести конфиги в `configs/sources/<source>/` и обновить импорты. | `python -m bioetl.cli.main list`, `pytest tests/unit/test_cli_contract.py` |
| E2 | Миграция нормализации и схем | 1) Переместить нормализаторы в `normalizer/`, обновить реестр. 2) Перенести Pandera-схемы в `schema/`, скорректировать `VALIDATION_RULES.md`. | `pytest tests/unit/schemas`, `python -m tools.qa.check_required_docs` |
| E3 | Миграция пайплайнов и CLI | 1) Перенести `pipeline.py` и вспомогательные слои, обновить `PIPELINE_COMMAND_REGISTRY`. 2) Обновить документацию (FAQ, README) на новый namespace `bioetl.sources.<source>.pipeline`. | `python -m bioetl.cli.main activity --help`, `pytest tests/integration/pipelines` |
| E4 | Чистка и автоматизация | 1) Удалить старые модули `src/bioetl/pipelines/*`. 2) Закрепить линк-чек и проверки наличия артефактов в CI (`lychee`, `tools/qa/check_pipeline_docs.py`). | `.github/workflows/ci.yaml` с новыми job, `python src/scripts/run_inventory.py --check` |

## Критерии приёмки

1. Все публичные импорты используют namespace `bioetl.sources.<source>`. [ref: repo:docs/architecture/refactoring/MODULE_RULES.md@test_refactoring_32]
2. Документы NORMALIZATION_RULES и VALIDATION_RULES обновляются синхронно с переносами и проходят линк-чек. [ref: repo:docs/architecture/NORMALIZATION_RULES.md@test_refactoring_32]
3. CLI-команда `python -m bioetl.cli.main <pipeline>` остаётся рабочей на каждом этапе и покрыта дымовым тестом. [ref: repo:src/bioetl/cli/main.py@test_refactoring_32]
4. Pandera-валидация и QC для всех источников проходят без деградаций после миграции. [ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]

## Rollback

- Каждый этап завершается релизом, в котором сохраняются прокси-импорты. В случае отказа достаточно откатить конкретный PR и восстановить указатели реестра. [ref: repo:src/scripts/__init__.py@test_refactoring_32]
- Для критических инцидентов предусмотрен возврат к предыдущему релизу `src/bioetl/pipelines/*`, поскольку каталоги не удаляются до завершения E4.
