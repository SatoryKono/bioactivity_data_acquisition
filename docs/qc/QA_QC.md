# qa-qc

## тестовое-покрытие

| Категория | Модули | Назначение |
| --- | --- | --- |
| Unit | `tests/unit/` | Ключевые утилиты (`test_utils_io`, `test_numeric_normalizer`), CLI контракт, материализация, таргет/документ пайплайны.[ref: repo:tests/unit/test_cli_contract.py@test_refactoring_32]
| Schemas | `tests/schemas/` | Pandera схемы и реестр (`test_registry`, `test_document_raw_schema`).[ref: repo:tests/schemas/test_registry.py@test_refactoring_32]
| Pipelines | `tests/pipelines/` | Материализация и совместимость менеджера стадий.[ref: repo:tests/pipelines/test_materialization_manager.py@test_refactoring_32]
| Sources | `tests/sources/` | Поведение адаптеров и HTTP слоёв.[ref: repo:tests/sources/test_base_adapter.py@test_refactoring_32]
| Golden | `tests/golden/` | Стабильность схемы и CLI (`test_unified_schema_golden`, `test_cli_golden`).[ref: repo:tests/golden/test_cli_golden.py@test_refactoring_32]
| Integration | `tests/integration/` | Сквозные сценарии QC и пайплайнов.[ref: repo:tests/integration/qc/test_unified_qc.py@test_refactoring_32]
| Perf | `tests/perf/` | Проверки производительности QC-отчётов и хеширования.[ref: repo:tests/perf/test_quality_report_generator.py@test_refactoring_32]

## golden-наборы

- `tests/golden/test_unified_schema_golden.py` сравнивает зарегистрированные схемы с

  эталонным CSV и гарантирует неизменность колонок.[ref: repo:tests/golden/test_unified_schema_golden.py@test_refactoring_32]

- `tests/golden/test_cli_golden.py` выполняет CLI команды и сверяет артефакты с

  сохранёнными снэпшотами, что защищает публичный интерфейс.[ref: repo:tests/golden/test_cli_golden.py@test_refactoring_32]

## ci-и-покрытие

- Pytest конфигурация требует `--cov=src/bioetl` и `--cov-fail-under=85`.

  Обновление модулей без тестов приведёт к падению CI.[ref: repo:pyproject.toml@test_refactoring_32]

- Ruff и mypy запускаются через pre-commit; `pipeline-inventory` хук гарантирует

  актуальность документации по источникам.[ref: repo:.pre-commit-config.yaml@test_refactoring_32]

- Документ-чекер `scripts/qa/check_required_docs.py` выполняется в тестах и

  валидирует [ref: repo:docs/@test_refactoring_32] ссылки и маркеры.[ref: repo:scripts/qa/check_required_docs.py@test_refactoring_32]

## qc-отчёты-и-метрики

- `OutputWriter` записывает QC файлы (`*_quality_report.csv`, `*_correlation_report.csv`)

  и сериализует `validation_issues` в метаданные.[ref: repo:src/bioetl/core/output_writer.py@test_refactoring_32]

- Утилиты `update_summary_metrics` и `duplicate_summary` агрегируют покрытие и

  дубликаты, попадающие в итоговый QC.[ref: repo:src/bioetl/utils/qc.py@test_refactoring_32]

- Таргет-пайплайн использует `prepare_missing_mappings` для отчёта о неразрешённых

  идентификаторах.[ref: repo:src/bioetl/utils/qc.py@test_refactoring_32]

## property-based-идеи

- Расширить Hypothesis-тесты для нормализаторов единиц (`test_numeric_normalizer`) с

  генераторами произвольных значений.[ref: repo:tests/unit/test_numeric_normalizer.py@test_refactoring_32]

- Добавить property-based сценарии для `PageNumberPaginator`, проверяющие, что

  совокупность страниц всегда покрывает исходные записи без дубликатов.[ref: repo:tests/core/test_pagination_strategies.py@test_refactoring_32]

## локальные-проверки

| Команда | Назначение |
| --- | --- |
| `pytest` | Запуск полного тестового набора |
| `pytest tests/golden/test_cli_golden.py` | Проверка CLI артефактов |
| `python -m scripts.qa.check_required_docs` | Валидация ссылок и обязательных документов |
| `npx markdownlint-cli2 "**/*.md"` | Статический анализ Markdown |
| `ruff check src tests` | Проверка стиля Python |

## валидация-документации

- Все ссылки вида `[ref: repo:...]` MUST указывать на существующие пути и

  поддерживаться линк-чекером из `tests/scripts/test_doc_link_checker.py`.[ref: repo:tests/scripts/test_doc_link_checker.py@test_refactoring_32]

- При добавлении новых разделов обновляйте `docs/.pages` и `docs/INDEX.md`.
