# Качество и проверки

## цели

- Обеспечить детерминизм артефактов и воспроизводимость запусков.
- Зафиксировать покрытие тестами и golden-наборами.
- Описать локальные проверки документации и ссылок.

## тестовые-контуры

| Категория | Путь | Назначение |
| --- | --- | --- |
| Юнит-тесты | `tests/unit/` | Контракты `PipelineBase`, конфигурации, utils. |
| Тесты источников | `tests/sources/<source>/` | Клиенты, нормализаторы, merge-политики. |
| Интеграционные | `tests/integration/` | Сквозные сценарии пайплайнов. |
| Golden | `tests/golden/` | Побитовые сравнения детерминированных CSV/Parquet. |
| Производительность | `tests/perf/` | Регрессия времени и памяти. |
| Скрипты | `tests/scripts/` | CLI-хелперы и QA-инструменты. |

### маркеры-pytest

- `unit`, `integration`, `golden`, `determinism`, `qc`, `schema`, `property`, `slow` ([ref: repo:pyproject.toml@test_refactoring_32]).
- Запуск с фильтрацией: `pytest -m "unit and not slow"`.

## golden-наборы

- Расположены в `artifacts/baselines/golden_tests/`.
- Поддерживаются тестами `tests/golden/test_activity_pipeline.py` и аналогами для других сущностей.
- При изменении схем **MUST** пересчитываться через соответствующий pipeline с флагом `--extended`, затем проходить ручную ревизию diff.

## qc-отчёты пайплайна

- `UnifiedOutputWriter` формирует `quality_report.csv`, `correlation_report.csv` (если включено), `meta.yaml` с контрольными суммами ([ref: repo:src/bioetl/core/output_writer.py@test_refactoring_32]).
- Пороговые значения QC задаются в конфиге (`qc.thresholds`), например для `ActivityPipeline` контролируются `fallback.count` и `fallback.rate`.
- Нарушения QC **MUST** приводить к `ValidationError`, если `severity` ≥ `qc.severity_threshold`.

## проверки-документации

| Команда | Назначение |
| --- | --- |
| `make docs-verify` | Markdown-линт (`pymarkdown`) + линк-чекер (`lychee --offline --config .lychee.toml`). |
| `python src/scripts/run_inventory.py --config configs/inventory.yaml --check` | Проверка актуальности инвентаризационных артефактов. |

## выпуск-пайплайнов

- Перед релизом **MUST** быть выполнены:
  - `make lint format type-check`
  - `pytest` с профилем окружения (см. `docs/configs/CONFIGS.md`)
  - `python src/scripts/validate_columns.py --entity all --schema-version latest`
- Результаты запуска документируются через `meta.yaml` и прикладываются к QA-отчёту.

## property-based

- `tests/unit/test_hashing.py` и `tests/sources/*/test_normalizer_property.py` используют Hypothesis для проверки устойчивости нормализации (см. зависимости `hypothesis` в [ref: repo:pyproject.toml@test_refactoring_32]).
- Новые чистые функции **SHOULD** сопровождаться property-тестами с инвариантами (например, `hash_row` детерминирован, независим от порядка колонок).

## мониторинг-документации

- Любое изменение контрактов **MUST** фиксироваться в `CHANGELOG.md` и проверяться линк-чекером.
- Документы, затрагивающие схемы, **SHOULD** ссылаться на конкретные Pandera-классы через `[ref: repo:...]` для трассируемости.


