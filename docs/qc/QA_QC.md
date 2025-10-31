# Контроль качества и тестирование {#qa-qc}

## Тестовые уровни {#test-levels}
| Категория | Расположение | Маркер pytest | Описание |
| --- | --- | --- | --- |
| Unit | `tests/unit` | `@pytest.mark.unit` | Тесты базовых компонентов (конфиги, логгер, пайплайны). |
| Integration | `tests/integration` | `@pytest.mark.integration` | Проверка взаимодействия стадий, включая enrichment. |
| Golden | `tests/golden` | `@pytest.mark.golden` | Сравнение детерминированных артефактов CSV/JSON. |
| QC | `tests/integration/qc` | `@pytest.mark.qc` | Проверка агрегаторов QC и отчётов. |
| Sources | `tests/sources` | `@pytest.mark.integration` | E2E проверки внешних клиентов (PubChem, Document). |
| Scripts | `tests/scripts` | `@pytest.mark.integration` | Проверка CLI-реестра и entrypoints. |

Pytest конфигурация фиксирует строгие параметры (`--strict-markers`, coverage 85%) в
[`pyproject.toml`][ref: repo:pyproject.toml@test_refactoring_32].

## Golden и детерминизм {#golden-tests}
- Детерминированные наборы в `tests/golden/data` контролируют хеши строк, QC и метаданные.
- Пайплайны MUST сохранять `hash_row` и `hash_business_key` в порядке из конфигов, иначе golden-тесты падают.
- Файл [`tests/golden/test_cli_golden.py`][ref: repo:tests/golden/test_cli_golden.py@test_refactoring_32]
  гарантирует, что CLI запускает пайплайны с одинаковым выводом на фиксированных входах.

## QC сбор и отчёты {#qc-policies}
- Метрики обновляются через
  [`update_summary_metrics`][ref: repo:src/bioetl/utils/qc.py@test_refactoring_32] и
  [`update_summary_section`][ref: repo:src/bioetl/utils/qc.py@test_refactoring_32].
- Валидационные ошибки агрегируются функцией
  [`_summarize_schema_errors`][ref: repo:src/bioetl/utils/validation.py@test_refactoring_32].
- Пайплайны SHOULD регистрировать отсутствующие маппинги в `qc_missing_mappings`, как в
  [`TargetPipeline._materialize_silver`][ref: repo:src/bioetl/sources/chembl/target/pipeline.py@test_refactoring_32].

## Валидация схем {#schema-validation}
- Pandera схемы расположены в `src/bioetl/schemas/*` и покрываются тестами:
  - [`tests/schemas/test_activity_schema.py`][ref: repo:tests/schemas/test_activity_schema.py@test_refactoring_32]
  - [`tests/schemas/test_document_schema.py`][ref: repo:tests/schemas/test_document_schema.py@test_refactoring_32]
- При нарушении схемы поднимается `SchemaValidationError`, обработчик записывает issue в
  [`PipelineBase.record_validation_issue`][ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32].

## Property-based и перформанс {#property-perf}
- Hypothesis-тесты расположены в
  [`tests/unit/test_property_normalizers.py`][ref: repo:tests/unit/test_property_normalizers.py@test_refactoring_32] (пример).
- Перформанс-бенчмарки (`pytest-benchmark`) находятся в `tests/perf` и контролируют время
  исполнения критичных функций.

## CI и локальные проверки {#ci-local}
- GitHub Actions workflow запускает pytest, mypy, ruff (см.
  [`.github/workflows/ci.yaml`][ref: repo:.github/workflows/ci.yaml@test_refactoring_32]).
- Локально обязательны pre-commit хуки
  ([`.pre-commit-config.yaml`][ref: repo:.pre-commit-config.yaml@test_refactoring_32])
  включающие линтеры, mypy и генератор инвентаризации.
- Новый набор включает markdownlint и link-check (см. раздел CHANGELOG).

## Инварианты QA/QC {#qa-invariants}
- Любой пайплайн MUST завершать `run()` без незакрытых HTTP-клиентов;
  тест `tests/unit/test_pipelines.py::test_clients_closed` контролирует это.
- Отчёты QC MUST содержать `run_id`, `pipeline_version`, `chembl_release` —
  проверяется golden-тестами и валидируется writer-ом.
- При нарушении порогов QC (`qc.thresholds`) пайплайн MUST помечать выпуск как ошибочный
  и возвращать код завершения ≠ 0, что проверяется интеграционными тестами target/document.
