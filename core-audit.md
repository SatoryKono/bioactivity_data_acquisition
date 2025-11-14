# Аудит `src/bioetl/core` (2025‑11‑14)

## 1. Неиспользуемые элементы и мёртвый код

| Элемент | Путь и строки | Наблюдения | Рекомендация |
| --- | --- | --- | --- |
| Совместимый модуль `bioetl.core.config` | `src/bioetl/core/config/__init__.py` L1-L11 | Файл удалён, совместимость закрыта, документация ссылается на `config_contracts`. | ✓ Выполнено (2025‑11‑14). |
| Шим `bioetl.core.interfaces` | `src/bioetl/core/interfaces.py` L1-L35 | Модуль удалён, рекомендации вынесены в docs. | ✓ Выполнено. |
| Совместимый `base_pipeline_compat` | `src/bioetl/core/runtime/base_pipeline_compat.py` L1-L21 | Реэкспорт удалён; потребители используют `bioetl.pipelines.base`. | ✓ Выполнено. |
| Классы `BaseSourceParameters` и `BaseSourceConfig` | `src/bioetl/core/runtime/base_source.py` L11-L16 | Шим удалён, рекомендуется прямой импорт моделей из `bioetl.config.models.source`. | ✓ Выполнено. |
| Метод `LoadMetaStore.write_dataframe` | `src/bioetl/core/runtime/load_meta_store.py` L222-L237 | Публичный метод удалён, `_write_dataframe` остался единой точкой записи. | ✓ Выполнено. |
| Функция `finalise_output` | `src/bioetl/core/io/output.py` L384-L437 | Удалена, пайплайны используют `build_write_artifacts` + `emit_qc_artifact`. | ✓ Выполнено. |
| Помощники `stage_event`, `client_event`, `emit` | `src/bioetl/core/logging/log_events.py` L437-L457 | Удалены; naming-гайд обновлён (`docs/logging/08-event-inventory.md`). | ✓ Выполнено. |
| Набор событий `LogEvents` без потребителей | `src/bioetl/core/logging/log_events.py` L79-L412 | 26 неиспользуемых констант удалены, остались только реально используемые события. | ✓ Выполнено. |
| Алиасы `SchemaColumnFactory.chembl_*` и `bao_term_id` | `src/bioetl/core/schema/column_factory.py` L175-L208 | Алиасы удалены, схемы используют базовые фабрики `chembl_id`/`bao_id`. | ✓ Выполнено. |
| Логгерные утилиты `bind_global_context`, `reset_global_context` | `src/bioetl/core/logging/logger.py` L215-L288 | Удалены, `UnifiedLogger.bind/reset` напрямую работают с `contextvars`. | ✓ Выполнено. |

## 2. Дублирующаяся логика CLI ↔ core

### 2.1 Обработка ошибок

- `src/bioetl/cli/cli_command.py` L470-L543 и каждый CLI-tool (`cli/tools/vocab_audit.py` L83-L103, `cli/tools/catalog_code_symbols.py` L42-L54, `cli/tools/determinism_check.py` L37-L65) вручную печатают ошибки через `typer.secho` и `exit_with_code`. Это дублирует `bioetl.core.runtime.cli_errors.emit_cli_error` (структурные логи + детерминированный stderr).

  **Предложение:** вынести общий `emit_tool_error()` в `bioetl.core.runtime.cli_errors` (или `CliCommandBase`) и использовать его во всех CLI-командах.

### 2.2 Логирование

- `PipelineCliCommand.handle` (`cli/cli_command.py` L192-L228) вручную логирует старт/финиш через `LogEvents.CLI_*`, тогда как в `log_events.py` уже есть неиспользуемые `PIPELINE_*`/`STAGE_*` события. В результате логи CLI и пайплайна расходятся, а перечисленные события остаются мёртвыми.

  **Предложение:** перенести start/finish-логирование в `PipelineCommandRunner` (core) и использовать существующие `LogEvents.PIPELINE_*`, чтобы CLI и пайплайны делили один формат.

### 2.3 Нормализация CLI-параметров

- `CommonOptions` (`cli/cli_command.py` L45-L75) и `_coerce_option_value` / `_parse_set_overrides` / `_validate_*` (L113-L290) повторяют структуру `PipelineCommandOptions` (`core/runtime/pipeline_command_runner.py` L85-L116) и проверок в `PipelineConfigFactory`. Нормализация значений Typer и проверка путей реализованы дважды.

  **Предложение:** перенести коэрсинг и валидацию в `PipelineCommandRunner` (или `CliCommandBase`) и переиспользовать из CLI через общий helper.

### 2.4 Фабрики клиентов

- CLI-инструменты (`bioetl.tools.vocab_audit` L86-L102 и др.), запускаемые через `bioetl.cli.tools.*`, создают ChEMBL-клиентов напрямую (`chembl_webresource_client.new_client`) и логируют через собственные события. Это дублирует `bioetl.core.http.client_factory.APIClientFactory` + `ChemblEntityClientFactory`, что используются в пайплайнах.

  **Предложение:** добавить shared helper (`core.http.client_factory.for_tool`) и заставить CLI-инструменты получать HTTP/ChEMBL клиентов через него, чтобы соблюдались единые лимиты/таймауты.

## 3. Нарушения NR-001…NR-007 и rename‑планы

| Rule | Нарушение | Предложение |
| --- | --- | --- |
| NR-004 (CLI-модули `cli_*.py`) | `src/bioetl/core/runtime/pipeline_command_runner.py` содержит чисто CLI-логику (подготовка команд и ошибок), но не имеет префикса `cli_`. | Переименовать файл в `cli_pipeline_command_runner.py` и обновить импорты (`bioetl.cli.cli_command`, `tests/...`). |

Прочие правила (NR-001 — snake_case модулей, NR-002 — PascalCase классов, NR-003 — snake_case функций, NR-005/NR-006/NR-007 — приватные/ду́ндеры/исключения) нарушений в `src/bioetl/core/**` не выявили.

## 4. Публичный API core без потребителей

| Символ | Определение | Факт использования | Действие |
| --- | --- | --- | --- |
| `BaseSourceParameters`, `BaseSourceConfig` | `runtime/base_source.py` L11-L16 | Нет импортов вне совместимых шима/теста. | ✓ Удалены из публичного API. |
| `finalise_output` | `io/output.py` L384-L437 | Не вызывается пайплайнами или CLI; экспорт держит только тест `test_core_init`. | ✓ Удалена, заменена `build_write_artifacts` + `emit_qc_artifact`. |
| `LoadMetaStore.write_dataframe` | `runtime/load_meta_store.py` L222-L237 | Не вызывается (`rg` показал 0). | ✓ Удалён публичный метод, используется `_write_dataframe`. |
| `client_event`, `stage_event`, `emit` | `logging/log_events.py` L437-L457 | Нет обращений. | ✓ Удалены; naming-гайд обновлён. |
| `LogEvents` перечисления из списка §1 | `logging/log_events.py` L79-L412 | Не используются, но засоряют API. | ✓ 26 событий удалены, остались только используемые. |
| `SchemaColumnFactory.chembl_* / bao_term_id` | `schema/column_factory.py` L175-L208 | Не используются в схемах, хотя дублируют `chembl_id`. | ✓ Алиасы удалены. |
| `bind_global_context`, `reset_global_context` | `logging/logger.py` L215-L288 | Не вызываются вне модуля. | ✓ Удалены, `UnifiedLogger` работает напрямую. |

> Прочие публичные функции/классы из `bioetl.core.__all__` имеют потребителей (пайплайны, `bioetl.tools`, тесты) и требуют только обычной поддержки.
