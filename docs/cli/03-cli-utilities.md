# CLI Utilities Inventory

Это инвентаризация всех служебных утилит, поставляемых вместе с репозиторием.
Каждая утилита запускается через `python scripts/<name>.py` (Typer-обёртка)
и делегирует бизнес-логику в модуль `bioetl.devtools.cli_<name>`. Если не
указано иное, артефакты записываются в `artifacts/`.

| Команда                                                            | Основные опции                                                                   | Назначение                                                              | Ключевые артефакты                                            | Пример запуска                                                                                      |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `bioetl-audit-docs`                                                | `--artifacts PATH`                                                               | Аудит документации, поиск пробелов по пайплайнам и битых ссылок.        | `GAPS_TABLE.csv`, `LINKCHECK.md` в каталоге из `--artifacts`. | `bioetl-audit-docs --artifacts artifacts`                                                           |
| `bioetl-build-vocab-store`                                         | `--src`, `--output`                                                              | Сборка агрегированного словаря ChEMBL из YAML в `configs/dictionaries`. | Агрегированный YAML (путь из `--output`).                     | `bioetl-build-vocab-store --src configs/dictionaries --output artifacts/chembl_vocab.yaml`          |
| `bioetl-catalog-code-symbols`                                      | `--artifacts PATH`                                                               | Каталогизация CLI, конфигов и сущностей пайплайнов.                     | `code_signatures.json`, `cli_commands.txt`.                   | `bioetl-catalog-code-symbols --artifacts artifacts/code-symbols`                                    |
| `bioetl-check-comments`                                            | `--root PATH`                                                                    | Проверка TODO/комментариев и статуса реализации.                        | Вывод в STDOUT, код возврата.                                 | `bioetl-check-comments --root src`                                                                  |
| [`bioetl-check-output-artifacts`](../qc/05-check-output-artifacts.md) | `--max-bytes`                                                                    | Проверка крупных файлов в `data/output`.                                | Сообщения в STDERR/STDOUT, код возврата.                      | `bioetl-check-output-artifacts --max-bytes 2000000`                                                 |
| `bioetl-create-matrix-doc-code`                                    | `--artifacts PATH`                                                               | Формирование матрицы соответствия документации и кода.                  | `matrix-doc-code.csv`, `matrix-doc-code.json`.                | `bioetl-create-matrix-doc-code --artifacts artifacts/matrix`                                        |
| `bioetl-determinism-check`                                         | `--pipeline,-p`                                                                  | Проверка детерминизма пайплайна через двойной прогон и сравнение логов. | `DETERMINISM_CHECK_REPORT.md`.                                | `bioetl-determinism-check --pipeline activity_chembl`                                               |
| `bioetl-doctest-cli`                                               | —                                                                                | Запуск CLI-примеров из документации и отчёт по статусу.                 | `cli_doctest_report.md`.                                      | `bioetl-doctest-cli`                                                                                |
| `bioetl-inventory-docs`                                            | `--inventory PATH`, `--hashes PATH`                                              | Каталогизация Markdown-доков и расчёт SHA256.                           | `docs_inventory.txt`, `docs_hashes.txt`.                      | `bioetl-inventory-docs --inventory artifacts/docs_inventory.txt --hashes artifacts/docs_hashes.txt` |
| `bioetl-link-check`                                                | `--timeout`                                                                      | Линк-чекер документации через `lychee`.                                 | `link-check-report.md`.                                       | `bioetl-link-check --timeout 600`                                                                   |
| `bioetl-remove-type-ignore`                                        | `--root PATH`                                                                    | Удаление директив `type: ignore` в исходниках.                          | Вывод в STDOUT, код возврата.                                 | `bioetl-remove-type-ignore --root src`                                                              |
| `bioetl-run-test-report`                                           | `--output-root`                                                                  | Запуск `pytest` и сбор артефактов (coverage, meta.yaml).                | Каталог отчётов внутри `output-root`.                         | `bioetl-run-test-report --output-root artifacts/test-reports`                                       |
| `bioetl-schema-guard`                                              | —                                                                                | Проверка конфигов пайплайнов и Pandera-реестра.                         | `schema_guard_report.md`.                                     | `bioetl-schema-guard`                                                                               |
| `bioetl-semantic-diff`                                             | —                                                                                | Семантическое сравнение документации и кода.                            | `semantic-diff-report.json`.                                  | `bioetl-semantic-diff`                                                                              |
| `bioetl-vocab-audit`                                               | `--store PATH`, `--output PATH`, `--meta PATH`, `--pages INT`, `--page-size INT` | Аудит словарей ChEMBL с выгрузкой отчётов и метаданных.                 | CSV отчёт и `meta.yaml` (пути из опций).                      | `bioetl-vocab-audit --store data/cache/vocab.yaml --pages 5 --page-size 500`                        |

`scripts/run_test_report.py` использует `bioetl.devtools.cli_run_test_report`
и `bioetl.tools.test_report_artifacts` для формирования каталога отчётов и
`meta.yaml`. Тесты обращаются к тем же определениям, что и CLI, поэтому
импорт из `tests.bioetl` не требуется.

Все CLI команды доступны после `pip install -e .[dev]` и запускаются из корня
репозитория, чтобы относительные пути разрешались корректно.

## Структурная обработка ошибок

- Все инструменты **должны** использовать `bioetl.core.runtime.cli_errors.emit_tool_error`.
- Helper логирует событие `LogEvents.CLI_RUN_ERROR`, добавляет `error_code/label`
  и печатает детерминированную строку `[bioetl-cli] ERROR <code>: <message>` в
  `stderr`.
- Внешние ошибки (`BioETLError`, `CircuitBreakerOpenError`, `HTTPError`,
  `Timeout`) завершаются через `exit_code=3`, остальные — через `exit_code=1`.
- Сообщения должны включать бизнес‑контекст (команда, путь к отчёту, параметры),
  а структурный `context` дополняется `error_code` и `error_label`.
