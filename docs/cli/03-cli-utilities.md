# CLI Utilities Inventory

Это инвентаризация всех служебных утилит, поставляемых вместе с репозиторием. Каждая
утилита оформлена как Typer-приложение в `bioetl.cli.tools.<name>` и публикуется как
консольная команда с префиксом `bioetl-`. По умолчанию артефакты записываются в
`artifacts/`, если не указано иное.

| Command | Module | Назначение | Ключевые артефакты | Пример запуска |
| --- | --- | --- | --- | --- |
| `bioetl-audit-docs` | `bioetl.cli.tools.audit_docs` | Аудит документации, поиск битых ссылок и пробелов по пайплайнам. | `artifacts/GAPS_TABLE.csv`, `artifacts/LINKCHECK.md`. | `bioetl-audit-docs --artifacts artifacts` |
| `bioetl-build-vocab-store` | `bioetl.cli.tools.build_vocab_store` | Агрегирует справочники из `configs/dictionaries` в единый YAML. | `artifacts/chembl_dictionaries.yaml` (либо путь из `--output`). | `bioetl-build-vocab-store --src configs/dictionaries --output artifacts/chembl_vocab.yaml` |
| `bioetl-catalog-code-symbols` | `bioetl.cli.tools.catalog_code_symbols` | Извлекает сигнатуры `PipelineBase`, модели конфигов и зарегистрированные CLI. | `artifacts/code_signatures.json`, `artifacts/cli_commands.txt`. | `bioetl-catalog-code-symbols` |
| `bioetl-check-comments` | `bioetl.cli.tools.check_comments` | Заготовка проверки TODO/комментариев (возвращает предупреждение). | — | `bioetl-check-comments --root .` |
| `bioetl-check-output-artifacts` | `bioetl.cli.tools.check_output_artifacts` | Быстрая проверка на наличие тяжёлых файлов в `data/output`. | Сообщения в STDERR, возврат кода 1 при нарушениях. | `bioetl-check-output-artifacts --max-bytes 2000000` |
| `bioetl-create-matrix-doc-code` | `bioetl.cli.tools.create_matrix_doc_code` | Формирует матрицу трассировки документация ↔ код. | `artifacts/matrix-doc-code.csv`, `artifacts/matrix-doc-code.json`. | `bioetl-create-matrix-doc-code` |
| `bioetl-determinism-check` | `bioetl.cli.tools.determinism_check` | Запускает пайплайны в `--dry-run` и сравнивает структурные логи. | `artifacts/DETERMINISM_CHECK_REPORT.md`. | `bioetl-determinism-check --pipeline activity_chembl` |
| `bioetl-doctest-cli` | `bioetl.cli.tools.doctest_cli` | Исполняет CLI-примеры из документации с безопасным `--dry-run`. | `artifacts/CLI_DOCTEST_REPORT.md`. | `bioetl-doctest-cli` |
| `bioetl-inventory-docs` | `bioetl.cli.tools.inventory_docs` | Создаёт список и SHA256-хеши markdown-документации. | `artifacts/docs_inventory.txt`, `artifacts/docs_hashes.txt`. | `bioetl-inventory-docs --docs-root docs` |
| `bioetl-link-check` | `bioetl.cli.tools.link_check` | Запускает `lychee` со штатной конфигурацией, формирует отчёт или заглушку. | `artifacts/link-check-report.md`. | `bioetl-link-check` |
| `bioetl-remove-type-ignore` | `bioetl.cli.tools.remove_type_ignore` | Удаляет директивы `type: ignore` из исходников. | — | `bioetl-remove-type-ignore --root src` |
| `bioetl-run-test-report` | `bioetl.cli.tools.run_test_report` | Запускает `pytest`, собирает отчёты и выпускает `meta.yaml`. | Каталог внутри `TEST_REPORTS_ROOT`. | `bioetl-run-test-report --output-root artifacts/test-reports` |
| `bioetl-schema-guard` | `bioetl.cli.tools.schema_guard` | Валидирует конфиги пайплайнов и контролирует целостность Pandera-схем. | `artifacts/SCHEMA_GUARD_REPORT.md`. | `bioetl-schema-guard` |
| `bioetl-semantic-diff` | `bioetl.cli.tools.semantic_diff` | Сравнивает документацию и реализацию ключевых API. | `artifacts/semantic-diff-report.json`. | `bioetl-semantic-diff` |
| `bioetl-vocab-audit` | `bioetl.cli.tools.vocab_audit` | Сравнивает значения из ChEMBL с локальными словарями. | `artifacts/vocab_audit.csv`, `artifacts/vocab_audit.meta.yaml`. | `bioetl-vocab-audit --pages 5 --page-size 500` |

Все CLI команды доступны после `pip install -e .[dev]` и исполняются из корня репозитория,
чтобы относительные пути разрешались корректно.
