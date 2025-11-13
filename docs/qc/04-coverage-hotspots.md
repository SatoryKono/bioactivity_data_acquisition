# Coverage hotspots (pytest-cov 2025-11-08)

Ниже перечислены участки кода с наибольшими потерями покрытия в полном прогоне `pytest` (выход cov: 72.13% при требовании 85%). Процентные значения считывались из свежего отчёта `coverage.xml`.

## 1. High-priority

- `bioetl.config.models.*`: новые модульные схемы конфигов пока без dedicated-тестов; следует покрыть валидаторы (`base`, `determinism`, `http`, `io`) smoke-кейсами и негативными сценариями.
- `bioetl.tools.*` (audit_docs, build_vocab_store, run_test_report, vocab_audit, link_check и т. д.): 20-35% или ниже, многие ветви CLI остаются нетестированными; следует писать unit-тесты с полным мокированием FS/HTTP.
- `bioetl.cli.tools.*`: среднее покрытие 50-75%, есть необработанные ветви ошибок и проверки аргументов.
- `bioetl.core.load_meta_store` (79%), `bioetl.core.output` (87%), `bioetl.core.hashing` (80%): отсутствуют негативные сценарии и проверки исключений; можно покрыть детерминизм/атомарные записи.

## 2. Pipeline layer

- `bioetl.pipelines.activity` (71%), `document` (67%), `target` (67%), `testitem` (58%): тяжёлые ETL последовательности. Львиная доля пропусков лежит в ветках обогащения, fallback-логики и отчётов.
- `bioetl.pipelines.activity.activity_enrichment` и `join_molecule`: 70-78%. Недостаёт тестов на edge-case входы и ретраи.
- `bioetl.pipelines.base`: 77% - базовый оркестратор, отсутствуют тесты на обработку исключений стадий.

## 3. Config & registry

- `bioetl.config.loader`: 82% при 51 незакрытой строке - ветви с `extends`, циклическими include и ошибками чтения YAML.
- `bioetl.config.testitem.__init__`: 49% - требуется smoke покрытие для комбинаций профилей.
- `bioetl.schemas.activity.*`: 68% - недостаёт проверок специфичных ограничений.

## 4. CLI / QC / вспомогательные сервисы

- `bioetl.cli.command` (78%) и `cli.tools.*`: улучшить тесты на разбор опций, сообщения об ошибках.
- `bioetl.tools.semantic_diff` (58%), `schema_guard` (84%) - уже есть база тестов, но остаются ветви с обработкой ошибок ввода/FS.
- QC-подсистема (`bioetl.qc.metrics`, `bioetl.qc.report`) почти закрыта, но нужно гарантировать отрицательные сценарии (низкий риск).

## 5. Observed anti-patterns

- Крупные функции без выделения зависимостей усложняют unit-тесты (`pipelines.*`, `tools.*`).
- Нехватка фиктивных данных/фикстур для быстрых smoke-сценариев пайплайнов.
- Нет отдельных профилей покрытия по подсистемам, что затрудняет мониторинг прогресса.

## Next steps

1. Сконцентрироваться на high-priority списке (config models, tools, core IO).
2. Подготовить лёгкие фикстуры для пайплайнов, чтобы покрыть fallback-ветви без запуска реального ETL.
3. Ввести модульные отчёты покрытия (например, `pytest --cov=src/bioetl/tools`), чтобы отслеживать прогресс в CI.
