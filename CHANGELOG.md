# Changelog

## Unreleased

### Изменено
- Унифицированы ChEMBL-пайплайны: `ChemblActivityPipeline` и `ChemblAssayPipeline` наследуются от `ChemblPipelineBase`, а `PipelineBase.write()` переиспользует `plan_run_artifacts` с поддержкой пользовательского `run_directory`.
- Обновлены Pandera-схемы (`assay`, `target`, `testitem`): добавлены обязательные hash-колонки, повышены `SCHEMA_VERSION`, усилены проверки `SchemaRegistry` и `schema_guard.py`.
- Расширены тесты: интеграционный сценарий жизненного цикла пайплайна и юнит-контроль реестра схем.

### Инструменты
- `scripts/schema_guard.py` валидирует реестр схем (версии, дубликаты, hash-поля) и пишет отчёт `artifacts/SCHEMA_GUARD_REPORT.md`.
- Утилиты перенесены в `bioetl.cli.tools.*`, добавлены консольные entry points `bioetl-*`, каталог `scripts/` удалён. Smoke-тесты CLI добавлены в `tests/integration/cli/test_tools_cli.py`.

