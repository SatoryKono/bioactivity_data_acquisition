# Changelog

## Unreleased

### Изменено

- Проведена финальная консолидация констант/паттернов ChEMBL: API-поля
  вынесены в `bioetl.pipelines.chembl._constants`, `ChemblPipelineBase`
  использует общие helpers из `bioetl.clients.base`, `Paginator` логирует
  страницы, а схемы/валидаторы получили единые проверки `RELATIONS`.
- Устранён цикл `bioetl.config ↔ bioetl.pipelines.base`: пакет конфигурации
  переключён на ленивые реэкспорты, а `PipelineBase` использует новый
  `PipelineConfigProtocol` из `bioetl.core.config_contracts`, что разблокировало
  запуск тестов.
- Добавлен слой `load_meta`: словари, Pandera-схема, `LoadMetaStore`,
  прокидка `load_meta_id` в ChEMBL-пайплайны, новые тесты и документация.
- Унифицированы ChEMBL-пайплайны: `ChemblActivityPipeline` и
  `ChemblAssayPipeline` наследуются от `ChemblPipelineBase`, а
  `PipelineBase.write()` переиспользует `plan_run_artifacts` с поддержкой
  пользовательского `run_directory`.
- Обновлены Pandera-схемы (`assay`, `target`, `testitem`): добавлены
  обязательные hash-колонки, повышены `SCHEMA_VERSION`, усилены проверки
  `SchemaRegistry` и `schema_guard.py`.
- Расширены тесты: интеграционный сценарий жизненного цикла пайплайна и
  юнит-контроль реестра схем.
- Добавлен модуль `bioetl.schemas.pipeline_contracts` с helper-функциями
  (`get_pipeline_contract/get_out_schema/get_business_key_fields`), пайплайны
  ChEMBL и `PipelineBase` больше не хардкодят строковые идентификаторы схем; в
  docs обновлён раздел про контракты, добавлены юнит-тесты helper’ов.

### Инструменты

- `scripts/schema_guard.py` валидирует реестр схем (версии, дубликаты,
  hash-поля) и пишет отчёт `artifacts/schema_guard_report.md`.
- Утилиты перенесены в `tools.*`, добавлены консольные entry points
  `bioetl-*`, каталог `scripts/` удалён. Smoke-тесты CLI добавлены в
  `tests/integration/cli/test_tools_cli.py`.
