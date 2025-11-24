# Changelog

## Unreleased

### Изменено

- Проведена финальная консолидация констант/паттернов ChEMBL: API-поля
  вынесены в `bioetl.pipelines.chembl._constants`, `ChemblPipelineBase`
  использует общие helpers из `bioetl.clients.base`, `Paginator` логирует
  страницы, а схемы/валидаторы получили единые проверки `RELATIONS`.
- Устранён цикл `bioetl.config ↔ bioetl.core.pipeline`: пакет конфигурации
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
- Нормализована структура ChEMBL-пайплайнов: `PipelineBase` и ошибки перенесены
  в `bioetl.core.pipeline`, общие helper’ы из `bioetl.pipelines.common/*` и
  `pipelines/qc/*` вынесены в `bioetl.chembl.common`/`bioetl.qc`, каждая
  сущность ChEMBL получила stage-модули (`extract/transform/validate/normalize/write`),
  а отчёты `artifacts/pipelines_inventory.csv` и `artifacts/pipelines_orphans.csv`
  фиксируют соответствие политике `PIPE-004`.
- CLI-слой очищен от dev-инструментов: `bioetl.cli.cli_app` регистрирует
  только пайплайновые команды и использует новый фасад
  `bioetl.core.runtime.cli_feedback` для унифицированного вывода.
- Фасад `bioetl.core` помечает `ChemblReleaseMixin` и
  `join_activity_with_molecule` как устаревшие shim-обёртки над
  `bioetl.chembl.common.*`; при импорте из `bioetl.core` теперь
  выдаётся `DeprecationWarning`.
- Инвентаризированы импорты из `bioetl.core`: рабочий код и тесты переведены на
  прямые обращения к подмодулям (`bioetl.core.http`, `.logging`, `.io`,
  `.schema`, `.runtime`), а фасад оставлен только как compat-слой.

### Устаревшее

- Устаревшие шорткаты из `_DEPRECATED_EXPORTS` (`ChemblReleaseMixin`,
  `join_activity_with_molecule`, `BaseApiClient`, `IParser`, `INormalizer`)
  продолжают выдавать `DeprecationWarning` при импорте из `bioetl.core` и будут
  удалены после следующего релиза; используйте исходные модули
  (`bioetl.chembl.common.release_tracker`, `bioetl.chembl.common`,
  `bioetl.base_classes`).

### Удалено

- Удалён устаревший слой `bioetl.config.models`: корневой пакет больше не
  реэкспортирует модели, а модуль `bioetl/config/models.py` исключён. Импорты
  должны указывать на `bioetl.config.models.models` или `.policies`.
- Пакет `bioetl.core` больше не должен использоваться как точка доступа к
  `BaseApiClient`, `IParser` и `INormalizer`; используйте
  `bioetl.base_classes` напрямую (compat-импорт временно сохранён).
- Убран легаси-префикс `BIOACTIVITY__` и связанные DeprecationWarning — для
  оверрайдов конфигурации теперь поддерживается только `BIOETL__` (при
  необходимости можно передать пользовательский префикс в `load_config`).

### Инструменты

- `scripts/schema_guard.py` валидирует реестр схем (версии, дубликаты,
  hash-поля) и пишет отчёт `artifacts/schema_guard_report.md`.
- Все dev-утилиты перенесены из `bioetl.cli.tools.*` в `scripts/*.py`,
  логика живёт в `bioetl.devtools.*`, а список миграций обновляется в
  `artifacts/cli_tools_migration.csv`. Консольные entry points `bioetl-*`
  удалены в пользу запуска `python scripts/<name>.py`.
