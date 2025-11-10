# Changelog

## Unreleased

### Изменено

- Добавлен слой `load_meta`: словари, Pandera-схема, `LoadMetaStore`, прокидка `load_meta_id` в ChEMBL-пайплайны, новые тесты и документация.
- Унифицированы ChEMBL-пайплайны: `ChemblActivityPipeline` и `ChemblAssayPipeline` наследуются от `ChemblPipelineBase`, а `PipelineBase.write()` переиспользует `plan_run_artifacts` с поддержкой пользовательского `run_directory`.
- Обновлены Pandera-схемы (`assay`, `target`, `testitem`): добавлены обязательные hash-колонки, повышены `SCHEMA_VERSION`, усилены проверки `SchemaRegistry` и `schema_guard.py`.
- Расширены тесты: интеграционный сценарий жизненного цикла пайплайна и юнит-контроль реестра схем.
- Унифицирован нейминг I/O-хелперов и канонические сигнатуры:

  | Старое имя                | Новое имя                |
  | ------------------------ | ----------------------- |
  | `load_config`            | `read_pipeline_config`  |
  | `load_environment_settings` | `read_environment_settings` |
  | `load_vocab_store`       | `read_vocab_store`      |
  | `_normalise_base_url`    | `_normalize_base_url`   |

  Алиасы со старым API сохраняются на один релиз и выбрасывают `DeprecationWarning`.
- Обновлены гайды по нормализации имён (`docs/styleguide/00-naming-conventions.md`, `docs/styleguide/VIOLATIONS_TABLE.md`, `docs/INDEX.md`) и добавлен onboarding-чеклист с ссылками на план.
- В `pyproject.toml` расширены whitelist-правила `ruff`/`flake8-naming` для защищённых идентификаторов и вынесен набор `STANDARD_RELATIONS` в `assay_transform` на модульный уровень для корректного контроля нейминга.

### Инструменты

- `scripts/schema_guard.py` валидирует реестр схем (версии, дубликаты, hash-поля) и пишет отчёт `artifacts/SCHEMA_GUARD_REPORT.md`.
- Утилиты перенесены в `bioetl.cli.tools.*`, добавлены консольные entry points `bioetl-*`, каталог `scripts/` удалён. Smoke-тесты CLI добавлены в `tests/integration/cli/test_tools_cli.py`.
