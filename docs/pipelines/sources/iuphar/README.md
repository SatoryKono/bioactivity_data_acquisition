# Guide to Pharmacology (IUPHAR)

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

## Maintainers

- `@SatoryKono` — глобальный ответственный за код и документацию, утверждает изменения по источнику IUPHAR.【F:.github/CODEOWNERS†L5-L41】

## Требования к содержанию карточки источника

- Карточка источника обязана документировать публичный API, конфигурацию и merge-политику согласно `MODULE_RULES.md` для `docs/pipelines/sources/<source>/README.md`.【F:refactoring/MODULE_RULES.md†L37-L42】【F:refactoring/MODULE_RULES.md†L146-L149】
- `DATA_SOURCES.md` задаёт обязательные поля карточки (`source`, `public_api`, `config_keys`, `merge_policy`), применимые к IUPHAR как внешнему энрихеру таргетов.【F:refactoring/DATA_SOURCES.md†L63-L76】

## Public API

- `bioetl.sources.iuphar.pipeline.GtpIupharPipeline` — полнофункциональный ETL для Guide to Pharmacology: пагинация `/targets` и `/targets/families`, нормализация и материализация gold-слоя.【F:src/bioetl/sources/iuphar/pipeline.py†L1-L160】
- `bioetl.sources.iuphar.service.IupharService` (`IupharServiceConfig`) — сервис для сопоставления IUPHAR-идентификаторов и классификаций в таргет-пайплайне, экспортируется через `__all__`.【F:src/bioetl/sources/iuphar/__init__.py†L1-L10】

## Module layout

- Источник содержит выделенные слои: HTTP клиент (`client/IupharClient`), билдеры запросов и пагинацию (`request.py`, `pagination/PageNumberPaginator`), парсер, normalizer и schema, соответствуя структуре из `MODULE_RULES`.【F:src/bioetl/sources/iuphar/client/__init__.py†L1-L80】【F:src/bioetl/sources/iuphar/pagination/__init__.py†L1-L60】【F:src/bioetl/sources/iuphar/schema/__init__.py†L1-L60】

## Configuration keys

- `configs/pipelines/iuphar.yaml` определяет HTTP-профиль (`http.iuphar` с таймаутами, ретраями, лимитами, заголовками), параметры кэша (`cache_enabled`, `cache_ttl`), ключ API (`headers.x-api-key`), каталог материализации и QC-порог `enrichments.iuphar.min` (0.6).【F:src/bioetl/configs/pipelines/iuphar.yaml†L1-L103】
- CLI блок в том же профиле регистрирует `default_config`, допустимые режимы (`default`, `smoke`) и включённую проверку колонок, что используется в скрипте Typer.【F:src/bioetl/configs/pipelines/iuphar.yaml†L104-L107】

## Merge policy

- Матрица источников фиксирует, что таргеты агрегируют ChEMBL (основа), UniProt (имена/гены) и IUPHAR (классификация); приоритеты: UniProt > ChEMBL для маркерных полей, IUPHAR > ChEMBL для классов.【F:refactoring/DATA_SOURCES.md†L35-L37】
- `TargetPipeline` использует `IupharService` и paginator для дозагрузки классификаций, добавляя их к данным ChEMBL; обогащённые поля входят в итоговый gold-слой (например, `iuphar_type`, `iuphar_class`, `iuphar_subclass`).【F:src/bioetl/sources/chembl/target/pipeline.py†L69-L156】

## Tests

- `tests/unit/test_iuphar_pipeline.py` проверяет экстракцию, материализацию и интеграцию пагинатора с мок-клиентом.【F:tests/unit/test_iuphar_pipeline.py†L1-L80】
- Пакетные тесты `tests/sources/iuphar/test_client.py`, `test_parser.py`, `test_normalizer.py`, `test_schema.py`, `test_pipeline_e2e.py` покрывают соответствующие слои клиента, парсера, нормализации и end-to-end-проходы.【F:tests/sources/iuphar/test_client.py†L1-L80】【F:tests/sources/iuphar/test_pipeline_e2e.py†L1-L60】
