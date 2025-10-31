# BioETL Refactoring Audit (test_refactoring_32)

> **Примечание:** Структура `src/bioetl/sources/` — правильная организация для внешних источников данных. Внешние источники (crossref, pubmed, openalex, semantic_scholar, iuphar, uniprot) имеют правильную структуру с подпапками (client/, request/, parser/, normalizer/, output/, pipeline.py). Для ChEMBL существует дублирование между `src/bioetl/pipelines/` (монолитные файлы) и `src/bioetl/sources/chembl/` (прокси).

## Executive summary
- **P0** — Документы в `refactoring/` предполагают дерево `src/bioetl/sources/<source>/...`, которого нет в текущей кодовой базе: фактические пайплайны расположены в монолитных файлах `src/bioetl/pipelines/*.py`, а конфиги лежат под `src/bioetl/configs`. Это создаёт прямое расхождение между архитектурными критериями и исполняемым кодом. [ref: repo:refactoring/MODULE_RULES.md@test_refactoring_32] [ref: repo:refactoring/PIPELINES.md@test_refactoring_32] [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_32] [ref: repo:src/bioetl/configs/pipelines/activity.yaml@test_refactoring_32]
- **P0** — Обязательная «инвентаризация» и связанные артефакты отсутствуют: в репозитории нет каталога `tools/` и файла `docs/requirements/PIPELINES.inventory.csv`, несмотря на жёсткие требования FAQ. [ref: repo:refactoring/FAQ.md@test_refactoring_32] [ref: repo:refactoring/REFACTOR_PLAN.md@test_refactoring_32]
- **P0** — Ряд документов ссылается на другую ветку (`@test_refactoring_32`, `@test_refactoring_32`), что лишает текущую ветку источника истины и мешает актуализации требований. [ref: repo:refactoring/PIPELINES.md@test_refactoring_32] [ref: repo:refactoring/DATA_SOURCES.md@test_refactoring_32]
- **P1** — Регламент тестов и документации требует каталогов `tests/sources/<source>` и `docs/requirements/sources/<source>`, но фактическая структура тестов и требований построена иначе (папки `tests/pipelines`, `docs/requirements/*.md`). [ref: repo:refactoring/MODULE_RULES.md@test_refactoring_32] [ref: repo:tests/integration/pipelines/test_activity_pipeline.py@test_refactoring_32] [ref: repo:docs/requirements/00-architecture-overview.md@test_refactoring_32]
- **P1** — Требования к CLI (`bioetl pipeline run`) расходятся с реальным интерфейсом Typer, регистрирующим команды напрямую из `PIPELINE_COMMAND_REGISTRY`. [ref: repo:refactoring/FAQ.md@test_refactoring_32] [ref: repo:src/bioetl/cli/main.py@test_refactoring_32] [ref: repo:src/scripts/__init__.py@test_refactoring_32]
- **P1** — Списки депрекаций и политики SemVer упоминаются, но файл `DEPRECATIONS.md` в репозитории отсутствует, что блокирует прозрачность API-обещаний. [ref: repo:refactoring/FAQ.md@test_refactoring_32]
- **P2** — Требования к JSON Schema конфигов и расширенному `meta.yaml` частично реализованы (Pydantic-модели, `UnifiedOutputWriter`), но не отражены в документах `refactoring/`, затрудняя проверку готовности. [ref: repo:refactoring/IO.md@test_refactoring_32] [ref: repo:src/bioetl/config/models.py@test_refactoring_32] [ref: repo:src/bioetl/core/output_writer.py@test_refactoring_32]
- **P2** — CI-пайплайн закрывает lint/mypy/pytest, но не содержит проверок, упомянутых в документах (инвентаризация, линк-чек, Pandera drift). [ref: repo:refactoring/ACCEPTANCE_CRITERIA.md@test_refactoring_32] [ref: repo:.github/workflows/ci.yaml@test_refactoring_32]

## Таблица «Где болит»
| Файл | Проблема | Источник ссылки | Предлагаемое действие | Риск |
| --- | --- | --- | --- | --- |
| `refactoring/MODULE_RULES.md` | Требует иерархию `src/bioetl/sources/<source>/...`, которой нет в коде | [ref: repo:refactoring/MODULE_RULES.md@test_refactoring_32] | Переписать правило под фактическую схему `src/bioetl/pipelines/*.py` или спланировать миграцию структуры | Высокий — несогласованность архитектуры и реального кода
| `refactoring/PIPELINES.md` | Ссылается на ветку `@test_refactoring_32` и не совпадает с текущей реализацией стадий | [ref: repo:refactoring/PIPELINES.md@test_refactoring_32] | Обновить ссылки на `@test_refactoring_32`, описать текущие классы и стадии пайплайнов | Высокий — устаревший источник истины
| `refactoring/DATA_SOURCES.md` | Жёстко привязан к ветке `@test_refactoring_32`, перечисляет артефакты, которых нет | [ref: repo:refactoring/DATA_SOURCES.md@test_refactoring_32] | Привести описание к текущей ветке и реальным артефактам, зафиксировать актуальную матрицу источников | Высокий — риски несогласованной нормализации
| `refactoring/FAQ.md` | Требует `tools/inventory/...` и CLI `bioetl pipeline run`, которых нет | [ref: repo:refactoring/FAQ.md@test_refactoring_32] | Сформировать новый FAQ с отражением актуального CLI и roadmap по инвентаризации | Средний — операционные регламенты недостоверны
| `refactoring/ACCEPTANCE_CRITERIA.md` | Гарантии детерминизма и тестов не сопоставлены с существующими инструментами | [ref: repo:refactoring/ACCEPTANCE_CRITERIA.md@test_refactoring_32] | Добавить кросс-ссылки на `UnifiedOutputWriter`, существующие тесты и планы по недостающим проверкам | Средний — сложно подтвердить готовность
| `refactoring/IO.md` | Требует JSON Schema/YAML в `src/bioetl/configs/pipelines`, тогда как конфиги — в `src/bioetl/configs` | [ref: repo:refactoring/IO.md@test_refactoring_32] | Обновить раздел конфигов и схем под реальную систему Pydantic + YAML includes | Средний — документация не помогает интеграции
| `refactoring/REFACTOR_PLAN.md` | Описывает UnifiedAPIClient/инвентаризацию как уже существующие шаги без артефактов | [ref: repo:refactoring/REFACTOR_PLAN.md@test_refactoring_32] | Синхронизировать план с фактическим состоянием и декомпозировать на PR/вехи | Средний — потеря управляемости roadmap

## Список противоречий
1. «`src/bioetl/sources/<source>/...` с подпапками client/request/... — MUST» ↔ фактические пайплайны размещены в файлах `src/bioetl/pipelines/*.py` без указанной иерархии. Решение: либо задокументировать текущий монолитный подход и обновить Acceptance Criteria, либо включить миграцию структуры в roadmap с артефактами и тестами. [ref: repo:refactoring/MODULE_RULES.md@test_refactoring_32] [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_32]
2. FAQ требует «инвентаризацию» из `tools/inventory/inventory_sources.py`, но такого каталога нет. Необходимо запланировать создание скрипта и артефакта `docs/requirements/PIPELINES.inventory.csv` или убрать требование до его реализации. [ref: repo:refactoring/FAQ.md@test_refactoring_32]
3. Документы ссылаются на ветки `@test_refactoring_32` и `@test_refactoring_32`, что противоречит указанию «источник истины — test_refactoring_32». Требуется очистить внешние ссылки и заменить их на актуальные пути. [ref: repo:refactoring/PIPELINES.md@test_refactoring_32] [ref: repo:refactoring/DATA_SOURCES.md@test_refactoring_32]
4. CLI в FAQ фиксирован как `bioetl pipeline run`, хотя реальная реализация — отдельные команды Typer, регистрируемые из `PIPELINE_COMMAND_REGISTRY`. Нужно скорректировать документацию и описать существующий интерфейс. [ref: repo:refactoring/FAQ.md@test_refactoring_32] [ref: repo:src/bioetl/cli/main.py@test_refactoring_32] [ref: repo:src/scripts/__init__.py@test_refactoring_32]
5. Требования к депрекациям указывают на `DEPRECATIONS.md`, но файл отсутствует. Следует создать артефакт и завести регистр предупреждений или скорректировать требования. [ref: repo:refactoring/FAQ.md@test_refactoring_32]
6. Раздел `IO.md` описывает конфиги в `src/bioetl/configs/pipelines/<source>.yaml`, тогда как YAML-файлы находятся в `src/bioetl/configs/pipelines/...`. Нужно обновить путь и схему, чтобы избежать путаницы. [ref: repo:refactoring/IO.md@test_refactoring_32] [ref: repo:src/bioetl/configs/pipelines/activity.yaml@test_refactoring_32]
7. Acceptance Criteria требуют Pandera-схем и детерминизма, но документы не ссылаются на `UnifiedOutputWriter` и Pydantic-конфиги, которые уже реализованы. Нужно добавить ссылки на текущие реализации и выделить пробелы (например, отсутствие property-based тестов). [ref: repo:refactoring/ACCEPTANCE_CRITERIA.md@test_refactoring_32] [ref: repo:src/bioetl/core/output_writer.py@test_refactoring_32] [ref: repo:src/bioetl/config/models.py@test_refactoring_32]
8. План рефакторинга декларирует наличие единого клиента и артефактов, хотя шаги ещё не завершены. Требуется переработка плана с указанием статуса и приоритетов. [ref: repo:refactoring/REFACTOR_PLAN.md@test_refactoring_32]

## Артефакты к созданию/обновлению/удалению
| Путь | Тип | Владелец | Дедлайн | Действие |
| --- | --- | --- | --- | --- |
| `refactoring/MODULE_RULES.md` | guide | Архитектор ETL | E1 | Обновить раскладку/зависимости под текущий каталог `pipelines`
| `refactoring/PIPELINES.md` | spec | Архитектор пайплайнов | E1 | Переписать ссылки на `test_refactoring_32`, описать реальные стадии/классы
| `refactoring/DATA_SOURCES.md` | spec | Ведущий по данным | E1 | Синхронизировать матрицу источников с фактическими пайплайнами и конфигами
| `refactoring/FAQ.md` | guide | Техписатель | E2 | Зафиксировать актуальный CLI и план по инвентаризации
| `docs/requirements/PIPELINES.inventory.csv` | schema/report | DevOps | E2 | Сгенерировать инвентаризацию из будущего скрипта и включить в CI
| `refactoring/ACCEPTANCE_CRITERIA.md` | spec | QA-лид | E2 | Увязать критерии с существующими инструментами (`UnifiedOutputWriter`, тесты)
| `DEPRECATIONS.md` (корень) | spec | Release manager | E3 | Создать регистр депрекаций и связать с CLI/API
| `.github/workflows/ci.yaml` | ci-check | DevOps | E4 | Добавить шаги: инвентаризация, линк-чек документации, проверка Pandera схем

## Детальный план рефакторинга (этапы E1–E4)
### E1 — Документация архитектуры (2 PR)
- **Цели:** привести архитектурные документы к текущему состоянию кодовой базы; убрать ссылки на чужие ветки.
- **Задачи:**
  1. Обновить `MODULE_RULES.md`, `PIPELINES.md`, `DATA_SOURCES.md` с описанием структуры `src/bioetl/pipelines`, текущих конфигов и матрицы источников.
  2. Зафиксировать в документации статус отсутствующих артефактов (инвентаризация, депрекации) с пометкой TODO.
- **Коммиты-носители:** `docs: align refactoring module rules with pipelines layout`, `docs: sync pipeline/data-source specs with test_refactoring_32`.
- **Критерии приёмки:** ссылки только на `@test_refactoring_32`; описание стадий соответствует коду `src/bioetl/pipelines/*.py`; список источников совпадает с YAML-конфигами.
- **Rollback:** откатить PR и восстановить прежние файлы из Git, если выяснится необходимость иного таргет-лейаута.

### E2 — Операционные регламенты и артефакты (3 PR)
- **Цели:** документировать реальный CLI, подготовить требования к инвентаризации и критериям приёмки.
- **Задачи:**
  1. Обновить `FAQ.md` и `ACCEPTANCE_CRITERIA.md`, добавив ссылки на `UnifiedOutputWriter`, Pydantic-конфиги и существующие тесты.
  2. Создать шаблон `docs/requirements/PIPELINES.inventory.csv` и описать будущий скрипт.
  3. Добавить драфт `DEPRECATIONS.md` с пустой таблицей и политикой SemVer.
- **Коммиты-носители:** `docs: refresh faq and acceptance criteria`, `docs: add pipeline inventory template`, `docs: introduce deprecations register`.
- **Критерии приёмки:** CLI-описание совпадает с Typer-приложением; Acceptance Criteria ссылаются на существующие модули; шаблон CSV проходит линтер CSV.
- **Rollback:** удалить созданные/изменённые файлы и обновить ссылки обратно.

### E3 — Автоматизация проверок (2 PR)
- **Цели:** реализовать генератор инвентаризации и покрыть его тестами.
- **Задачи:**
  1. Реализовать скрипт `scripts/qa/inventory_sources.py`, собирающий данные по `src/bioetl`, `src/bioetl/configs`, `docs/requirements`, `tests`.
  2. Добавить модульные тесты для генератора и обновить `docs/requirements/PIPELINES.inventory.csv` детерминированным выводом.
- **Коммиты-носители:** `feat: add inventory generator`, `test: cover inventory generator and refresh artifact`.
- **Критерии приёмки:** запуск скрипта воспроизводим и идемпотентен; тесты проходят локально и в CI; артефакт обновляется автоматически.
- **Rollback:** удалить новый скрипт/артефакт и восстановить шаблон CSV.

### E4 — Интеграция в CI и контроль схем (2 PR)
- **Цели:** закрепить автоматические проверки в CI и включить Pandera/линк-чек.
- **Задачи:**
  1. Расширить `.github/workflows/ci.yaml` шагами: инвентаризация, линк-чек (`lychee` или аналог), проверка «Pandera schema drift».
  2. Добавить в `tests/` проверку соответствия `meta.yaml` и Pandera схем.
- **Коммиты-носители:** `ci: enforce inventory and doc links`, `test: add schema drift guard`.
- **Критерии приёмки:** новые шаги зелёные, CI падает при расхождении артефактов/линков; тесты покрывают Pandera схему.
- **Rollback:** временно отключить новые шаги в CI и откатить тесты.

## Матрица соответствия
| Требование | Документ/код | Проверка в CI |
| --- | --- | --- |
| Детерминированная запись и атомарность | `src/bioetl/core/output_writer.py` | pytest `tests/integration/pipelines/test_extended_mode_outputs.py`
| Строгая валидация конфигов | `src/bioetl/config/models.py` | mypy/pytest `tests/unit/config/test_pipeline_config.py`
| Pandera схемы для пайплайнов | `src/bioetl/schemas` регистрируются в `schema_registry` | pytest `tests/schemas`
| CLI отражает доступные пайплайны | `src/scripts/__init__.py`, `src/bioetl/cli/main.py` | pytest `tests/integration/pipelines/test_enrichment_stages.py`
| UnifiedAPIClient retry/limiter | `src/bioetl/core/api_client.py` | pytest `tests/unit/core/test_api_client.py`
| QC/extended выходы | `src/bioetl/core/output_writer.py` + YAML конфиги | pytest `tests/integration/pipelines/test_extended_mode_outputs.py`
| Конфиги YAML → Pydantic | `src/bioetl/configs/pipelines/*.yaml` + `PipelineConfig` | pytest `tests/unit/config/test_pipeline_config.py`

## Патчи/диффы (черновики)
```diff
--- a/refactoring/MODULE_RULES.md
+++ b/refactoring/MODULE_RULES.md
@@
-`src/bioetl/sources/<source>/` с подпапками:
-  - `client/`
-  - `request/`
-  - `pagination/`
-  - `parser/`
-  - `normalizer/`
-  - `schema/`
-  - `merge/`
-  - `output/`
-  - `pipeline.py`
+`src/bioetl/pipelines/` содержит по одному модулю на сущность (`activity.py`, `assay.py`, `document.py`, `target.py`, `testitem.py`).
+Каждый модуль реализует стадии extract/normalize/validate/write/run и использует общие компоненты из `src/bioetl/core/`.
@@
-`tests/sources/<source>/` с `test_client.py`, `test_parser.py`, ...
+`tests/integration/pipelines/` и `tests/unit/**` покрывают соответствующие стадии; новые тесты должны следовать этому шаблону.
```
```diff
--- a/refactoring/FAQ.md
+++ b/refactoring/FAQ.md
@@
-MUST: единая команда запуска — bioetl pipeline run ...
+MUST: CLI команды регистрируются через Typer (`python -m bioetl.cli.main <pipeline>`); раздел должен ссылаться на актуальный реестр `PIPELINE_COMMAND_REGISTRY`.
@@
-Артефакт: docs/requirements/PIPELINES.inventory.csv. Путь исходника: [ref: repo:tools/inventory/inventory_sources.py@test_refactoring_32].
+Артефакт: docs/requirements/PIPELINES.inventory.csv (создать). Скрипт-генератор будет размещён в `scripts/qa/inventory_sources.py`.
```
```diff
--- a/refactoring/PIPELINES.md
+++ b/refactoring/PIPELINES.md
@@
-Все пути и ссылки указываются на ветку @test_refactoring_32.
+Все пути и ссылки указываются на ветку test_refactoring_32.
@@
-Конфиг каждого источника: `src/bioetl/configs/pipelines/<source>.yaml`.
+Конфиг каждого пайплайна: `src/bioetl/configs/pipelines/<pipeline>.yaml`.
```
