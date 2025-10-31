# MODULE_RULES.md

Нормативные правила раскладки, зависимостей и границ ответственности модулей для всех пайплайнов. Ключевые слова MUST/SHOULD/MAY трактуются по RFC 2119/BCP 14.

## 1. Раскладка и именование

### Каталог `pipelines` (MUST)

Все публичные пайплайны размещены в `src/bioetl/pipelines/` и организованы по одноимённым модулям:

- `base.py` — контракт `PipelineBase`, фабрики клиентов ChEMBL и реестр стадий enrichment. [ref: repo:src/bioetl/pipelines/base.py@HEAD]
- `activity.py` — пайплайн ChEMBL activity. [ref: repo:src/bioetl/pipelines/activity.py@HEAD]
- `assay.py` — пайплайн ассайев ChEMBL. [ref: repo:src/bioetl/pipelines/assay.py@HEAD]
- `document.py` — пайплайн публикаций с внешними обогащениями. [ref: repo:src/bioetl/pipelines/document.py@HEAD]
- `document_enrichment.py` — сервисные стадии enrichment для документов. [ref: repo:src/bioetl/pipelines/document_enrichment.py@HEAD]
- `target.py` — пайплайн таргетов ChEMBL/UniProt/IUPHAR. [ref: repo:src/bioetl/pipelines/target.py@HEAD]
- `target_gold.py` — построение golden-наборов таргетов. [ref: repo:src/bioetl/pipelines/target_gold.py@HEAD]
- `testitem.py` — пайплайн тест-айтемов (молекулы). [ref: repo:src/bioetl/pipelines/testitem.py@HEAD]
- `__init__.py` — реэкспорт публичных классов пайплайнов. [ref: repo:src/bioetl/pipelines/__init__.py@HEAD]

Каждый модуль MUST объявлять ровно один публичный класс пайплайна и экспортировать его через `__all__`. Общая инфраструктура (клиенты, writer, логгер, материализация) лежит в `src/bioetl/core/` и не содержит бизнес-правил конкретных пайплайнов.

### Служебные модули пайплайнов (MUST)

- `document_enrichment.py` и `target_gold.py` MAY импортироваться только из соответствующих пайплайнов и тестов. Переиспользование допускается через helper-функции, вынесенные в `core/`.
- Любые новые файлы в `src/bioetl/pipelines/` MUST быть добавлены в таблицу §5 и зарегистрированы в Typer-реестре при наличии CLI.

### Тесты и доки (MUST)

- Контракт CLI и реестр пайплайнов покрываются `tests/unit/test_cli_contract.py` и `tests/golden/test_cli_golden.py`. [ref: repo:tests/unit/test_cli_contract.py@HEAD] [ref: repo:tests/golden/test_cli_golden.py@HEAD]
- Файлы пайплайнов проверяются в `tests/pipelines/` и `tests/pipelines/golden/`. [ref: repo:tests/pipelines/test_materialization_manager.py@HEAD]
- Конфигурационные снапшоты и golden-данные находятся в `tests/golden/` и `tests/pipelines/golden/`; обновление выполняется общими helper'ами (см. §4).
- Документация требований к пайплайнам ведётся в `docs/requirements/PIPELINES.inventory.csv`. [ref: repo:docs/requirements/PIPELINES.inventory.csv@HEAD]

### Typer-реестр (MUST)

- Единственный источник правды для CLI — `scripts.PIPELINE_COMMAND_REGISTRY`. [ref: repo:src/scripts/__init__.py@HEAD]
- Все новые команды добавляются через `register_pipeline_command` и автоматически появляются в `bioetl.cli.main`. [ref: repo:src/bioetl/cli/main.py@HEAD]
- Скрипты-обёртки (`src/scripts/run_*.py`) MUST использовать `create_pipeline_app` для единообразия. [ref: repo:src/scripts/run_activity.py@HEAD]

### Отсутствие побочных эффектов (MUST)

Импорт любого модуля не должен выполнять сетевые вызовы, запись на диск или менять глобальное состояние. Допустима инициализация констант и лёгких датаклассов.

## 2. Границы слоёв и допустимые зависимости

### Матрица импортов (MUST)

| From \ To | core/* | normalizers/* | schemas/* | utils/* | config/* | pipelines/* | cli/* | scripts/* |
|-----------|--------|---------------|-----------|---------|----------|--------------|-------|-----------|
| core/*    | ✔︎     | —             | ✔︎        | ✔︎      | ✔︎       | —            | —     | —         |
| normalizers/* | ✔︎ | —             | —         | ✔︎      | —        | —            | —     | —         |
| schemas/* | ✔︎     | —             | —         | ✔︎      | ✔︎       | —            | —     | —         |
| pipelines/* | ✔︎   | ✔︎            | ✔︎        | ✔︎      | ✔︎       | —            | —     | —         |
| cli/*     | ✔︎     | —             | —         | ✔︎      | ✔︎       | ✔︎ (через публичные API) | — | ✔︎ (реестр) |
| scripts/* | ✔︎     | —             | —         | ✔︎      | ✔︎       | ✔︎           | ✔︎    | —         |
| tests/*   | ✔︎     | ✔︎            | ✔︎        | ✔︎      | ✔︎       | ✔︎           | ✔︎    | ✔︎        |

Правила:

- Пакет `core` формирует инфраструктуру (логгер, writer, материализацию) и MAY зависеть от `config`, `schemas` и `utils`, но MUST NOT импортировать `pipelines` или CLI.
- `normalizers` и `schemas` MAY использовать `core` и `utils`, но взаимные импорты между ними запрещены, чтобы избежать циклов.
- Модули в `pipelines/` строятся поверх `core`, `normalizers`, `schemas`, `config` и `utils`; обратные зависимости запрещены.
- CLI (`src/bioetl/cli/`) взаимодействует с `scripts/__init__.py`, который хранит Typer-реестр. Оба слоя не должны импортироваться из `pipelines`.
- Тесты MAY импортировать любые слои, но обязаны уважать публичные API (без monkeypatch приватных атрибутов вне крайней необходимости).

### Правила слоёв

- `core/` содержит клиентов, writer, материализацию и инфраструктуру логирования. Сетевые вызовы MUST выполняться только через `bioetl.core.api_client` и фабрику клиентов. [ref: repo:src/bioetl/core/api_client.py@HEAD]
- `normalizers/` выполняют чистые преобразования данных и MUST NOT содержать IO. [ref: repo:src/bioetl/normalizers/__init__.py@HEAD]
- `schemas/` объявляют Pandera-модели и валидационные helper'ы; мутация входного DataFrame запрещена. [ref: repo:src/bioetl/schemas/__init__.py@HEAD]
- `pipelines/` координируют этапы extract → enrich → validate → write, не дублируя реализацию клиентов или нормализаторов. [ref: repo:src/bioetl/pipelines/base.py@HEAD]
- CLI (`src/bioetl/cli/`) и `scripts/` отвечают за пользовательский интерфейс и MUST использовать только публичные API пайплайнов.

### Запрещено (MUST NOT):

- Сетевые вызовы вне `bioetl.core.api_client` и инфраструктурных адаптеров.
- Импорты из `pipelines` в `core`, `normalizers`, `schemas`, `cli.command`.
- Неструктурные логи и `print`; используем только `UnifiedLogger`.

## 3. Конфиги

Конфиги пайплайнов MUST лежать в `src/bioetl/configs/pipelines/*.yaml`; глобальная инвентаризация — `configs/inventory.yaml`. [ref: repo:src/bioetl/configs/pipelines/activity.yaml@HEAD] [ref: repo:configs/inventory.yaml@HEAD]

Модели конфигураций определены в `bioetl.config.models` и валидируются при запуске Typer-команд. [ref: repo:src/bioetl/config/models.py@HEAD]

Алиасы ключей MAY поддерживаться в переходный период с DeprecationWarning и записью в `DEPRECATIONS.md`.

Параметры API (rate-limit, Retry-After, идентификаторы источников) передаются через конфиги и обрабатываются `APIClientFactory`. [ref: repo:src/bioetl/core/client_factory.py@HEAD]

## 4. Детерминизм, форматы и хеши

### Общие инварианты (MUST)

- Стабильный `column_order`.
- Сортировка по бизнес-ключам до записи.
- Кодировка UTF-8, перевод строки `\n`.
- CSV: фиксированный диалект, явный режим quoting (рекомендуется QUOTE_MINIMAL), предсказуемая экранизация.
- Дата/время — RFC 3339 (ISO 8601 профиль), только UTC и timezone-aware.
- Представление отсутствующих значений единообразно (например, `""` в CSV и `null` в JSON/Parquet, без смешения NaN/None).

### Хеши (MUST)

`hash_row` и `hash_business_key` — BLAKE2 (hex), перед хешированием применять нормализацию типов/локали/регистров, исключить нестабильные поля (время генерации, случайные ID).

### Атомарная запись (MUST)

Запись через временный файл в той же ФС и атомарную замену (replace/move_atomic), с flush+fsync перед коммитом. Реализация — общий writer.

### Линиедж (MUST)

`meta.yaml`: размеры и хеши артефактов, версия кода/конфигов, длительности шагов, ключ сортировки, сведения о пагинации/курсоре.

## 5. Каталог пайплайнов и CLI-названия

| CLI ключ | Класс пайплайна | Конфиг по умолчанию | Ввод | Вывод | Примечания |
|----------|-----------------|---------------------|------|-------|------------|
| `activity` | `ActivityPipeline` | `pipelines/activity.yaml` | `data/input/activity.csv` | `data/output/activity/` | Материализация активностей ChEMBL. [ref: repo:src/bioetl/pipelines/activity.py@HEAD] [ref: repo:src/scripts/__init__.py@HEAD] |
| `assay` | `AssayPipeline` | `pipelines/assay.yaml` | `data/input/assay.csv` | `data/output/assay/` | Управление QC ассайев. [ref: repo:src/bioetl/pipelines/assay.py@HEAD] |
| `target` | `TargetPipeline` | `pipelines/target.yaml` | `data/input/target.csv` | `data/output/target/` | Интеграция UniProt/IUPHAR. [ref: repo:src/bioetl/pipelines/target.py@HEAD] |
| `document` | `DocumentPipeline` | `pipelines/document.yaml` | `data/input/document.csv` | `data/output/documents/` | Поддержка режимов `chembl` и `all`. [ref: repo:src/bioetl/pipelines/document.py@HEAD] |
| `testitem` | `TestItemPipeline` | `pipelines/testitem.yaml` | `data/input/testitem.csv` | `data/output/testitems/` | Обогащение PubChem. [ref: repo:src/bioetl/pipelines/testitem.py@HEAD] |

Служебные стадии (`document_enrichment`, `target_gold`) документируются в коде и тестах, но не публикуют отдельные CLI-команды.

Реестр CLI команд находится в `scripts.PIPELINE_COMMAND_REGISTRY` и синхронизируется тестами. [ref: repo:src/scripts/__init__.py@HEAD] [ref: repo:tests/unit/test_cli_contract.py@HEAD]

## 6. Тестирование

- Каждый пайплайн MUST иметь unit-тесты на контракт CLI и публичные методы (`tests/unit/test_cli_contract.py`, `tests/pipelines/test_materialization_manager.py`). [ref: repo:tests/unit/test_cli_contract.py@HEAD] [ref: repo:tests/pipelines/test_materialization_manager.py@HEAD]
- Golden-файлы пайплайнов лежат в `tests/golden/` и `tests/pipelines/golden/`; обновление выполняется helper'ами из `tests/golden/test_cli_golden.py`. [ref: repo:tests/golden/test_cli_golden.py@HEAD]
- Property-based тесты SHOULD покрывать границы нормализации и QC, используя Hypothesis, где применимо.
- Новые пайплайны не считаются GA, пока не будут добавлены в Typer-реестр, покрыты golden-тестами и задокументированы в inventory.

## 7. MergePolicy

- Ключи объединения и приоритеты источников MUST быть описаны в коде соответствующего пайплайна (`DocumentPipeline`, `TargetPipeline`, `TestItemPipeline`) и задокументированы в docstring'ах функций объединения. [ref: repo:src/bioetl/pipelines/document.py@HEAD] [ref: repo:src/bioetl/pipelines/target.py@HEAD] [ref: repo:src/bioetl/pipelines/testitem.py@HEAD]
- Конфликты SHOULD разрешаться стратегиями prefer_source / prefer_fresh / concat_unique / score_based; решение фиксируется в QC-отчётах (`update_summary_section`, `update_validation_issue_summary`). [ref: repo:src/bioetl/utils/qc.py@HEAD]
- Объединение выполняется после валидации обеих сторон (MUST) и должно быть детерминированным.

## 8. Логирование и наблюдаемость

Структурные логи MUST содержать: pipeline, source, request_id, page|cursor, status_code, retries, elapsed_ms, rows_in/out.

Формат логов единый (JSON или logfmt). structlog допустим и рекомендуется для контекстных событий и корелляции. [ref: repo:src/bioetl/core/logger.py@HEAD]

Редакция секретов (tokens/API-keys) — обязательна (MUST).

Таймстемпы в логах — RFC 3339 UTC (MUST).

## 9. Request/Rate-Limit/Retry

Политики Retry/Backoff/RateLimit настраиваются в конфиге; учёт Retry-After (MUST). [ref: repo:src/bioetl/configs/pipelines/document.yaml@HEAD]

«Этикет» API (например, Crossref/OpenAlex mailto) обязателен, если повышает квоты или предписан провайдером (MUST).

Пагинация осуществляется стратегиями из `pagination/` (MUST); порядок страниц и дедупликация фиксированы.

## 10. Документация

Каждый пайплайн должен быть описан в inventory (`docs/requirements/PIPELINES.inventory.csv`) и, при необходимости, иметь расширенную страницу требований с указанием конфигов, merge-политик и golden-наборов. [ref: repo:docs/requirements/PIPELINES.inventory.csv@HEAD]

## 11. Ошибки и исключения

Таксономия ошибок (MUST): NetworkError, RateLimitError, ParsingError, NormalizationError, ValidationError, WriteError.

`pipeline.run()` возвращает структурную сводку и, при фатальной ошибке, завершает процесс с неизменёнными артефактами.

## 12. Совместимость и версии

Любая смена публичного API должна сопровождаться SemVer-инкрементом; MINOR — совместимые изменения, MAJOR — несовместимые.

Депрекации объявляются заранее, выдерживаются минимум два MINOR-релиза, фиксируются в `DEPRECATIONS.md`.

## 13. Безопасность и секреты

- Конфиги пайплайнов MAY ссылаться на секреты только через плейсхолдеры окружения (`${VAR}`) и MUST NOT содержать реальные токены. [ref: repo:src/bioetl/configs/pipelines/document.yaml@HEAD]
- Параметры аутентификации, ключи API и персональные данные передаются через переменные окружения или секрет-хранилище CI; пайплайны не должны читать `.env` вне инициализации конфигурации.
- Логи и `meta.yaml` MUST фильтровать секреты, использовать редактирование значений и избегать попадания PII.

## 14. Производительность и параллелизм

- Ограничения по параллелизму и rate-limit конфигурируются в конфиге пайплайна (например, `workers`, `rate_limit_max_calls`) и соблюдаются клиентами UnifiedAPIClient. [ref: repo:src/bioetl/configs/pipelines/document.yaml@HEAD] [ref: repo:src/bioetl/core/api_client.py@HEAD]
- Пайплайны MAY распараллеливать enrichment-стадии, но MUST уважать инварианты детерминизма (`sort`, `column_order`) и синхронизировать запись через общий writer.
- При увеличении объёмов данных обязательны smoke-/stress-тесты на CLI-уровне (`tests/golden/test_cli_golden.py`) и наблюдение за метриками времени выполнения.

