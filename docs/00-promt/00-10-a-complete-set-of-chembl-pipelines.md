# Промт: «Полный набор ChEMBL-пайплайнов с CLI-командами и конфигами (ТОЛЬКО извлечение из ChEMBL)»

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

## Роль и режим

Ты — системный аналитик ETL и технический писатель. Работаешь строго по ветке test_refactoring_32 репозитория SatoryKono/bioactivity_data_acquisition. Никаких внешних догадок. Все ссылки в тексте оформляй так:

[ref: repo:`<path>`@refactoring_001]

## Обязательные источники

[ref: repo:README.md@refactoring_001] — перечень ChEMBL-пайплайнов, примеры CLI python -m bioetl.cli.main ..., пути к YAML.

[ref: repo:src/bioetl/cli/app.py@refactoring_001] — точные имена команд/флагов.

[ref: repo:src/bioetl/configs/models.py@refactoring_001], [ref: repo:docs/configs/00-typed-configs-and-profiles.md@refactoring_001] — ключи конфигов и мердж профилей.

Пайплайны/источники/схемы: [ref: repo:src/bioetl/pipelines/@refactoring_001], [ref: repo:src/bioetl/sources/@refactoring_001], [ref: repo:docs/pipelines/00-pipeline-base.md@refactoring_001].

## Жёсткие ограничения области

ТОЛЬКО извлечение данных из ChEMBL API.

Никаких внешних источников, джойнов, «обогащения», кросс-линков и мерджей вне ChEMBL.

Любые упоминания внешних источников удалить из карточек.

## Задача

Подготовь синхронизированные карточки для ChEMBL-пайплайнов: activity, assay, target, document, testitem. Для каждого зафиксировать: CLI-команду, конфиг, входы/выходы, клиент/пагинацию/парсер (только ChEMBL), нормализацию и валидацию своей Pandera-схемой, детерминизм, QC-метрики, коды возврата, примеры запуска.

## Что выдать

Один файл: docs/pipelines/10-chembl-pipelines-catalog.md.

## Структура файла

### 1) Оглавление и матрица покрытий

Таблица:

| Пайплайн | CLI команда | Конфиг | Источник (ChEMBL) | Эндпоинт(ы) | Артефакт |
|----------|-------------|--------|-------------------|-------------|----------|
| Activity | bioetl.cli.main activity | [ref: repo:src/bioetl/configs/pipelines/chembl/activity.yaml@refactoring_001] | ChEMBL API | указать по коду/конфигу | CSV/Parquet |
| Assay | bioetl.cli.main assay | [ref: repo:src/bioetl/configs/pipelines/chembl/assay.yaml@refactoring_001] | ChEMBL API | указать по коду/конфигу | CSV/Parquet |
| Target | bioetl.cli.main target | [ref: repo:src/bioetl/configs/pipelines/chembl/target.yaml@refactoring_001] | ChEMBL API | указать по коду/конфигу | CSV/Parquet |
| Document | bioetl.cli.main document | [ref: repo:src/bioetl/configs/pipelines/chembl/document.yaml@refactoring_001] | ChEMBL API | указать по коду/конфигу | CSV/Parquet |
| TestItem | bioetl.cli.main testitem | [ref: repo:src/bioetl/configs/pipelines/chembl/testitem.yaml@refactoring_001] | ChEMBL API | указать по коду/конфигу | CSV/Parquet |

Эндпоинты выписывать строго по исходникам/конфигах данного пайплайна.

### 2) Единый шаблон карточки пайплайна (заполнить для всех пяти)

#### Идентификация

Название, CLI, путь к YAML, статус (prod/test). Ссылки на README и YAML.

#### Назначение и охват (только ChEMBL)

Что извлекаем из какого эндпоинта ChEMBL, какие поля считаются обязательными для строки результата.

#### Входы (CLI/конфиги/профили)

Флаги: --config, --output-dir, --dry-run, --limit, --profile determinism и др.
Порядок мерджа профилей (base.yaml → determinism.yaml → явный --config → флаги).

#### Извлечение (клиент → пагинация → парсер)

Клиент и параметры (таймауты, ретраи, RPS), тип пагинации (page/size, offset/limit, cursor), правила остановки и backoff.
Парсер ответа ChEMBL → итерация строк. НИКАКИХ внешних джойнов.

#### Нормализация и валидация

Каноникализация значений, Pandera-схема (strict=True, ordered=True, coerce=True), бизнес-ключ, проверка композиционной уникальности в рамках выгрузки.

#### Выходы и детерминизм

Формат (CSV/Parquet), стабильные сорт-ключи, hash_row/hash_business_key, meta.yaml с row_count/schema_version/hash_algo/business_key/inputs/outputs/config_fingerprint/generated_at_utc.

#### QC-метрики (уровня извлечения)

response_count, pages_total, duplicate_count по бизнес-ключу в выгрузке, missing_required_fields, retry_events. Где пишутся: логи и meta.yaml.

#### Ошибки и коды возврата

Классы ошибок: конфиг, сеть/лимиты, пагинация, парсинг, валидация, запись; exit codes.

#### Примеры запуска (копируемые)

Минимальный, --dry-run, с профилем детерминизма. Путь к YAML и имя команды брать из кода/README.

## Жёсткие требования (MUST)

Никаких упоминаний внешних источников и «обогащения»; только ChEMBL API.

Карточки синхронизированы с README и app.py по именам команд и путям YAML.

В каждой карточке есть: эндпоинт(ы) ChEMBL, схема валидации, бизнес-ключ, сорт-ключи, meta.yaml, QC-метрики извлечения, примеры CLI.

Все ссылки внутри — строго [ref: repo:`<path>`@refactoring_001].
