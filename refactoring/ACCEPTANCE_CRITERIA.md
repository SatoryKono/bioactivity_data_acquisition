# Acceptance Criteria

> **Обновление:** Структура `src/bioetl/sources/` остаётся канонической для внешних источников данных. Модульные реализации ChEMBL находятся в `src/bioetl/sources/chembl/<entity>/`, а файлы `src/bioetl/pipelines/*.py` сохранены как совместимые прокси, которые реэкспортируют новые пайплайны.

## A. Архитектура и раскладка

Для каждого источника существует ровно один публичный пайплайн с минимальным набором модулей (`client/`, `request/`, `pagination/`, `parser/`, `normalizer/`, `schema/`, `merge/`, `output/`, `pipeline.py`). Инвентаризация в CI сверяет дерево против эталона; расхождения — FAIL. Альтернативные реализации сведены к параметрам/стратегиям в пределах одного пайплайна; комплементарные части оформлены композиционно без дублирования кода. Сквозных импорт-циклов нет; матрица допустимых зависимостей по слоям выдержана (запрещены сетевые вызовы вне `client/`, запись на диск вне `output/`).

## B. Публичный API, депрекации, версии

Публичные точки импорта задокументированы и стабилизированы; старые импорты работают через реэкспорт и помечены к удалению через 2 MINOR-релиза. Любое изменение публичного API сопровождается инкрементом версии по SemVer 2.0.0 и записью в CHANGELOG; несовместимые изменения — MAJOR. Semantic Versioning. Процесс обновления SemVer требует единым коммитом обновлять `pyproject.toml`, `CHANGELOG.md` и [DEPRECATIONS.md](../DEPRECATIONS.md).

Во всех модулях с депрекациями испускается `DeprecationWarning` с датой удаления; список синхронизирован с [DEPRECATIONS.md](../DEPRECATIONS.md) и обновляется при каждом выпуске, затрагивающем окно удаления.

## C. Детерминизм и идемпотентность

Два подряд запуска одного пайплайна на одинаковом входе формируют бит-идентичные артефакты (CSV/Parquet/JSON) при одинаковых конфигурациях окружения; допускается отличие только поля времени в `meta.yaml`. Все артефакты пишутся атомарно: через временный файл на той же ФС с последующей атомарной заменой; при сбое частичных файлов не остаётся. Проверяется тестом на инъекцию отказа. [python-atomicwrites](https://python-atomicwrites.readthedocs.io)

Временные метки в логах и `meta.yaml` — RFC 3339/UTC; формат валидируется. [RFC 3339](https://datatracker.ietf.org/doc/html/rfc3339)

Репродуцируемость зафиксирована: стабильный порядок строк и столбцов, детерминированные сериализаторы, исключение недетерминированных источников (случайных сидов, нефиксированных локалей). Принципы соответствуют гайдлайнам «deterministic outputs». [Reproducible Builds](https://reproducible-builds.org)

## D. Контракты данных и валидация

100% записей, производимых `normalize()`, проходят Pandera-схемы целевых сущностей; нарушение схемы — FAIL. [pandera](https://pandera.readthedocs.io)

Поля, не входящие в контракт, сохранены в `extras` без потерь; единицы измерения и идентификаторы приведены к унифицированным представлениям. В `schema/` отсутствуют трансформации данных; только спецификации и валидаторы.

## E. Пагинация, лимиты и «этикет» API

Пагинация реализована стратегиями (PageNumber, Cursor, OffsetLimit, Token), без ручных циклов в коде источника; отсутствие дубликатов и пропусков подтверждено e2e-тестами. Выполняются требования «этикета» API: обязательные заголовки и параметры клиента (например, mailto/User-Agent для Crossref/OpenAlex); проверяется контрактными тестами запросов. [Hypothesis](https://hypothesis.readthedocs.io)

## F. Конфигурации

Конфиги источников проходят строгую схему валидации до запуска пайплайна; несовместимые ключи/значения приводят к ERROR. Алиасы ключей поддерживаются только в переходный период и сопровождаются `DeprecationWarning`; срок снятия — не более 2 MINOR.

## G. Логирование и наблюдаемость

Логи структурные (JSON/logfmt) и содержат минимум: `source`, `request_id`, `page|cursor`, `status_code`, `retries`, `elapsed_ms`, `rows_in`, `rows_out`. Никаких `print`. Секреты/API-ключи в логах и артефактах отсутствуют; включена редакция секретов на уровне логгера. Рекомендуемый стек — `structlog` или эквивалент с key/value полями. [structlog](https://structlog.readthedocs.io)

`meta.yaml` содержит lineage: версии кода/конфигов, контрольные суммы, длительности шагов, политику пагинации, последний курсор.

## H. Вывод и форматы

`OutputWriter` применён во всех пайплайнах; зафиксированы `column_order`, сортировка по бизнес-ключам, `hash_row` и `hash_business_key` (SHA256 из [src/bioetl/core/hashing.py](../src/bioetl/core/hashing.py)); диалект CSV, порядок JSON-ключей и поведение по NaN/Null стабильны. Атомарная запись — обязательна. [python-atomicwrites](https://python-atomicwrites.readthedocs.io) Дополнительные детали канонической политики хеширования описаны в [docs/requirements/00-architecture-overview.md](../docs/requirements/00-architecture-overview.md).

Extended-режим с генерацией `meta.yaml`, correlation и QC отчётов проверяется интеграционным тестом `tests/integration/pipelines/test_extended_mode_outputs.py`, который валидирует содержимое артефактов и ключевые поля метаданных (`run_id`, `pipeline_version`, `source_system`, `file_checksums`, `artifacts.qc`).【F:tests/integration/pipelines/test_extended_mode_outputs.py†L75-L130】 Дополнительно unit-тест `tests/unit/test_output_writer.py::test_unified_output_writer_writes_extended_metadata` страхует `UnifiedOutputWriter._write_metadata()` на уровне прямой сериализации.

**Обязательные проверки IO-контракта:**

- [ ] `pytest tests/integration/pipelines/test_extended_mode_outputs.py tests/unit/test_output_writer.py::test_unified_output_writer_writes_extended_metadata`
- [ ] Содержимое `meta.yaml` содержит обязательные поля (`run_id`, `pipeline_version`, `source_system`, `extraction_timestamp`, `row_count`, `column_count`, `column_order`, `file_checksums`, `config_hash`, `sources`, `artifacts.qc.*`).【F:tests/integration/pipelines/test_extended_mode_outputs.py†L109-L130】【F:src/bioetl/core/output_writer.py†L992-L1043】
- [ ] QC-артефакты (`*_correlation_report.csv`, `*_summary_statistics.csv`, `*_dataset_metrics.csv`) присутствуют и непустые; столбцы соответствуют tidy-формату, а датафрейм метрик включает `row_count`.【F:src/bioetl/core/output_writer.py†L695-L743】【F:tests/integration/pipelines/test_extended_mode_outputs.py†L92-L103】

## I. MergePolicy

Для всех объединений задокументированы ключи слияния и стратегия разрешения конфликтов (prefer_source, prefer_fresh, concat_unique, score_based); слияния выполняются после валидации обеих сторон.

## J. Тестовый контур

Все unit/contract/property/e2e-тесты проходят; «флейки» отсутствуют. Property-based тесты (Hypothesis) покрывают граничные случаи парсинга, нормализации и пагинации; минимальные настройки (кол-во примеров/seed) зафиксированы. [Hypothesis](https://hypothesis.readthedocs.io)

Golden-снимки обновлены детерминированно единым helper'ом; изменения артефактов допускаются только с явным ревью и причинами в CHANGELOG.

## K. Метрики «до/после»

В семьях, попавших под рефакторинг, число файлов сокращено на ≥30% при сохранении функциональности (по `PIPELINES.metrics.md`). Время прогона e2e-набора не ухудшилось более чем на оговорённый бюджет; при ухудшении приложены компенсирующие оптимизации.

## L. Безопасность и секреты

Секреты и токены не хранятся в YAML/артефактах; доступ только через переменные окружения/секрет-хранилище клиента. Проверки redaction и отсутствие PII в логах — обязательные.
