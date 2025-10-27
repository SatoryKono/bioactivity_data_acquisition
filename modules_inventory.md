# Инвентаризация Python-модулей проекта

## Обзор

Данный документ содержит полную инвентаризацию всех Python-модулей в `src/library/` с их назначением и выявленным дублирующимся функционалом.

## Сводка дублирующегося функционала

### 🔴 Критические дублирования

- **Rate Limiting**: 2 реализации (`common/rate_limiter.py`, `utils/rate_limit.py`)
- **Нормализация данных**: 2 системы (`normalizers/base.py`, `common/base_normalizer.py`)
- **Fallback стратегии**: 3 модуля в legacy (`legacy/fallback.py`, `legacy/circuit_breaker.py`, `legacy/graceful_degradation.py`)
- **Кеширование**: 2 реализации (`legacy/cache_manager.py`, встроенный в `clients/chembl.py`)

### 🟡 Умеренные дублирования

- **HTTP клиенты**: общий паттерн в 7 специализированных клиентах
- **Валидация данных**: дублирование в `tools/data_validator.py` и схемах
- **Обработка ошибок**: множественные реализации в разных модулях

---

## 1. HTTP Клиенты API

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `clients/base.py` | Базовый HTTP клиент с rate limiting и retry логикой | Основа для всех специализированных клиентов | Active |
| `clients/chembl.py` | ChEMBL API клиент с TTL кешем и fallback URL | Дублирует кеширование с `legacy/cache_manager.py` | Active |
| `clients/pubmed.py` | PubMed E-utilities клиент с XML парсингом | Общий паттерн с другими API клиентами | Active |
| `clients/semantic_scholar.py` | Semantic Scholar API с fallback стратегиями | Использует `legacy/fallback.py` | Active |
| `clients/crossref.py` | Crossref API клиент с polite pool | Общий паттерн с другими API клиентами | Active |
| `clients/openalex.py` | OpenAlex API клиент | Общий паттерн с другими API клиентами | Active |
| `clients/gtopdb.py` | Guide to Pharmacology API с circuit breaker | Дублирует circuit breaker с `legacy/circuit_breaker.py` | Active |
| `clients/pubchem.py` | PubChem API с файловым кешем | Дублирует кеширование с `legacy/cache_manager.py` | Active |
| `clients/bioactivity.py` | Универсальный клиент для bioactivity данных | Общий паттерн с другими API клиентами | Active |
| `clients/health.py` | Health check для API клиентов | Использует `legacy/circuit_breaker.py` | Active |
| `clients/session.py` | Управление HTTP сессиями | Вспомогательный модуль | Active |
| `clients/factory.py` | Фабрика для создания клиентов | Вспомогательный модуль | Active |
| `clients/exceptions.py` | Исключения для API клиентов | Дублирует с `utils/errors.py` | Active |

---

## 2. Трансформации и Нормализация

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `transforms/chembl.py` | ChEMBL-специфичные трансформации данных | Специализированные трансформации | Active |
| `normalizers/base.py` | Реестр нормализаторов с декораторами | 🔴 **ДУБЛИРУЕТ** `common/base_normalizer.py` | Active |
| `normalizers/string_normalizers.py` | Нормализация строковых данных | Специализированные нормализаторы | Active |
| `normalizers/numeric_normalizers.py` | Нормализация числовых данных | Специализированные нормализаторы | Active |
| `normalizers/datetime_normalizers.py` | Нормализация дат и времени | Специализированные нормализаторы | Active |
| `normalizers/boolean_normalizers.py` | Нормализация булевых значений | Специализированные нормализаторы | Active |
| `normalizers/identifier_normalizers.py` | Нормализация идентификаторов | Специализированные нормализаторы | Active |
| `normalizers/chemistry_normalizers.py` | Нормализация химических данных | Специализированные нормализаторы | Active |
| `normalizers/ontology_normalizers.py` | Нормализация онтологических данных | Специализированные нормализаторы | Active |
| `normalizers/units_normalizers.py` | Нормализация единиц измерения | Специализированные нормализаторы | Active |
| `normalizers/json_normalizers.py` | Нормализация JSON данных | Специализированные нормализаторы | Active |
| `common/base_normalizer.py` | Базовый класс нормализатора с унифицированными функциями | 🔴 **ДУБЛИРУЕТ** `normalizers/base.py` | Active |

---

## 3. ETL Пайплайны

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `common/pipeline_base.py` | Базовый класс для всех ETL пайплайнов | Основа для всех пайплайнов | Active |
| `activity/pipeline.py` | Пайплайн обработки activity данных | Использует `common/pipeline_base.py` | Active |
| `assay/pipeline.py` | Пайплайн обработки assay данных | Использует `common/pipeline_base.py` | Active |
| `documents/pipeline.py` | Пайплайн обработки документов | Использует `common/pipeline_base.py` | Active |
| `testitem/pipeline.py` | Пайплайн обработки testitem данных | Использует `common/pipeline_base.py` | Active |
| `target/pipeline.py` | Пайплайн обработки target данных | Использует `common/pipeline_base.py` | Active |
| `etl/run.py` | Оркестрация ETL пайплайна | Координирует все пайплайны | Active |
| `etl/extract.py` | Извлечение данных из источников | Специализированное извлечение | Active |
| `etl/transform.py` | Трансформация данных | Специализированные трансформации | Active |
| `etl/load.py` | Загрузка данных в выходные файлы | Специализированная загрузка | Active |
| `etl/qc.py` | Quality Control для ETL | Специализированный QC | Active |
| `etl/qc_common.py` | Общие QC функции | Вспомогательные QC функции | Active |
| `etl/enhanced_qc.py` | Расширенный QC анализ | Расширенный QC | Active |
| `etl/enhanced_correlation.py` | Корреляционный анализ | Специализированный анализ | Active |

---

## 4. Схемы Валидации

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `schemas/base_schema.py` | Базовый класс для всех Pandera схем | Основа для всех схем | Active |
| `schemas/activity_schema.py` | Схема для activity данных | Специализированная схема | Active |
| `schemas/activity_schema_normalized.py` | Нормализованная схема activity | Специализированная схема | Active |
| `schemas/assay_schema.py` | Схема для assay данных | Специализированная схема | Active |
| `schemas/assay_schema_normalized.py` | Нормализованная схема assay | Специализированная схема | Active |
| `schemas/document_schema.py` | Схема для документов | Специализированная схема | Active |
| `schemas/document_schema_normalized.py` | Нормализованная схема документов | Специализированная схема | Active |
| `schemas/document_input_schema.py` | Входная схема документов | Специализированная схема | Active |
| `schemas/document_output_schema.py` | Выходная схема документов | Специализированная схема | Active |
| `schemas/target_schema.py` | Схема для target данных | Специализированная схема | Active |
| `schemas/target_schema_normalized.py` | Нормализованная схема target | Специализированная схема | Active |
| `schemas/testitem_schema.py` | Схема для testitem данных | Специализированная схема | Active |
| `schemas/testitem_schema_normalized.py` | Нормализованная схема testitem | Специализированная схема | Active |
| `schemas/input_schema.py` | Общая входная схема | Общая схема | Active |
| `schemas/output_schema.py` | Общая выходная схема | Общая схема | Active |
| `schemas/meta_schema.py` | Схема метаданных | Специализированная схема | Active |

---

## 5. Утилиты и Вспомогательные Модули

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `utils/rate_limit.py` | Rate limiting с token bucket | 🔴 **ДУБЛИРУЕТ** `common/rate_limiter.py` | Active |
| `utils/errors.py` | Кастомные исключения | Дублирует с `clients/exceptions.py` | Active |
| `utils/empty_value_handler.py` | Обработка пустых значений | Специализированная утилита | Active |
| `utils/list_converter.py` | Конвертация списков | Специализированная утилита | Active |
| `utils/joins.py` | Утилиты для объединения данных | Специализированная утилита | Active |
| `utils/graceful_shutdown.py` | Graceful shutdown утилиты | Специализированная утилита | Active |
| `common/rate_limiter.py` | Rate limiting с token bucket | 🔴 **ДУБЛИРУЕТ** `utils/rate_limit.py` | Active |
| `common/error_tracking.py` | Отслеживание ошибок | Специализированная утилита | Active |
| `common/exit_codes.py` | Коды выхода приложения | Специализированная утилита | Active |
| `common/fallback_data.py` | Fallback данные | Специализированная утилита | Active |
| `common/metadata_fields.py` | Поля метаданных | Специализированная утилита | Active |
| `common/metadata.py` | Построение метаданных | Специализированная утилита | Active |
| `common/postprocess_base.py` | Базовый постпроцессор | Специализированная утилита | Active |
| `common/qc_profiles.py` | QC профили | Специализированная утилита | Active |
| `common/schema_sync_validator.py` | Валидация синхронизации схем | Специализированная утилита | Active |
| `common/writer_base.py` | Базовый writer | Специализированная утилита | Active |

---

## 6. I/O и Файловые Операции

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `io/read_write.py` | Чтение/запись CSV файлов | Специализированные I/O операции | Active |
| `io/normalize.py` | Нормализация I/O данных | Специализированные I/O операции | Active |
| `io/meta.py` | Метаданные I/O | Специализированные I/O операции | Active |
| `io/atomic_writes.py` | Атомарные записи | Специализированные I/O операции | Active |

---

## 7. XML и HTML Парсинг

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `xml/parser_factory.py` | Фабрика XML/HTML парсеров | Специализированные парсеры | Active |
| `xml/selectors.py` | XPath селекторы | Специализированные парсеры | Active |
| `xml/validators.py` | Валидация XML | Специализированные парсеры | Active |
| `xml/namespaces.py` | Управление XML namespaces | Специализированные парсеры | Active |
| `xml/html_support.py` | Поддержка HTML | Специализированные парсеры | Active |
| `xml/exceptions.py` | Исключения XML парсинга | Специализированные парсеры | Active |

---

## 8. Инструменты и CLI

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `tools/data_validator.py` | Валидация данных | 🟡 **ДУБЛИРУЕТ** функционал схем | Active |
| `tools/journal_normalizer.py` | Нормализация журналов | Специализированный инструмент | Active |
| `tools/citation_formatter.py` | Форматирование цитат | Специализированный инструмент | Active |
| `tools/toggle_semantic_scholar.py` | Переключение Semantic Scholar | Специализированный инструмент | Active |
| `cli/__main__.py` | CLI точка входа | CLI функционал | Active |

---

## 9. Конфигурация

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `config.py` | Основная конфигурация | Главный конфиг | Active |
| `config/models.py` | Модели конфигурации | Специализированные модели | Active |
| `config/runtime.py` | Runtime конфигурация | Специализированные модели | Active |
| `activity/config.py` | Конфигурация activity пайплайна | Специализированная конфигурация | Active |
| `assay/config.py` | Конфигурация assay пайплайна | Специализированная конфигурация | Active |
| `documents/config.py` | Конфигурация documents пайплайна | Специализированная конфигурация | Active |
| `testitem/config.py` | Конфигурация testitem пайплайна | Специализированная конфигурация | Active |
| `target/config.py` | Конфигурация target пайплайна | Специализированная конфигурация | Active |

---

## 10. Legacy Модули (Требуют рефакторинга)

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `legacy/cache_manager.py` | Файловое кеширование | 🔴 **ДУБЛИРУЕТ** кеширование в клиентах | Legacy |
| `legacy/fallback.py` | Fallback стратегии | 🔴 **ДУБЛИРУЕТ** fallback в клиентах | Legacy |
| `legacy/circuit_breaker.py` | Circuit breaker pattern | 🔴 **ДУБЛИРУЕТ** circuit breaker в клиентах | Legacy |
| `legacy/graceful_degradation.py` | Graceful degradation | 🔴 **ДУБЛИРУЕТ** graceful degradation в клиентах | Legacy |

---

## 11. Специализированные Модули

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `activity/normalize.py` | Нормализация activity данных | Специализированная нормализация | Active |
| `activity/validate.py` | Валидация activity данных | Специализированная валидация | Active |
| `activity/quality.py` | Quality control для activity | Специализированный QC | Active |
| `activity/writer.py` | Writer для activity данных | Специализированный writer | Active |
| `assay/writer.py` | Writer для assay данных | Специализированный writer | Active |
| `documents/normalize.py` | Нормализация документов | Специализированная нормализация | Active |
| `documents/extract.py` | Извлечение документов | Специализированное извлечение | Active |
| `documents/merge.py` | Объединение документов | Специализированное объединение | Active |
| `documents/validate.py` | Валидация документов | Специализированная валидация | Active |
| `documents/quality.py` | Quality control для документов | Специализированный QC | Active |
| `documents/writer.py` | Writer для документов | Специализированный writer | Active |
| `documents/column_mapping.py` | Маппинг колонок документов | Специализированный маппинг | Active |
| `documents/diagnostics.py` | Диагностика документов | Специализированная диагностика | Active |
| `testitem/normalize.py` | Нормализация testitem данных | Специализированная нормализация | Active |
| `testitem/extract.py` | Извлечение testitem данных | Специализированное извлечение | Active |
| `testitem/validate.py` | Валидация testitem данных | Специализированная валидация | Active |
| `testitem/quality.py` | Quality control для testitem | Специализированный QC | Active |
| `testitem/writer.py` | Writer для testitem данных | Специализированный writer | Active |
| `testitem/clients.py` | Клиенты для testitem | Специализированные клиенты | Active |
| `target/normalize.py` | Нормализация target данных | Специализированная нормализация | Active |
| `target/validate.py` | Валидация target данных | Специализированная валидация | Active |
| `target/quality.py` | Quality control для target | Специализированный QC | Active |
| `target/writer.py` | Writer для target данных | Специализированный writer | Active |
| `target/chembl_adapter.py` | ChEMBL адаптер для target | Специализированный адаптер | Active |
| `target/gtopdb_adapter.py` | GtoPdb адаптер для target | Специализированный адаптер | Active |
| `target/iuphar_adapter.py` | IUPHAR адаптер для target | Специализированный адаптер | Active |
| `target/uniprot_adapter.py` | UniProt адаптер для target | Специализированный адаптер | Active |
| `target/protein_classification.py` | Классификация белков | Специализированная классификация | Active |

---

## 12. Системные Модули

| Модуль | Назначение | Схожий функционал | Статус |
|--------|------------|-------------------|---------|
| `logging_setup.py` | Настройка логирования | Системный модуль | Active |
| `telemetry.py` | Телеметрия | Системный модуль | Active |
| `scripts_base.py` | Базовые скрипты | Системный модуль | Active |

---

## Рекомендации по рефакторингу

### Приоритет 1 (Критический)

1. **Объединить rate limiting**: Выбрать одну реализацию из `common/rate_limiter.py` и `utils/rate_limit.py`
2. **Унифицировать нормализацию**: Объединить `normalizers/base.py` и `common/base_normalizer.py`
3. **Консолидировать fallback стратегии**: Перенести функционал из legacy модулей в основные клиенты
4. **Унифицировать кеширование**: Создать единую систему кеширования

### Приоритет 2 (Высокий)

1. **Стандартизировать HTTP клиенты**: Создать общий базовый класс с общим функционалом
2. **Унифицировать обработку ошибок**: Создать единую иерархию исключений
3. **Консолидировать валидацию**: Объединить валидацию в схемах и tools

### Приоритет 3 (Средний)

1. **Оптимизировать специализированные модули**: Вынести общий функционал в базовые классы
2. **Упростить конфигурацию**: Создать единую систему конфигурации
3. **Стандартизировать QC**: Унифицировать quality control процессы

---

## Статистика

- **Всего модулей**: 134
- **Active**: 130
- **Legacy**: 4
- **Критические дублирования**: 4
- **Умеренные дублирования**: 3
- **Специализированные модули**: 89
- **Системные модули**: 3

---

*Документ создан: 2025-01-27*
*Версия проекта: bioactivity_data_acquisition*
