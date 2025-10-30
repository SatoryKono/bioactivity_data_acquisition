# Унифицированная архитектура: Best Practices
## Обзор
Данная архитектура объединяет лучшие практики из двух проектов:

- **bioactivity_data_acquisition5** — современность, расширяемость, модульность

- **ChEMBL_data_acquisition6** — детерминизм, стабильность, воспроизводимость

Все компоненты построены на принципах ООП с едиными абстрактными классами и конкретными реализациями через конфигурацию.

## Архитектурные принципы
### 1. Композиция над наследованием
Каждый компонент — единый унифицированный класс с композицией подсистем. Поведение определяется конфигурацией, а не множественным наследованием.

### 2. Детерминизм
Все операции воспроизводимы: UTC-время, фиксированная сортировка, детерминированная запись, checksums. Каноническая сериализация для хешей гарантирует бит-в-бит идентичность при одинаковом вводе.

**Инварианты канонизации**: JSON с `sort_keys=True`, ISO8601 для дат, float формат %.6f, NA-policy: None→null. См. [gaps.md](../gaps.md) (G5), [acceptance-criteria.md](../acceptance-criteria.md) (AC3).

### 3. Безопасность
Редактирование секретов, защита от ошибок форматирования, валидация входных данных.

### 4. Расширяемость
Легко добавить новые источники данных, нормализаторы, форматы вывода через конфигурацию и реестры.

## Компоненты системы
### 1. UnifiedLogger — Система логирования
**Цель**: структурированное, безопасное, воспроизводимое логирование.

**Особенности**:

- Структурированный вывод через structlog

- UTC timestamps для детерминизма

- Контекстные переменные (run_id, stage, trace_id)

- Автоматическое редактирование секретов

- OpenTelemetry интеграция (опционально)

**Режимы**:

- development: text, DEBUG, telemetry off

- production: JSON, INFO, telemetry on, rotation

- testing: text, WARNING, telemetry off

📄 **Полное описание**: [01-logging-system.md](01-logging-system.md)

### 2. UnifiedOutputWriter — Система ввода-вывода
**Цель**: детерминированный вывод данных с качественными метриками.

**Особенности**:

- Атомарная запись через run-scoped временные директории с `os.replace`

- Поддержка CSV и Parquet форматов

- Автоматическая генерация QC отчетов

- **Опциональные** correlation отчеты (по умолчанию выключены)

- Валидация через Pandera схемы

- Run manifests для отслеживания

- Каноническая сериализация для воспроизводимых хешей

**Режимы и инварианты**:

**Standard (2 файла, без correlation по умолчанию)**:

- `dataset.csv`, `quality_report.csv`

- Correlation отчет **только** при явном `postprocess.correlation.enabled: true`

- См. детали в [10-configuration.md](10-configuration.md#53-cli-interface-specification-aud-4)

- Инварианты:
  - Checksums стабильны при одинаковом вводе (SHA256)
  - Порядок строк фиксирован (deterministic sort)
  - Column order **только** из Schema Registry
  - NA-policy: `""` для строк, `null` для чисел
  - Каноническая сериализация (JSON+ISO8601, float=%.6f)

**Extended (+ metadata и manifest)**:

- Добавляет `meta.yaml`, `run_manifest.json`

- Инварианты:
  - `meta.yaml` валиден по YAML schema
  - `lineage` содержит все трансформации
  - `checksums` вычислены для всех артефактов
  - `git_commit` присутствует в production
  - Нет частичных артефактов (все или ничего)
  - `column_order` в meta.yaml — копия из схемы (не источник истины)

📄 **Полное описание**: [02-io-system.md](02-io-system.md)

### 3. UnifiedAPIClient — Извлечение данных
**Цель**: надежный, масштабируемый доступ к внешним API.

**Особенности**:

- Опциональный TTL-кэш

- Circuit breaker для защиты от каскадных ошибок

- Fallback manager со стратегиями отката

- Token bucket rate limiter с jitter

- Exponential backoff с giveup условиями

**Поддерживаемые источники**:

- ChEMBL (кэш, batch, XML/JSON)

- CrossRef, OpenAlex, PubMed, Semantic Scholar (документы)

- PubChem, UniProt, IUPHAR (молекулы, белки, лиганды)

📄 **Полное описание**: [03-data-extraction.md](03-data-extraction.md)

### 4. UnifiedSchema — Нормализация и валидация
**Цель**: строгая валидация и стандартизация данных.

**Особенности**:

- Модульная система нормализаторов (реестр)

- Источник-специфичные схемы для Document, Target, TestItem

- Pandera валидация с метаданными нормализации

- Фабрики полей для типовых идентификаторов

- Автоматические QC проверки

**Категории нормализаторов**:

- String, Numeric, DateTime, Boolean

- Chemistry (SMILES, InChI)

- Identifier (DOI, PMID, ChEMBL ID, UniProt, PubChem CID)

- Ontology (MeSH, GO terms)

📄 **Полное описание**: [04-normalization-validation.md](04-normalization-validation.md)

### 5. Assay Pipeline — Извлечение данных ассаев из ChEMBL
**Цель**: детерминированное извлечение данных ассаев с защитой от потери данных.

**Особенности**:

- Батчевое извлечение (batch_size=25, URL limit)

- Long format для nested structures (assay_parameters, variant_sequences)

- Whitelist enrichment (target, assay_class)

- Каноническая сериализация для хеширования

- Referential integrity checks

- Run-scoped метаданные (run_id, git_commit, config_hash)

**Критические исправления**:

- ✅ Batch size: жестко 25 с валидацией конфига

- ✅ Long format для предотвращения потери данных

- ✅ Whitelist для enrichment (строгая валидация)

- ✅ Каноническая сериализация (JSON+ISO8601, float=%.6f, NA-policy)

- ✅ Atomic writes через run_id-scoped temp dirs с os.replace()

- ✅ **Корреляции опциональны** (по умолчанию выключены)

- ✅ Referential integrity checks с порогами

📄 **Полное описание**: [05-assay-extraction.md](05-assay-extraction.md)

## Диаграмма взаимодействия компонентов

```text

┌─────────────────────────────────────────────────────────────┐
│                     Pipeline Execution                      │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │         UnifiedLogger               │
        │  • Structured logging                │
        │  • Context tracking                  │
        │  • Security filters                  │
        └────────────────┬────────────────────┘
                         │
                         ▼
        ┌─────────────────────────────────────┐
        │      UnifiedAPIClient               │
        │  • Cache (TTL, optional)            │
        │  • Circuit breaker                  │
        │  • Rate limiter                     │
        │  • Retry policy                     │
        └────────────────┬────────────────────┘
                         │ Raw data
                         ▼
        ┌─────────────────────────────────────┐
        │     Normalizers + Schemas           │
        │  • Data normalization               │
        │  • Schema validation (Pandera)      │
        │  • Quality checks                   │
        └────────────────┬────────────────────┘
                         │ Validated data
                         ▼
        ┌─────────────────────────────────────┐
        │     UnifiedOutputWriter             │
        │  • Atomic writes                    │
        │  • QC reports                       │
        │  • Correlation analysis             │
        │  • Manifests                        │
        └─────────────────────────────────────┘

```

## Преимущества подхода
1. **Единообразие**: все компоненты следуют общим паттернам

2. **Гибкость**: поведение настраивается через конфигурацию

3. **Надежность**: встроенные механизмы защиты от ошибок

4. **Детерминизм**: воспроизводимость результатов

5. **Расширяемость**: легко добавить новые источники и форматы

6. **Безопасность**: защита секретов и валидация данных

7. **Мониторинг**: логирование и метрики для всех операций

## Интеграция компонентов
### Конфигурация
Все компоненты используют общий подход к конфигурации:

- YAML-файлы с валидацией через Pydantic

- Переменные окружения для переопределения

- CLI-флаги для runtime настроек

### Зависимости
- **UnifiedLogger** → независимый

- **UnifiedAPIClient** → использует UnifiedLogger

- **UnifiedSchema** → использует UnifiedLogger для валидации

- **UnifiedOutputWriter** → использует UnifiedLogger + UnifiedSchema

### Тестирование
- Базовые классы упрощают создание моков

- Композиция позволяет изолировать тесты

- Детерминизм обеспечивает reproducible тесты

## Следующие шаги
1. Изучите детальное описание каждого компонента в отдельных документах

2. Настройте конфигурацию под ваши требования

3. Выберите режимы работы (development/production/testing)

4. Начните с простых use cases и расширяйте функциональность

---

**Версия**: 1.0
**Дата**: 2025-01-28
**Автор**: Unified Architecture Team
