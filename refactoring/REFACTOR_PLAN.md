# REFACTOR_PLAN.md

План рефакторинга по «семьям файлов» и по источникам. Все пути — ветка test_refactoring_32.

> **Обновление:** Структура `src/bioetl/sources/` остаётся канонической для внешних источников данных. Модульные реализации ChEMBL находятся в `src/bioetl/sources/chembl/<entity>/`, а файлы `src/bioetl/pipelines/*.py` сохранены как совместимые прокси, которые реэкспортируют новые пайплайны.

> **Срез текущего поведения:** Документ описывает то, что уже работает в `test_refactoring_32`, и помогает выявлять регрессии при дальнейших изменениях.

## 0) Методика инвентаризации (MUST)

Цель: воспроизводимый срез текущего состояния кода, конфигов, тестов и требований по каждому источнику и по семействам файлов.

Скрипт и выход:

Скрипт: [ref: repo:src/scripts/run_inventory.py@test_refactoring_32]

Выходной CSV: [ref: repo:docs/requirements/PIPELINES.inventory.csv@test_refactoring_32]

Колонки:
source | path | module | size_kb | loc | mtime | top_symbols | imports_top | docstring_first_line | config_keys

Наполнение полей:

loc: строки кода без пустых и комментариев.

top_symbols: экспортируемые классы/функции/константы (по __all__ или эвристике).

imports_top: нормализованные первые импортные директивы (без локальных алиасов).

config_keys: объединение ключей из YAML [ref: repo:src/bioetl/configs/**/*.yaml@test_refactoring_32] и типовых спецификаций в коде.

source: из дерева src/bioetl/sources/<source>/… или имени файла конфига.

Кластеризация (подготовка к п.3):

Имя: n-граммы по client|adapter|parser|normalizer|schema|pipeline|validator|io|logger.

Код: Jaccard по сигнатурам функций/методов; строковые шинглы; доля общих импортов.

Пороговые значения и эвристики: [ref: repo:configs/inventory.yaml@test_refactoring_32].

Протокол:

Запуск локально и в CI; рассинхрон с репозиторием — FAIL.

Шаг CI: `python src/scripts/run_inventory.py --check --config configs/inventory.yaml`.

Конвейер: inventory → clusters → отчёт (см. п.3).

Контрольная точка (baseline) перед миграцией:

- [Inventory CSV](../artifacts/baselines/pre_migration/PIPELINES.inventory.csv)
- [Inventory clusters](../artifacts/baselines/pre_migration/PIPELINES.inventory.clusters.md)
- [Golden-тесты pipelines (pytest лог + coverage)](../artifacts/baselines/golden_tests/)

### Регулярность генерации артефактов инвентаризации

- **Baseline:** ежегодно переопределяется из директории `artifacts/baselines/pre_migration/` перед стартом крупных миграций (см. файлы выше).
- **CI:** job `inventory-check` в `.github/workflows/ci.yaml` выполняет `python src/scripts/run_inventory.py --check --config configs/inventory.yaml` на каждом push/PR.
- **Актуальный артефакт:** `docs/requirements/PIPELINES.inventory.csv` и `docs/requirements/PIPELINES.inventory.clusters.md` пересобираются job `inventory-snapshot` и публикуются как GitHub artifact.

Норматив: требования трактуются по RFC 2119/BCP 14 (MUST/SHOULD/MAY).
datatracker.ietf.org
+1

Тестовая иерархия синхронизирована с `MODULE_RULES.md`: обязательные проверки для каждого источника находятся в
`tests/sources/<source>/`, а каталог `tests/integration/pipelines/` зарезервирован для общих многоисточниковых E2E-сценариев
(golden, QC, bit-identical).

1) Семьи файлов и целевые объединения
1.1 HTTP-клиенты

Current
Разнородные клиенты, локальные ретраи/бэкофф, дублированный rate-limit.
Примеры:
[ref: repo:src/bioetl/sources/pubchem/client/pubchem_client.py]
[ref: repo:src/bioetl/sources/uniprot/client.py]

Target
Единый UnifiedAPIClient в [ref: repo:src/bioetl/core/api_client.py] с политиками:

UnifiedAPIClient — универсальный клиент для работы с внешними API, объединяющий:

- TTL-кэш для тяжелых источников (ChEMBL_data_acquisition6)
- Circuit breaker для защиты от каскадных ошибок (bioactivity_data_acquisition5)
- Fallback manager со стратегиями отката (bioactivity_data_acquisition5)
- Token bucket rate limiter с jitter (ChEMBL_data_acquisition6)
- Exponential backoff с giveup условиями (оба проекта)

Архитектура:

```
UnifiedAPIClient
├── Cache Layer (опционально)
│   └── TTLCache (cachetools; НЕ потокобезопасен, использовать из одного потока или под внешним lock)
├── Circuit Breaker Layer
│   └── CircuitBreaker (half-open state, timeout tracking)
├── Fallback Layer
│   ├── Strategy registry (`cache`, `partial_retry`, `network`, `timeout`, `5xx`)
│   └── FallbackManager (интегрирован, классифицирует ошибки и подбирает стратегию)
├── Rate Limiting Layer
│   └── TokenBucketLimiter (with jitter, per-API)
├── Retry Layer
│   └── RetryPolicy (exponential backoff, giveup conditions)
└── Request Layer
    ├── Session management
    ├── Response parsing (JSON/XML)
    └── Pagination handling
```

**Важно:** cachetools.TTLCache по умолчанию не потокобезопасен. Мы используем его только из одного потока или оборачиваем каждое
обращение внешним `lock` в клиенте.

APIConfig (dataclass):

```python
@dataclass
class APIConfig:
    """Конфигурация API клиента."""
    name: str  # Имя API (chembl, pubmed, etc.)
    base_url: str
    headers: dict[str, str] = field(default_factory=dict)
    cache_enabled: bool = False
    cache_ttl: int = 3600  # секунды
    cache_maxsize: int = 1024
    rate_limit_max_calls: int = 1
    rate_limit_period: float = 1.0  # секунды
    rate_limit_jitter: bool = True
    retry_total: int = 3
    retry_backoff_factor: float = 2.0
    retry_giveup_on: list[type[Exception]] = field(default_factory=lambda: [])
    partial_retry_max: int = 3
    timeout_connect: float = 10.0
    timeout_read: float = 30.0
    cb_failure_threshold: int = 5
    cb_timeout: float = 60.0
    fallback_enabled: bool = True
    fallback_strategies: list[str] = field(
        default_factory=lambda: [
            "cache",
            "partial_retry",
            "network",
            "timeout",
            "5xx",
        ]
    )
```

**Примечание о fallback стратегиях:**

В системе существуют два уровня fallback стратегий, объединённых общей конфигурацией:

| Уровень | Компонент | Стратегии | Назначение |
|---------|-----------|-----------|------------|
| 1 | UnifiedAPIClient (`_apply_fallback_strategies`) | `"cache"`, `"partial_retry"` | Поведенческие стратегии, управляющие повторными запросами и использованием кэша |
| 2 | FallbackManager (`src/bioetl/core/fallback_manager.py`) | `"network"`, `"timeout"`, `"5xx"` | Классификация типов ошибок и генерация детерминированных fallback-плейсхолдеров |

`APIConfig.fallback_strategies` и YAML-конфигурации обязаны перечислять **все** стратегии (`cache`, `partial_retry`, `network`, `timeout`, `5xx`). UnifiedAPIClient и FallbackManager читают единый список и распределяют стратегии по соответствующим уровням.

RetryPolicy:

Политика повторов с учётом Retry-After:

```python
class RetryPolicy:
    """Политика повторов с giveup условиями."""
    def should_giveup(self, exc: Exception, attempt: int) -> bool:
        """Определяет, нужно ли прекратить попытки."""
        if attempt >= self.total:
            return True
        if type(exc) in self.giveup_on:
            return True
        # Специальная обработка для HTTP ошибок
        if isinstance(exc, requests.exceptions.HTTPError):
            if hasattr(exc, 'response') and exc.response:
                status_code = exc.response.status_code
                # Не прекращаем для 429 (rate limit) и 5xx
                if status_code == 429 or (500 <= status_code < 600):
                    return False
                # Fail-fast на 4xx (кроме 429)
                elif 400 <= status_code < 500:
                    return True
        return False
```

Политика ретраев:
- 2xx, 3xx: успех, возвращаем response
- 429: respect Retry-After, ретраить
- 4xx (кроме 429): не ретраить, fail-fast
- 5xx: exponential backoff, retry

Протокол для HTTP 429:

```python
if response.status_code == 429:
    retry_after = response.headers.get('Retry-After')
    if retry_after:
        wait = min(int(retry_after), 60)  # Cap at 60s
        time.sleep(wait)
    raise RateLimitError("Rate limited")
```

Идентификация клиента в request/builder.py там, где это предписано или повышает квоты (mailto для Crossref/OpenAlex). 

Steps

Вынести общий клиент и политики в core/api_client.py.

Заменить пер-источник-клиенты на тонкие адаптеры, не ломая их публичные сигнатуры.

Ввести конфиги retry/backoff/timeout/headers с жёсткой валидацией (ошибка запуска при несоответствии).

Контрактные тесты отказов (429/5xx), учёт Retry-After.

Compatibility
Реэкспорт временно:
[ref: repo:src/bioetl/sources/pubchem/client/pubchem_client.py] → [ref: repo:src/bioetl/core/api_client.py] с DeprecationWarning.

Testing
Unit: политики и адаптеры.
E2E: на источник с коротким golden-набором.

Acceptance
Все источники используют общий клиент; результаты детерминированны.

1.2 Pagination

Current
Ручные циклы, разная семантика page/size/cursor.

Target
[ref: repo:src/bioetl/core/pagination/strategy.py]
Стратегии: PageNumber, Cursor, OffsetLimit, Token. Инварианты порядка, дедупликация.

Steps

Вынести стратегии в core/pagination.

Подключить через request/builder в источниках.

Ввести алиасы ключей конфигов и зафиксировать дефолты.

Testing
Контрактные тесты границ страниц; property-based на монотонность курсора (Hypothesis). 
Hypothesis

Risks
Cursor-дрифты: хранить последний курсор в meta.yaml (см. Output).

Acceptance
Ни одного ручного цикла пагинации в коде источников; адаптеры используют стратегии из `bioetl.core.pagination`.
[ref: repo:src/bioetl/sources/openalex/pagination/__init__.py], [ref: repo:src/bioetl/sources/crossref/pagination/__init__.py]

1.3 Parser/Normalizer

Current
Дублируемые преобразования, разные ключи, «угадывание» типов.

Target
Шаблонный парсер; нормализатор на Schema Registry.
[ref: repo:src/bioetl/schemas/registry.py]

UnifiedSchema — система нормализации и валидации, объединяющая:

- Модульные нормализаторы с реестром (bioactivity_data_acquisition5)
- Источник-специфичные схемы для разных API (ChEMBL_data_acquisition6)
- Pandera валидацию с метаданными
- Фабрики полей для типовых идентификаторов

Архитектура:

```
Normalization System
├── BaseNormalizer (ABC)
│   ├── StringNormalizer
│   ├── NumericNormalizer
│   ├── DateTimeNormalizer
│   ├── BooleanNormalizer
│   ├── ChemistryNormalizer
│   ├── IdentifierNormalizer
│   └── OntologyNormalizer
├── NormalizerRegistry
│   └── registration and lookup

Schema System (Pandera)
├── BaseSchema
│   ├── InputSchema
│   ├── IntermediateSchema
│   └── OutputSchema
│       ├── DocumentSchema
│       ├── TargetSchema
│       ├── AssaySchema
│       ├── ActivitySchema
│       └── TestItemSchema
```

Нормализаторы:

**StringNormalizer**: нормализация строк (strip, NFC, whitespace)
**IdentifierNormalizer**: нормализация идентификаторов (DOI, PMID, ChEMBL ID, UniProt, PubChem CID)
**ChemistryNormalizer**: нормализация химических структур (SMILES, InChI)
**DateTimeNormalizer**: нормализация дат в ISO8601 UTC
**NumericNormalizer**: нормализация чисел с точностью

Реестр нормализаторов:
[ref: repo:src/bioetl/normalizers/registry.py]

```python
class NormalizerRegistry:
    """Реестр нормализаторов."""
    _registry: dict[str, BaseNormalizer] = {}

    @classmethod
    def register(cls, name: str, normalizer: BaseNormalizer):
        """Регистрирует нормализатор."""
        cls._registry[name] = normalizer

    @classmethod
    def get(cls, name: str) -> BaseNormalizer:
        """Получает нормализатор по имени."""
        if name not in cls._registry:
            raise ValueError(f"Normalizer {name} not found")
        return cls._registry[name]

    @classmethod
    def normalize(cls, name: str, value: Any) -> Any:
        """Нормализует значение через нормализатор."""
        normalizer = cls.get(name)
        return normalizer.safe_normalize(value)
```

Steps

Зафиксировать UnifiedSchema и реестр схем.

Вынести общий набор трансформеров: единицы, даты, авторы/аффилиации.

Перевести источники на трансформеры и правила сериализации.

Поля вне контракта — в extras без потери информации.

Testing
Golden-снимки нормализованных рядов; property-based на инварианты трансформаций. 
Hypothesis

Acceptance
Все нормализаторы соответствуют UnifiedSchema.

1.4 Schema/Validator (Pandera)

Current
Схемы распылены, расхождения типов/единиц.

Target
Реестр: [ref: repo:src/bioetl/schemas/registry.py]
Сущности: documents, targets, assays, testitems, activities.

Schema Registry:

Централизованный реестр Pandera-схем с версионированием. Официальный фасад (`bioetl.core.unified_schema`) инкапсулирует операции `register_schema()`, `get_schema()` и `get_schema_metadata()`, чтобы пайплайны не зависели от внутренних структур `SchemaRegistry`. Каждая схема содержит:

- `schema_id`: уникальный идентификатор (например, `document.chembl`)
- `schema_version`: семантическая версия (semver: MAJOR.MINOR.PATCH)
- `column_order`: источник истины для порядка колонок

```python
class BaseSchema(pa.DataFrameModel):
    """Базовый класс для Pandera схем."""
    # Системные поля
    index: int = pa.Field(ge=0, nullable=False)
    pipeline_version: str = pa.Field(nullable=False)
    source_system: str = pa.Field(nullable=False)
    chembl_release: str | None = pa.Field(nullable=True)
    extracted_at: str = pa.Field(nullable=False)  # ISO8601 UTC
    hash_row: str = pa.Field(nullable=False, str_length=64)  # SHA256
    hash_business_key: str = pa.Field(nullable=False, str_length=64)

    class Config:
        strict = True
        coerce = True
        ordered = True
```

SchemaRegistry:

```python
class SchemaRegistry:
    """Реестр всех Pandera схем с валидацией версий."""
    _schemas: dict[str, type[BaseSchema]] = {}

    @classmethod
    def register(cls, schema: type[BaseSchema]):
        """Регистрирует схему."""
        schema_id = schema.schema_id
        cls._schemas[schema_id] = schema

    @classmethod
    def get(
        cls,
        schema_id: str,
        expected_version: str | None = None,
        fail_on_drift: bool = True
    ) -> type[BaseSchema]:
        """Получает схему по ID с проверкой версии."""
        schema = cls._schemas.get(schema_id)
        if not schema:
            raise ValueError(f"Schema {schema_id} not found")
        if expected_version:
            validate_schema_compatibility(schema, expected_version, fail_on_drift)
        return schema
```

Правила эволюции схем (Semantic Versioning):

| Изменение | Impact | Пример | Версия |
|-----------|--------|--------|--------|
| Удаление колонки | Breaking | Удалить `pmid` | MAJOR++ |
| Переименование колонки | Breaking | `title` → `article_title` | MAJOR++ |
| Добавление обязательной колонки | Breaking | Добавить обязательный `source` | MAJOR++ |
| Изменение типа колонки | Breaking | `int` → `float` | MAJOR++ |
| Добавление опциональной колонки | Compatible | Добавить опциональный `abstract` | MINOR++ |
| Добавление constraint | Backward | Добавить `min=0` | MINOR++ |
| Изменение column_order | Compatible | Переставить колонки | PATCH++ |

Матрица совместимости:

| From | To | Compatibility | Required Actions |
|------|-----|---------------|------------------|
| 2.0.0 | 2.1.0 | ✅ Compatible | Нет |
| 2.0.0 | 3.0.0 | ⚠️ Breaking | Migration script |
| 2.1.0 | 2.0.0 | ❌ Incompatible | Downgrade запрещен |

Fail-fast на major drift:

При несовпадении major-версии схемы пайплайн **обязан** упасть, если включен флаг `--fail-on-schema-drift` (по умолчанию в production — включен):

```python
def validate_schema_compatibility(
    schema: type[BaseSchema],
    expected_version: str,
    fail_on_drift: bool
) -> None:
    """Проверяет совместимость версий схем."""
    actual_version = schema.schema_version
    expected_major = int(expected_version.split('.')[0])
    actual_major = int(actual_version.split('.')[0])
    
    if expected_major != actual_major:
        if fail_on_drift:
            raise SchemaDriftError(
                f"Schema version mismatch: expected {expected_version}, "
                f"got {actual_version}. Major version change indicates breaking changes."
            )
```

Steps

Создать и описать реестр схем.

Включить валидацию в pipeline.validate() как обязательный шаг (fail-fast). 
pandera.readthedocs.io

Категориальные множества и диапазоны оформлены явными проверками. 
pandera.readthedocs.io

Acceptance
100% строк проходят Pandera на golden-наборах.

1.5 Output/IO/Determinism

Current
Разные писатели, локальные хэши, плавающий порядок столбцов.

Target
Единый UnifiedOutputWriter: стабильная сортировка, hash_row, hash_business_key, атомарная запись, meta.yaml.
[ref: repo:src/bioetl/core/output_writer.py]

UnifiedOutputWriter — детерминированная система записи данных, объединяющая:

- Атомарную запись через временные файлы (bioactivity_data_acquisition5)
- Трехфайловую систему с QC отчетами (ChEMBL_data_acquisition6)
- Автоматическую валидацию через Pandera
- Run manifests для отслеживания пайплайнов
  - JSON документ фиксированной структуры: `run_id`, `artifacts`, `checksums`, `schema`
  - `artifacts` перечисляет `dataset`, `quality_report`, `metadata`, а также `qc`/`additional_datasets`/`debug_dataset` при наличии
  - `checksums` содержит SHA256 хэши артефактов (ключ — имя файла)
  - `schema` — словарь с `id` и `version` (значения `null`, если схема не привязана)

Архитектура:

```
UnifiedOutputWriter
├── Validation Layer
│   └── PanderaSchemaValidator
├── Format Layer
│   ├── CSVHandler (deterministic sorting)
│   └── ParquetHandler (compression, column types)
├── Quality Layer
│   ├── QualityReportGenerator
│   └── CorrelationReportGenerator
├── Metadata Layer
│   ├── OutputMetadata
│   └── ManifestWriter
└── Atomic Write Layer
    └── AtomicWriter (temporary files + rename)
```

AtomicWriter:

Безопасная атомарная запись через run-scoped временные директории с использованием `os.replace`:

```python
import os
from pathlib import Path

class AtomicWriter:
    """Атомарная запись с защитой от corruption."""
    def __init__(self, run_id: str):
        self.run_id = run_id

    def write(self, data: pd.DataFrame, path: Path, **kwargs):
        """Записывает data в path атомарно через run-scoped temp directory."""
        # Run-scoped temp directory
        temp_dir = path.parent / f".tmp_run_{self.run_id}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        # Temp file path
        temp_path = temp_dir / f"{path.name}.tmp"
        try:
            # Запись во временный файл
            self._write_to_file(data, temp_path, **kwargs)
            # Атомарный rename через os.replace (Windows-compatible)
            path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(str(temp_path), str(path))
        except Exception as e:
            # Cleanup временного файла при ошибке
            temp_path.unlink(missing_ok=True)
            raise
        finally:
            # Cleanup temp directory
            try:
                if temp_dir.exists() and not any(temp_dir.iterdir()):
                    temp_dir.rmdir()
            except OSError:
                pass
```

Режимы работы:

**Standard (2 файла, без correlation по умолчанию):**
- `dataset.csv`, `quality_report.csv`
- Correlation отчёт **только** при явном `postprocess.correlation.enabled: true`

**Extended (+ metadata и manifest):**
- Добавляет `meta.yaml`, `run_manifest.json` с описанной выше структурой `run_id`/`artifacts`/`checksums`/`schema`
- Полные метаданные: lineage, checksums, git_commit

Инварианты детерминизма:

- Checksums стабильны при одинаковом вводе (SHA256)
- Порядок строк фиксирован (deterministic sort)
- Column order **только** из Schema Registry
- NA-policy: `""` для строк, `null` для чисел
- Каноническая сериализация (JSON+ISO8601, float=%.6f)

Запрет частичных артефактов:

- CSV с неполными данными недопустимы
- `meta.yaml` без checksums или lineage недопустимы
- Пустые файлы (размер = 0) недопустимы

Steps

Вынести writer в core/output_writer.py, зафиксировать column_order.

Включить контрольные хэши и заполнение meta.yaml (суммы, версии, время).

Писать через временный файл на той же ФС, затем os.replace(). 
python-atomicwrites.readthedocs.io

Acceptance
Бит-идентичный вывод при повторных запусках.

1.6 Logging

Current
Несогласованные форматы, print.

Target
Структурный логгер с корреляционными ID; единый формат key/value.
[ref: repo:src/bioetl/core/logger.py]

Details

UnifiedLogger — универсальная система логирования, объединяющая:

- Структурированность из structlog (bioactivity_data_acquisition5)
- Детерминизм через UTC timestamps (ChEMBL_data_acquisition6)
- Контекстное логирование через ContextVar
- Автоматическое редактирование секретов

Архитектура:

```
UnifiedLogger
├── Core: structlog с extensions
│   ├── ContextVar для run_id, stage, trace_id
│   ├── Processors (timestamp UTC, redact, add_context)
│   └── Renderers (text для консоли, JSON для файлов)
├── Security Layer
│   ├── RedactSecretsFilter (token, api_key, password)
│   └── SafeFormattingFilter (защита от % ошибок)
├── Output Layer
│   ├── ConsoleHandler (text/JSON формат)
│   └── FileHandler (JSON, ротация 10MB×10)
└── Telemetry (опционально)
    └── OpenTelemetry интеграция
```

LogContext (dataclass):

Унифицированный контекст для всех логов:

```python
@dataclass(frozen=True)
class LogContext:
    """Контекст выполнения для логирования."""
    run_id: str  # UUID8 уникальный идентификатор запуска
    stage: str  # Текущий этап пайплайна
    actor: str  # Инициатор (system, scheduler, username)
    source: str  # Источник данных (chembl, pubmed, ...)
    generated_at: str  # UTC timestamp ISO8601
    trace_id: str | None = None  # OpenTelemetry trace ID
    endpoint: str | None = None  # HTTP эндпоинт или None для стадийных логов
    page_state: str | None = None  # Положение пагинации
    params: dict[str, Any] | None = None  # Запрос или дополнительные параметры
    attempt: int | None = None  # Номер попытки повторного запроса
    retry_after: float | None = None  # Планируемая пауза между повторами
    duration_ms: int | None = None  # Длительность операции
    error_code: int | None = None  # Код ошибки (HTTP, бизнес-правила)
    error_message: str | None = None  # Сообщение об ошибке
```

SecurityProcessor (structlog processor):

Редактирование чувствительных данных в structlog:

```python
def security_processor(logger, method_name, event_dict):
    """Удаляет секреты из event_dict."""
    sensitive_keys = [
        "api_key", "token", "password", "secret", "authorization",
        "bearer", "auth", "credential", "access_token"
    ]
    for key in list(event_dict.keys()):
        if any(s in key.lower() for s in sensitive_keys):
            event_dict[key] = "[REDACTED]"
    return event_dict
```

Режимы работы:

- **Development**: text формат, DEBUG уровень, telemetry off
- **Production**: JSON формат, INFO уровень, telemetry on, rotation
- **Testing**: text формат, WARNING уровень, telemetry off

Обязательные поля контекста:

Все логи должны содержать минимальный набор полей для трассируемости:

| Поле | Обязательность | Описание |
|------|----------------|----------|
| `run_id` | Всегда | UUID идентификатор запуска пайплайна |
| `stage` | Всегда | Текущий этап (extract, normalize, validate, write) |
| `actor` | Всегда | Инициатор (system, scheduler, username) |
| `source` | Всегда | Источник данных (chembl, pubmed, и т.п.) |
| `generated_at` | Всегда | UTC timestamp ISO8601 |

Для HTTP-запросов дополнительно обязательны:
- `endpoint`: URL эндпоинта
- `attempt`: номер попытки повтора
- `duration_ms`: длительность операции
- `params`: параметры запроса (если есть)
- `retry_after`: планируемая пауза (сек) при 429

Acceptance
Отсутствие print; у каждого события поля source, request_id, page|cursor, retry, elapsed_ms, rows_in/out.

1.7 RequestBuilder (добавление)

Target
Единый билдер на источник: [ref: repo:src/bioetl/sources/<source>/request/builder.py@test_refactoring_32]
Функции:

обязательные заголовки, mailto/User-Agent (Crossref/OpenAlex);

сериализация фильтров/полей; стабильный порядок параметров. 
python-atomicwrites.readthedocs.io

1.8 MergePolicy (добавление)

Target
[ref: repo:src/bioetl/sources/document/merge/policy.py]
Явные ключи объединения (doi|pmid|cid|uniprot_id|molecule_chembl_id…) и стратегии конфликтов: prefer_source, prefer_fresh, concat_unique, score_based.

1.9 Config & Schema (добавление)

Target
Строгая валидация конфигов до запуска пайплайна; несовместимые ключи — ERROR. Алиасы допустимы только в переходный период и фиксируются в [ref: repo:DEPRECATIONS.md@test_refactoring_32].
Для Crossref/OpenAlex ключ mailto обязателен. 
python-atomicwrites.readthedocs.io

1.10 Security & Secrets (добавление)

Target

API-ключи и токены не хранятся в YAML; источник правды — переменные окружения/секрет-хранилище, доступ через client/.

Логи MUST NOT содержать секреты; redaction фильтры в логгере.

Трассировка запросов исключает личные данные; request_id не коррелирует с пользователем.

1.11 Performance Budgets (добавление)

Target

Таймауты по умолчанию и верхние границы retry/backoff фиксированы в config/ и валидируются.

Лимиты на параллелизм/конкурентность — на слой клиента; настройка per-source.

Бюджеты времени на шаги фиксируются в meta.yaml для последующего мониторинга.

2) План по источникам

«Current» заполняется фактами из PIPELINES.inventory.csv.

2.1 Crossref

Current
[ref: repo:src/bioetl/sources/crossref/@test_refactoring_32]
[ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32]
[ref: repo:tests/sources/crossref/*@test_refactoring_32]

Target
Структура как в PIPELINES.md. Публичный API: CrossrefPipeline.

Steps

Пагинация → Cursor.

RequestBuilder добавляет mailto и корректный User-Agent. 
python-atomicwrites.readthedocs.io

Парсер: авторы/аффилиации/журнал → UnifiedSchema.

Pandera-схема documents; Writer с детерминизмом. 
pandera.readthedocs.io

Compatibility
Реэкспорт старых символов; алиасы конфиг-ключей.

Testing
Golden DOIs; property-based устойчивость нормализации. 
Hypothesis

Risks
Вариабельность полей — перенос в extras.

Acceptance
Схема проходит; вывод детерминирован.

2.2 PubMed E-utilities

Current
[ref: repo:src/bioetl/sources/pubmed/*@test_refactoring_32]

Target
PubMedPipeline, батчи efetch, связка esearch → efetch; фиксированный URL-синтаксис. 
Semantic Versioning

Steps

RequestBuilder для esearch/efetch (rettype/retmode).

Парсер MEDLINE → UnifiedSchema.

Pandera-схема documents; Writer.

Testing
Golden PMIDs + property-based.

2.3 OpenAlex

Target
Курсорная пагинация, mailto для polite pool; нормализация DOI/авторов. 
GitHub

Steps
Аналогично Crossref; аккуратная обработка неполных DOI.

2.4 Semantic Scholar

Target
Запрос полей через fields; нормализация paperId/doi; e2e golden. 
Hypothesis

2.5 ChEMBL

Target
Единицы/типы активностей; ключи molecule_chembl_id/assay_id; использование официального API/клиента. 
python-atomicwrites.readthedocs.io

2.6 UniProt, 2.7 PubChem, 2.8 IUPHAR/BPS GtoP

Target
Соблюдение официальных REST-правил, специфика пагинации и ключей; MergePolicy зафиксирован. (UniProt, PubChem, GtoP — см. соответствующие доки.) 
pandera.readthedocs.io
+1

3) Кластеризация и различия (MUST)

Выход: [ref: repo:docs/requirements/PIPELINES.inventory.clusters.md@test_refactoring_32]
cluster_id, source, type{duplicate|alternative|complementary}, similarity_name, similarity_code, files[], common_responsibility, divergence_points

Правила:

duplicate: совпадает назначение и публичный API; различия косметические.

alternative: одна ответственность, разные реализации/контракты.

complementary: взаимодополняющие части одного шага.

divergence_points: публичный API, зависимости, побочные эффекты IO/логов, форматы данных, ключи конфигов, схемы.

4) Метрики до/после (SHOULD)

Отчёт: [ref: repo:docs/requirements/PIPELINES.metrics.md@test_refactoring_32]

Показатели:

files_before → files_after, loc_before → loc_after, public_symbols_before → after.

Снижение дубликатов (по кластерам), время тестов.

Доля строк, прошедших Pandera, и покрытие property-based (Hypothesis). 
pandera.readthedocs.io
+1

5) Порядок миграции и стратегия ветвления (MUST)

Стратегия: короткоживущие атомарные PR'ы поверх основного ствола (trunk-based). Никаких «мега-диффов»: один PR — одна семья или один источник. 
Trunk Based Development
+1

Очередь PR (референс):

core/http + перевод простого источника.

core/pagination + перевод Crossref.

schemas/registry + общие трансформеры.

schema_registry + Pandera-валидация. 
pandera.readthedocs.io

core/output/writer + атомарные записи. 
python-atomicwrites.readthedocs.io

Перевод оставшихся источников; затем MergePolicy.

Коммиты: Conventional Commits, релизы по SemVer. 
Semantic Versioning

6) Совместимость, версии и депрекации (MUST)

Версионирование: SemVer. Изменение публичного API — MINOR/MAJOR (по совместимости). 
Semantic Versioning

Депрекации:

Предупреждение сразу, удаление через 2 MINOR; реестр — [ref: repo:DEPRECATIONS.md@test_refactoring_32].

Политика обратной совместимости соответствует принципам PEP 387 (этапы устаревания, период предупреждений). 
Python Enhancement Proposals (PEPs)

Стабильность:

Запрещено менять поведение между релизами без депрекации и записи в CHANGELOG.

7) Тестирование и артефакты (MUST)

Пирамида тестов:

Unit: клиент/политики, парсер, нормализатор, схемы.

Contract: статические фикстуры HTTP.

Property-based: инварианты сортировки, курсоров, маппингов. 
Hypothesis

E2E: пайплайн целиком, сверка с golden.

Golden: детерминированные файлы; обновление через единый helper.

Наблюдаемость: структурные логи и минимальные метрики пайплайна; допускается JSON/logfmt и подход structlog. 
structlog

8) Data Governance

meta.yaml фиксирует: количество записей, контрольные суммы, версии кода/конфигов, длительности шагов, дату/время, ключ сортировки.

Версионность golden-наборов; связь golden↔коммит зафиксирована в отчёте.

Политика по PII/секретам: запрет в логах/артефактах; тесты на redaction.

9) Риски и меры
Риск	Проявление	Меры
Расхождение API	падение нормализации	extras + тесты на неканонические ответы
Cursor-дрифты	пропуски/дубликаты	хранить/валидировать курсор в meta.yaml
Нестабильные единицы/форматы	ошибки валидации	Pandera-схемы + явные конвертеры 
pandera.readthedocs.io

Сбой записи	частичные файлы	атомарные записи (atomic_write), fsync, replace 
python-atomicwrites.readthedocs.io

Непредсказуемые входы	редкие кейсы	property-based покрытие (Hypothesis) 
Hypothesis
10) Acceptance (MUST)

Все источники переведены на минимальный состав модулей.

Публичный API задокументирован; старые импорты работают через реэкспорт и помечены к удалению через 2 MINOR.

Сокращение файлов в затронутых семьях ≥ 30% при сохранении функционала.

Детерминизм и идемпотентность подтверждены E2E-тестами.

Все тесты проходят; golden обновлены детерминированно; конфиги валидируются строго.

Логи структурные, print отсутствует.

11) Валидация конфигов через Pydantic (MUST)

Все конфигурации валидируются через Pydantic-совместимые модели при запуске пайплайна. Несовместимые ключи/значения вызывают ошибку запуска (MUST NOT продолжать работу).

**Базовая схема YAML:**

Все пайплайны используют единую систему конфигурации на базе YAML и Pydantic. Базовый файл `src/bioetl/configs/base.yaml` описывает обязательные секции, а профильные конфигурации расширяют его через механизм наследования.

Базовый файл `src/bioetl/configs/base.yaml`:

```yaml
version: 1
pipeline:
  name: "base"
  entity: "abstract"
  release_scope: true  # связывать конфиг с версией источника

http:
  global:
    timeout_sec: 60.0
    retries:
      total: 5
      backoff_multiplier: 2.0
      backoff_max: 120.0
    rate_limit:
      max_calls: 5
      period: 15.0
cache:
  enabled: true
  directory: "data/cache"
  ttl: 86400
  release_scoped: true
paths:
  input_root: "data/input"
  output_root: "data/output"
determinism:
  sort:
    by: []
    ascending: []
  column_order: []
postprocess:
  qc:
    enabled: true
qc:
  severity_threshold: "warning"
cli:
  default_config: "src/bioetl/configs/base.yaml"
```

**Обязательные поля и их назначение:**

| Секция        | Поле                                   | Назначение                                                       |
|---------------|----------------------------------------|------------------------------------------------------------------|
| `version`     | целое                                  | Версионирование схемы конфигурации.                              |
| `pipeline`    | `name`, `entity`                       | Идентификация пайплайна (используется в логах, метаданных).      |
| `http.global` | `timeout_sec`, `retries.total`         | Гарантированные лимиты для клиентов без локальных переопределений. |
| `cache`       | `enabled`, `directory`, `release_scoped` | Политика кэширования и инвалидации.                              |
| `paths`       | `output_root`                          | Каталог для детерминированных артефактов.                        |
| `determinism` | `sort.by`, `column_order`              | Формирование стабильного порядка строк/столбцов.                 |
| `postprocess` | `qc.enabled`                           | Включение QC-этапов.                                             |
| `qc`          | `severity_threshold`                   | Глобальный уровень, при превышении которого пайплайн падает.     |
| `cli`         | `default_config`                       | Значение по умолчанию для `--config`.                            |

**Правила наследования:**

Профильные конфигурации расширяют базовые через ключ `extends`:

```yaml
# src/bioetl/configs/pipelines/assay.yaml
extends:
  - "../base.yaml"
  - "../includes/determinism.yaml"

pipeline:
  name: "assay"
  entity: "assay"

sources:
  chembl:
    enabled: true
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    batch_size: 25
    max_url_length: 2000

determinism:
  sort:
    by: ["assay_chembl_id"]
    ascending: [true]
  column_order: ["assay_chembl_id", "pipeline_version", "hash_row", "hash_business_key"]
```

Мерж выполняется по правилам «глубокого» обновления словарей:
- словари объединяются рекурсивно;
- списки считаются атомарными и полностью заменяются дочерними значениями;
- скалярные значения заменяются последним источником.

**Приоритет переопределений:**

`base.yaml < profile.yaml < CLI < env`

**Pydantic-модели:**

Конфигурация загружается через корневую модель `PipelineConfig`:

```python
from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional
from pydantic import BaseModel, Field, validator

class RetryConfig(BaseModel):
    total: int = Field(..., ge=0)
    backoff_multiplier: float = Field(..., gt=0)
    backoff_max: float = Field(..., gt=0)

class RateLimitConfig(BaseModel):
    max_calls: int = Field(..., ge=1)
    period: float = Field(..., gt=0)

class HTTPSection(BaseModel):
    timeout_sec: float = Field(..., gt=0, env="HTTP__GLOBAL__TIMEOUT_SEC")
    retries: RetryConfig
    rate_limit: RateLimitConfig
    headers: Dict[str, str] = Field(default_factory=dict)

class SourceConfig(BaseModel):
    enabled: bool = True
    base_url: str
    batch_size: Optional[int] = Field(default=None, ge=1)
    extra: Dict[str, object] = Field(default_factory=dict, alias="*")

class CacheConfig(BaseModel):
    enabled: bool
    directory: Path
    ttl: int = Field(..., ge=0)
    release_scoped: bool

class DeterminismConfig(BaseModel):
    sort_by: List[str] = Field(..., alias="sort.by")
    column_order: List[str]

class PipelineMetadata(BaseModel):
    name: str
    entity: str
    release_scope: bool = True

class PipelineConfig(BaseModel):
    version: int
    pipeline: PipelineMetadata
    http: Dict[str, HTTPSection]
    sources: Dict[str, SourceConfig] = Field(default_factory=dict)
    cache: CacheConfig
    paths: Dict[str, Path]
    determinism: DeterminismConfig
    postprocess: Dict[str, object] = Field(default_factory=dict)
    qc: Dict[str, object] = Field(default_factory=dict)
    cli: Dict[str, object] = Field(default_factory=dict)

    @validator("version")
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("Unsupported config version")
        return value
```

**CLI Interface Specification:**

Унифицированные CLI флаги для всех пайплайнов:

| Флаг | Тип | Обязательность | Описание |
|---|---|---|---|
| `--config` | path | Опционально | Путь к YAML конфигурации (default: `src/bioetl/configs/base.yaml`) |
| `--golden` | path | Опционально | Путь к golden-файлу для детерминированного сравнения |
| `--sample` | int | Опционально | Ограничить входные данные до N записей (для тестирования) |
| `--fail-on-schema-drift` | flag | Опционально | Fail-fast при major-версии схемы (default: `True` в production) |
| `--extended` | flag | Опционально | Включить расширенные артефакты (correlation_report, meta.yaml, manifest) |
| `--mode` | str | Опционально | Режим работы (для Document: `chembl` \| `all`) |
| `--dry-run` | flag | Опционально | Проверка конфигурации без выполнения |
| `--verbose` / `-v` | flag | Опционально | Детальное логирование |

**Переопределения через CLI:**

CLI (`bioetl pipeline run`) поддерживает опцию `--set <path>=<value>`:

```bash
bioetl pipeline run \
  --config src/bioetl/configs/pipelines/assay.yaml \
  --set sources.chembl.batch_size=20 \
  --set http.global.timeout_sec=45
```

**Переопределения через переменные окружения:**

Каждое поле может быть переопределено через переменные окружения с двойным подчёркиванием:

| Переменная                   | Эквивалентный путь                |
|------------------------------|-----------------------------------|
| `BIOETL_HTTP__GLOBAL__TIMEOUT_SEC` | `http.global.timeout_sec`          |
| `BIOETL_SOURCES__CHEMBL__API_KEY`  | `sources.chembl.api_key`           |
| `BIOETL_CACHE__DIRECTORY`          | `cache.directory`                  |
| `BIOETL_QC__MAX_TITLE_FALLBACK`    | `qc.max_title_fallback`            |

Переменные окружения применяются **после** CLI-переопределений.

**Валидация:**

При загрузке конфига выполняется валидация через Pydantic модель `PipelineConfig`:
- Типобезопасность всех полей
- Проверка допустимых значений
- Автогенерация JSON Schema
- Ошибка запуска при несоответствии

Набор линтеров (`ruff`, `mypy`) должен проверять, что `PipelineConfig` не допускает неизвестных полей (`model_config = ConfigDict(extra="forbid")`).

12) Политика версионирования схем (SemVer) (MUST)

Все Pandera-схемы версионируются по Semantic Versioning (MAJOR.MINOR.PATCH) с правилами эволюции и проверкой совместимости.

**Структура схемы:**

Каждая схема содержит:
- `schema_id`: уникальный идентификатор (например, `document.chembl`)
- `schema_version`: семантическая версия (semver: MAJOR.MINOR.PATCH)
- `column_order`: источник истины для порядка колонок

```python
class DocumentSchema(BaseSchema):
    """Схема для ChEMBL документов."""
    # Метаданные схемы
    schema_id = "document.chembl"
    schema_version = "2.1.0"
    # Порядок колонок (источник истины)
    column_order = [
        "document_chembl_id", "title", "journal", "year",
        "doi", "pmid", "hash_business_key", "hash_row"
    ]
    # Поля схемы
    document_chembl_id: str = pa.Field(str_matches=r'^CHEMBL\d+$')
    title: str = pa.Field(nullable=False)
    ...
```

**Правила эволюции схем:**

| Изменение | Impact | Пример | Версия |
|-----------|--------|--------|--------|
| Удаление колонки | Breaking | Удалить `pmid` | MAJOR++ |
| Переименование колонки | Breaking | `title` → `article_title` | MAJOR++ |
| Добавление обязательной колонки | Breaking | Добавить обязательный `source` | MAJOR++ |
| Изменение типа колонки | Breaking | `int` → `float` | MAJOR++ |
| Добавление опциональной колонки | Compatible | Добавить опциональный `abstract` | MINOR++ |
| Добавление constraint | Backward | Добавить `min=0` | MINOR++ |
| Изменение column_order | Compatible | Переставить колонки | PATCH++ |
| Документация/комментарии | None | Обновить docstring | PATCH++ |

**Правило "заморозки" колонок:**

- Добавление колонки: minor или major (если обязательная)
- Удаление колонки: только major
- Изменение типа колонки: только major

**Fail-fast на major drift:**

При несовпадении major-версии схемы пайплайн **обязан** упасть, если включен флаг `--fail-on-schema-drift` (по умолчанию в production — включен):

```python
def validate_schema_compatibility(
    schema: type[BaseSchema],
    expected_version: str,
    fail_on_drift: bool
) -> None:
    """
    Проверяет совместимость версий схем.
    
    Raises:
        SchemaDriftError: при несовместимых изменениях и fail_on_drift=True
    """
    actual_version = schema.schema_version
    # Разбор версий
    expected_major = int(expected_version.split('.')[0])
    actual_major = int(actual_version.split('.')[0])
    
    # Major mismatch = breaking change
    if expected_major != actual_major:
        if fail_on_drift:
            raise SchemaDriftError(
                f"Schema version mismatch: expected {expected_version}, "
                f"got {actual_version}. Major version change indicates breaking changes."
            )
        else:
            logger.warning(
                "Schema drift detected",
                expected=expected_version,
                actual=actual_version
            )
```

**Проверка совместимости:**

Матрица допустимых апгрейдов:

| From | To | Compatibility | Required Actions |
|------|-----|---------------|------------------|
| 2.0.0 | 2.1.0 | ✅ Compatible | Нет |
| 2.0.0 | 3.0.0 | ⚠️ Breaking | Migration script |
| 2.1.0 | 2.0.0 | ❌ Incompatible | Downgrade запрещен |
| 2.x.x | 3.0.0 | ⚠️ Breaking | Полный перезапуск пайплайна |

```python
def is_compatible(from_version: str, to_version: str) -> bool:
    """Проверяет совместимость версий."""
    from_major = int(from_version.split('.')[0])
    to_major = int(to_version.split('.')[0])
    return from_major == to_major  # Major версия должна совпадать
```

**Хранение column_order в схеме (источник истины):**

**Инвариант:** column_order — единственный источник истины в схеме; meta.yaml содержит копию; несоответствие column_order схеме фиксируется в `PipelineBase.export()` как fail-fast до записи; NA-policy обязательна для всех таблиц.【F:src/bioetl/pipelines/base.py†L826-L855】

```python
# schema.py (Schema Registry) — источник истины
class DocumentSchema(BaseSchema):
    column_order = ["document_chembl_id", "title", "journal", ...]

# При экспорте
PipelineBase.export(df, output_path)

# В meta.yaml (только для справки)
meta = {
    "column_order": schema.column_order,  # Копия из схемы
    ...
}
```

**Fail-fast при несоответствии:**

```python
def validate_column_order(df: pd.DataFrame, schema: BaseSchema) -> None:
    """Валидация соответствия порядка колонок схеме."""
    if list(df.columns) != schema.column_order:
        raise SchemaValidationError(
            f"Column order mismatch: expected {schema.column_order}, "
            f"got {list(df.columns)}"
        )
```

13) NA-policy и precision-policy (MUST)

Единый источник истины для NA-policy и precision-policy — Pandera схема. Все пайплайны обязаны следовать этим правилам при нормализации данных и генерации хешей.

**Инвариант:** Единый источник истины для NA-policy и precision-policy — Pandera схема. Все пайплайны обязаны следовать этим правилам при нормализации данных и генерации хешей.

**NA-policy (Null Availability Policy):**

Определение: Политика обработки пропущенных значений для детерминированной сериализации и хеширования.

| Тип данных | NA-значение | JSON сериализация | Применение |
|------------|-------------|-------------------|------------|
| `str` / `StringDtype` | `""` (пустая строка) | `""` | Все текстовые поля |
| `int` / `Int64Dtype` | `None` → `null` | `null` | Все целочисленные поля |
| `float` / `Float64Dtype` | `None` → `null` | `null` | Все числовые поля |
| `bool` / `BooleanDtype` | `None` → `null` | `null` | Логические флаги |
| `datetime` | `None` → ISO8601 UTC | ISO8601 string | Временные метки |
| `dict` / JSON | `None` или `{}` | Canonical JSON | Вложенные структуры |

Применяется при:
- Нормализации данных
- Канонической сериализации для хеширования
- Записи в CSV/JSON/Parquet

**Precision-policy:**

Определение: Политика округления для числовых полей, обеспечивающая детерминизм и научную точность.

Единая карта точности для числовых полей:

| Поле | Precision | Формат | Обоснование |
|------|-----------|--------|-------------|
| `standard_value` | 6 | `%.6f` | Научная точность |
| `pic50` | 6 | `%.6f` | Фармакологические расчеты |
| `pchembl_value` | 2 | `%.2f` | log10-значения |
| `molecular_weight` | 2 | `%.2f` | Достаточно для молекул |
| `logp` | 3 | `%.3f` | Коэффициент распределения |
| `rotatable_bonds` | 0 | `%.0f` | Целочисленные дескрипторы |
| `tpsa` | 2 | `%.2f` | Polar surface area |
| Default (остальные `float`) | 6 | `%.6f` | Default для детерминизма |

Применяется при:
- Форматировании float значений
- Сериализации для хеширования
- Записи в CSV/JSON

Пример применения:

```python
def format_float(value: float, field_name: str) -> str:
    """Форматирует float согласно precision_policy."""
    precision_policy = {
        "standard_value": 6,
        "pic50": 6,
        "pchembl_value": 2,
        "molecular_weight": 2,
        "logp": 3,
        "rotatable_bonds": 0,
        "tpsa": 2,
    }
    decimals = precision_policy.get(field_name, 6)  # Default 6
    return f"{value:.{decimals}f}"
```

Обоснование:
- Детерминизм: одинаковое округление даёт одинаковый хеш
- Научная точность: 6 decimal places достаточно для IC50/Ki
- Экономия памяти: разумный баланс

**Каноническая сериализация для hash_row:**

Правила для детерминированного хеширования:

```python
def canonicalize_row_for_hash(
    row: dict[str, Any],
    column_order: list[str],
    *,
    string_columns: Collection[str],
) -> str:
    """
    Каноническая сериализация строки для детерминированного хеширования.
    
    Правила:
    1. JSON с sort_keys=True, separators=(',', ':')
    2. ISO8601 UTC для всех datetime с суффиксом 'Z'
    3. Float формат: %.6f
    4. NA-policy: строковые → "", остальные → None
    5. Column order: строго по column_order
    """
    from collections.abc import Collection
    from datetime import datetime, timezone
    import json
    import pandas as pd

    canonical = {}

    for col in column_order:
        value = row.get(col)

        # Применяем NA-policy: строковые → "", остальные → None
        if pd.isna(value):
            canonical[col] = "" if col in string_columns else None
            continue

        elif isinstance(value, float):
            canonical[col] = float(f"{value:.6f}")  # Фиксированная точность

        elif isinstance(value, datetime):
            canonical[col] = value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        elif isinstance(value, (dict, list)):
            canonical[col] = json.loads(json.dumps(value, sort_keys=True))  # Нормализация

        else:
            canonical[col] = value

    return json.dumps(canonical, sort_keys=True, ensure_ascii=False)
```

Правила канонической сериализации:
1. JSON с `sort_keys=True`, `separators=(',', ':')`
2. ISO8601 UTC для всех datetime с суффиксом 'Z'
3. Float формат: `%.6f`
4. NA-policy: строковые → `""`, остальные → `null`
5. Column order: строго по `schema.column_order`

**Проверка соответствия meta.yaml → schema:**

Meta.yaml должна содержать копию NA-policy и precision-policy из схемы для аудита:

```python
def generate_meta_with_policies(schema: BaseSchema, df: pd.DataFrame) -> dict:
    """Генерирует meta.yaml с копией политик из схемы."""
    return {
        "column_order": schema.column_order,  # Копия
        "na_policy": schema.na_policy,  # Копия
        "precision_policy": schema.precision_policy,  # Копия
        "pipeline_version": schema.schema_version,
        "row_count": len(df),
        # ... остальные метаданные
    }
```

Валидация:

```python
def validate_meta_policies(meta: dict, schema: BaseSchema) -> None:
    """Проверяет соответствие политик в meta.yaml схеме."""
    assert meta["column_order"] == schema.column_order
    assert meta.get("na_policy") == schema.na_policy
    assert meta.get("precision_policy") == schema.precision_policy
```

14) Стратегия тестирования (MUST)

**Пирамида тестов:**

1. **Unit-тесты** — клиент/политики, парсер, нормализатор, схемы
   - Покрытие каждого слоя изоляцией
   - Моки для внешних зависимостей

2. **Contract-тесты** — статические фикстуры HTTP
   - Проверка контрактов API без реальных запросов
   - Валидация форматов ответов

3. **Property-based тесты** — инварианты сортировки, курсоров, маппингов
   - Использование Hypothesis для генерации тестовых данных
   - Проверка инвариантов при различных входных данных
   - Минимальные настройки (кол-во примеров/seed) зафиксированы

4. **E2E-тесты** — пайплайн целиком, сверка с golden
   - Полный прогон пайплайна с реальными данными
   - Сравнение вывода с эталонными golden-файлами
   - Проверка идемпотентности на повторный прогон

**Golden-тесты:**

Golden-файлы должны быть:
- Детерминированными (бит-в-бит идентичность при одинаковом вводе)
- Обновляться через единый helper
- Иметь фиксированные checksums
- Связываться с коммитом через `meta.yaml`

**Обновление golden-файлов:**

```python
def update_golden(
    actual_output: Path,
    golden_path: Path,
    run_id: str,
    git_commit: str
) -> None:
    """Обновляет golden-файл с валидацией детерминизма."""
    # Валидация перед записью
    validate_determinism(actual_output)
    # Атомарная запись
    atomic_write(actual_output, golden_path)
    # Фиксация метаданных
    write_golden_meta(golden_path, run_id, git_commit)
```

**Acceptance Criteria:**

Все тесты должны проходить acceptance criteria:

**AC-01: Golden Compare Детерминизма**

Цель: Проверка бит-в-бит воспроизводимости вывода.

Команда:
```bash
python pipeline.py --input X --golden data/output/golden/Y.csv
```

Ожидаемый результат: "✅ Deterministic output confirmed" или пустой `diff.txt`.

**AC-02: Запрет Частичных Артефактов**

Цель: Гарантировать, что все файлы записаны полностью или не записаны вообще.

Проверка:
```python
# После write()
for artifact in [artifacts.dataset, artifacts.quality_report, ...]:
    assert artifact.exists(), f"Missing: {artifact}"
    assert artifact.stat().st_size > 0, f"Empty: {artifact}"
```

Порог: Нет частичных файлов в финальной директории.

**AC-03: Column Order из Схемы**

Цель: Гарантировать, что порядок колонок соответствует Schema Registry.

Момент проверки: Проверка выполняется **после** Pandera валидации схемы, **перед** атомарной записью.

Проверка:
```python
# После Pandera validation
df = schema.validate(df, lazy=True)

# Применяем column_order из схемы (источник истины)
df = df[schema.column_order]

# Проверка перед записью
assert list(df.columns) == schema.column_order
```

Ожидаемое: Полное совпадение порядка колонок.

**AC-08: Schema Drift Detection**

Цель: Гарантировать fail-fast при несовместимых изменениях схемы.

Тест:
```python
# Запуск с несовпадающей major-версией и --fail-on-schema-drift
schema = SchemaRegistry.get("document.chembl", expected_version="3.0.0", fail_on_drift=True)

# Ожидаемое: SchemaDriftError

# Без флага - warning
schema = SchemaRegistry.get("document.chembl", expected_version="3.0.0", fail_on_drift=False)

# Ожидаемое: warning в логах
```

Порог: exit != 0 при fail_on_drift=True и major mismatch.

**AC-05: NA-Policy в Сериализации**

Цель: Проверка применения канонической политики NA.

Тестовые данные:
- Строковое NA → `""`
- Числовое NaN → `null` в JSON, пустое в CSV

Проверка:
```python
# После канонической сериализации
serialized_row = canonicalize_row_for_hash(row, column_order)
assert '"string_field": ""' in serialized_row  # пустая строка
assert '"numeric_field": null' in serialized_row  # null
```

**AC-07: Respect Retry-After (429)**

Цель: Гарантировать корректную обработку HTTP 429 с Retry-After заголовком.

Тест:
```python
# Mock 429 ответа с Retry-After: 7
response.status_code = 429
response.headers['Retry-After'] = '7'

# Запрос
result = client.get("/api/data", params={"limit": 100})

# Ожидаемое: логирование retry_after=7 и attempt

# Проверяем лог
assert "Rate limited by API" in log_output
assert "retry_after=7" in log_output
assert "attempt=1" in log_output
```

Порог: Время ожидания >= указанному Retry-After.

**AC-19: Fail-Fast на 4xx (кроме 429)**

Цель: Гарантировать немедленное прекращение ретраев при клиентских ошибках.

Тест:
```python
# Mock 400 ответа
response.status_code = 400

# Запрос
try:
    result = client.get("/api/data", params={"invalid": "param"})
except Exception:
    pass

# Ожидаемое: только 1 попытка, не 3
assert attempt == 1
assert "Client error, giving up" in log_output
```

Порог: Нет ретраев на 4xx (кроме 429).

Краткие ссылки на первичные источники

RFC 2119/BCP 14 — нормативные ключевые слова. 
datatracker.ietf.org
+1

Pandera — валидация датафреймов, checks/типизация, fail-fast. 
pandera.readthedocs.io
+1

Atomic writes — атомарная запись и AtomicWriter. 
python-atomicwrites.readthedocs.io
+1

Hypothesis — property-based testing. 
Hypothesis

SemVer — правила версионирования. 
Semantic Versioning

Trunk-based — короткоживущие ветки и частые мержи. 
Trunk Based Development
+1

structlog — структурное логирование в Python. 
structlog

## Описание текущего состояния (as-is)

> Срез текущего поведения пайплайнов на март 2025 года. Документ служит
> контрольной точкой перед крупными изменениями: он описывает то, что уже
> работает в `test_refactoring_32`, и помогает выявлять регрессии.

### 0) Методика инвентаризации (текущее состояние)

* Скрипт: `python src/scripts/run_inventory.py --config configs/inventory.yaml`.
* Выходные артефакты фиксируются в `docs/requirements/PIPELINES.inventory.csv`
  и `docs/requirements/PIPELINES.inventory.clusters.md`.
* Режим `--check` используется в CI. Он сравнивает срез с рабочим деревом и
  завершает выполнение с ошибкой при расхождениях.
* Запись выполняется детерминированно (`lineterminator="\n"`, сортировка
  записей в обработчиках). Таймстемп для отчёта о кластерах вычисляется из
  максимального `mtime` среди собранных модулей.

## 1) Семьи файлов и текущее состояние

### 1.1 HTTP-клиенты

* Все сетевые вызовы проходят через `bioetl.core.api_client.UnifiedAPIClient`.
  Клиент создаётся фабрикой `bioetl.core.client_factory.APIClientFactory` и
  использует `requests.Session` с повторным использованием соединений.
* Поверх `Session` настроены:
  * `TokenBucketLimiter` для мягкого rate-limit (jitter активирован по
    умолчанию и логирует ожидания длиннее секунды).
  * `CircuitBreaker` со счётчиком ошибок и состояниями `closed → half-open →
    open`. Порог и таймаут читаются из `APIConfig.cb_failure_threshold` и
    `APIConfig.cb_timeout`.
  * `RetryPolicy`, который обрабатывает `requests.exceptions.RequestException`.
    По умолчанию повторяются `429` и `5xx`, а 4xx (кроме 429) завершают
    попытки. Значения `total`, `backoff_factor` и `backoff_max` приходят из
    конфигурации (см. раздел 3 ниже).
  * Обработка `Retry-After`: заголовок парсится в секунды, лимитируются большие
    значения и повторный запрос выполняется только после повторного получения
    токена из `TokenBucketLimiter`.
* Кэш (TTLCache) и fallback стратегии включаются на уровне конфигурации. По
  умолчанию активны `cache`, `partial_retry`, `network`, `timeout`, `5xx`.
* `fallback_manager` применяется для стратегий `network`, `timeout`, `5xx`.
  Он возвращает детерминированные заглушки, если ошибка совпадает со
  стратегией. Остальные стратегии обрабатываются напрямую в клиенте.
* `partial_retry` повторяет тот же запрос без изменения payload. Количество
  дополнительных попыток ограничено `partial_retry_max` (по умолчанию 3 из
  `FallbackOptions`, для `target` — 2 согласно
  `src/bioetl/configs/pipelines/target.yaml`).
* ChEMBL-пайплайны используют helper
  `bioetl.core.chembl.build_chembl_client_context`, который:
  * Подтягивает значения по умолчанию из `includes/chembl_source.yaml`.
  * Ограничивает размер батча (`batch_size_cap`) и длину URL, которые затем
    учитываются в `DocumentChEMBLClient`/`ActivityClient` и т.д.
* Пайплайны enrichment (`document`, `target`, `activity`) регистрируют клиентов
  через `PipelineBase.register_client`, что гарантирует корректное закрытие
  сессии в `PipelineBase.close_resources`.
* `DocumentChEMBLClient.fetch_documents` рекурсивно делит запрос на меньшие
  чанки при превышении лимита URL или таймауте чтения, поддерживает кэш по
  ключу `document:{release}:{chembl_id}` и создаёт fallback-строки через
  `DocumentFetchCallbacks.create_fallback`.

#### Рекомендации по ретраям и лимитам (MUST)

| Источник / профиль | total | backoff_multiplier | backoff_max | statuses |
|--------------------|:-----:|:------------------:|:-----------:|:--------:|
| `http.global` (`src/bioetl/configs/base.yaml`) | 5 | 2.0 | 120.0 | 408, 425, 429, 500, 502, 503, 504 |
| `target.chembl` (`src/bioetl/configs/pipelines/target.yaml`) | 5 | 2.0 | 120.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 |
| `target.uniprot*` (`target.yaml`) | 4 | 2.0 | 90.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 |
| `target.iuphar` (`target.yaml`) | 4 | 2.0 | 60.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 |

* Новые источники должны синхронизировать таблицу выше с фактическими YAML,
  чтобы документация и конфиги совпадали.
* Если профиль HTTP отсутствует, `APIClientFactory` подставляет `_DEFAULT_*`
  (3 попытки, `backoff_max=60` и `statuses=[429, 500, 502, 503, 504]`). Такие
  значения нужно отражать в документации источника до добавления явной записи в
  конфиг.

### 1.2 Пагинация и повторное извлечение

* OpenAlex и Crossref используют cursor-based пагинацию через
  `sources/<source>/pagination.CursorPaginator`. Реализация сохраняет набор
  уникальных ключей и прекращает работу при пустой выборке или отсутствии
  `next_cursor`.
* PubMed применяет `WebEnvPaginator`: сначала выполняется `esearch` для
  получения `WebEnv`/`QueryKey`, затем итеративно вызывается `efetch` с
  детерминированным `retmax`. Ответы парсятся функциями из
  `sources/pubmed/parser`.
* Пайплайны ChEMBL (`document`, `activity`, `target`) делят входные списки на
  батчи фиксированного размера (применяя ограничение `batch_size_cap` и
  `max_url_length`) и повторно вызывают клиент при пустой выдаче.
* Все пагинаторы логируют ключевые события (`no_results`, `page_limit`), что
  используется QC-отчётами.

### 1.3 Парсеры и нормализаторы

* `sources/<source>/parser` содержат чистые функции, возвращающие
  `dict[str, Any]`/`list[dict[str, Any]]`. Они используют helpers из
  `bioetl.utils` и Pandas только для приведения типов.
* Нормализация реализована в `bioetl.normalizers`:
  * `NormalizerRegistry` регистрирует базовые нормализаторы (строки, числа,
    идентификаторы, библиографию).
  * Источники вызывают `registry.normalize("identifier", value)` и другие
    обобщённые операции. Пример: документ-пайплайн формирует поля
    `chembl_title`, `chembl_doi`, приводит идентификаторы к каноническому виду
    и переносит булевые признаки через `coerce_optional_bool`.
* Pandera-схемы расположены в `bioetl/schemas`. Регистрация выполняется через
  `bioetl.schemas.registry`. `DocumentPipeline` и другие пайплайны валидируют
  как «сырые», так и нормализованные фреймы и собирают ошибки через
  `_summarize_schema_errors`.

### 1.4 Координация пайплайна и вывод

* `PipelineBase` централизует чтение входных файлов (`read_input_table`),
  работу с режимом `limit`, учёт `run_id` и регистрацию клиентов.
* Материализация организована через `bioetl.core.output_writer`. Он создаёт
  временные файлы, фиксирует `meta.yaml` (хеши, длительности, версии
  конфигов), поддерживает дополнительный вывод QC и параллельные таблицы.
* `finalize_output_dataset` гарантирует сортировку и порядок колонок согласно
  `determinism.column_order` из конфигурации.

## 2) Контрольные точки и артефакты

* Базовый CSV и отчёт по кластерам: `docs/requirements/PIPELINES.inventory.*`.
* Golden-тесты пайплайнов (pytest + coverage): `artifacts/baselines/golden_tests/`.
* QC отчёты и показатели: формируются в каталоге `data/output/<pipeline>/qc`
  через хелперы `append_qc_sections` и `persist_rejected_inputs`.

## 3) Приёмочные критерии

* Все источники используют `UnifiedAPIClient`/`APIClientFactory`; конфигурация
  ретраев и rate-limit совпадает с YAML.
* Пагинация не содержит ручных «while True» без контроля курсора; источники
  опираются на общие классы из `sources/<source>/pagination` или деление батча
  в клиентах.
* Нормализация проходит через `NormalizerRegistry`, а Pandera схемы
  зарегистрированы и задействованы в тестах.
* Детерминизм выгрузок (порядок, кодировка, хеши) обеспечивается через общий
  writer и проверяется golden-тестами.
