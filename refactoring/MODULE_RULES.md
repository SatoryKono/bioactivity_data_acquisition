# MODULE_RULES.md

> Базовые правила для кодовой базы `src/bioetl/sources/*` и связанных
> компонентов `core/`, `config/`, `pipelines/`. Документ отражает текущее
> состояние ветки `test_refactoring_32` и используется как эталон.

Нормативные термины MUST/SHOULD/MAY трактуются по RFC 2119/BCP 14.

## 1. Раскладка и именование

### 1.1 Дерево каталога на источник (MUST)

Каждый источник располагается в `src/bioetl/sources/<source>/` и содержит
следующие подпапки:

- `client/` — сетевые вызовы и политики отказоустойчивости.
- `request/` — сборка параметров запроса, headers, etiquette.
- `parser/` — парсинг ответов API, чистые функции без IO.
- `normalizer/` — приведение данных к единой схеме, вызовы реестра
  нормализаторов.
- `schema/` — Pandera-схемы и вспомогательные валидаторы.
- `output/` — материализация, QC-отчёты, meta.yaml.
- `merge/` — политика объединения с внешними источниками (если применимо).
- `pagination/` — тонкие адаптеры поверх `bioetl.core.pagination` для API с курсорами/offset.
- `pipeline.py` — координация этапов пайплайна.

Фактическое дерево поддерживается `src/scripts/run_inventory.py` и
публикуется в `docs/requirements/PIPELINES.inventory.csv`.

### 1.2 Именование файлов (MUST)

Имена модулей описательные: `<source>_client.py`, `<source>_parser.py`,
`<source>_normalizer.py`. Экспортируемые символы фиксируются через `__all__`.
Стиль имен — PEP 8: `snake_case` для функций и переменных, `CapWords` для
классов.

### 1.3 Тесты и документация (MUST)

- `tests/sources/<source>/` содержит модульные тесты для клиента, парсера,
  нормализатора и e2e пайплайна.
- Для каждого источника есть README в `docs/requirements/sources/<source>/`
  (API, config_keys, merge_policy, сценарии тестирования, golden-наборы).

### 1.4 Отсутствие побочных эффектов (MUST)

Импорт модулей не должен инициировать HTTP-запросы, запись на диск или менять
глобальное состояние. Допускается подготовка констант и dataclass-объектов.

Импорт любого модуля не должен выполнять сетевые вызовы, запись на диск или менять глобальное состояние. Допустима инициализация констант и лёгких датаклассов.

`tests/sources/<source>/` с `test_client.py`, `test_parser.py`, `test_normalizer.py`, `test_schema.py`, `test_pipeline_e2e.py`.
Опциональные сценарии (`test_pagination.py`, `test_merge.py`, `test_request.py`) располагаются рядом, в этой же директории.
`tests/integration/pipelines/` содержит только общие E2E-проверки (golden, bit-identical, QC) для нескольких источников.
Тесты конкретного источника размещаются исключительно в `tests/sources/<source>/`.

`docs/requirements/sources/<source>/README.md` — краткая спецификация источника (API, config_keys, merge_policy, тесты/golden).

## 2. Границы слоёв и зависимости (MUST)

### Матрица импортов (MUST)

| From \ To | core/* | client | request | pagination | parser | normalizer | schema | merge | output | pipeline |
|-----------|--------|--------|---------|------------|--------|------------|--------|-------|--------|----------|
| client    | ✔︎     | —      | —       | —          | —      | —          | —      | —     | —      | —        |
| request   | ✔︎     | ✔︎     | —       | ✔︎         | —      | —          | —      | —     | —      | —        |
| pagination| ✔︎     | —      | —       | —          | —      | —          | —      | —     | —      | —        |
| parser    | ✔︎     | —      | —       | —          | —      | —          | —      | —     | —      | —        |
| normalizer| ✔︎     | —      | —       | —          | ✔︎     | —          | ✔︎     | —     | —      | —        |
| schema    | ✔︎     | —      | —       | —          | —      | —          | —      | —     | —      | —        |
| merge     | ✔︎     | —      | —       | —          | —      | ✔︎         | ✔︎     | —     | —      | —        |
| output    | ✔︎     | —      | —       | —          | —      | —          | ✔︎     | —     | —      | —        |
| pipeline  | ✔︎     | ✔︎     | ✔︎      | ✔︎         | ✔︎     | ✔︎         | ✔︎     | ✔︎    | ✔︎     | —        |

Правила:

- `parser` MUST NOT выполнять IO; только преобразования данных.
- `normalizer` приводит единицы измерения и идентификаторы через
  `NormalizerRegistry` (MUST).
- `schema` содержит только определение схем и helper-валидаторы (MUST).
- `output` обеспечивает детерминизм и атомарную запись (MUST).
- `pipeline.py` агрегирует стадии, не дублируя логику нижних слоёв (MUST).

## 3. Конфигурация

### 3.1 Размещение (MUST)

- Общий конфиг: `src/bioetl/configs/base.yaml`.
- Расширения и include-файлы: `src/bioetl/configs/includes/`.
- Конкретные пайплайны: `src/bioetl/configs/pipelines/<pipeline>.yaml`.
- Профили HTTP (`http.<profile>`) и fallback-опции описываются в YAML и
  валидируются `bioetl.config.models`.

### 3.2 Retry/Backoff и Rate Limit (MUST)

| Профиль | total | backoff_multiplier | backoff_max | statuses | rate_limit |
|---------|:-----:|:------------------:|:-----------:|:--------:|-----------|
| `http.global` (`base.yaml`) | 5 | 2.0 | 120.0 | 408, 425, 429, 500, 502, 503, 504 | 5 calls / 15s |
| `chembl` (`pipelines/target.yaml`) | 5 | 2.0 | 120.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 | 12 calls / 1s |
| `uniprot*` (`target.yaml`) | 4 | 2.0 | 90.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 | 2–3 calls / 1s |
| `iuphar` (`target.yaml`) | 4 | 2.0 | 60.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 | 6 calls / 1s |

Рекомендации:

- Документация и конфиги должны оставаться синхронизированными. При изменении
  YAML обновляйте таблицу выше и README источника.
- Если явный профиль не указан, `APIClientFactory` использует значения по
  умолчанию (`total=3`, `backoff_max=60`, `statuses=[429, 500, 502, 503, 504]`,
  `rate_limit.max_calls=1`, `period=1.0`). Такие случаи необходимо явно
  документировать и, по возможности, добавлять профиль.
- `FallbackOptions.partial_retry_max` по умолчанию равен 3 и наследуется
  источниками; `target` переопределяет до 2. Значения должны совпадать между
  конфигом и кодом.

### 3.3 Дополнительные требования

- Параметры etiquette (например, `mailto` для Crossref) задаются в YAML и
  обрабатываются сборщиками запросов (MUST).
- Секреты читаются из окружения через конструкции `${ENV_NAME}` и валидируются
  `TargetSourceConfig` (MUST).
- Материализация datasets управляется `materialization.*` и должна иметь
  явные имена файлов и форматы.

## 4. Детерминизм и хеши (MUST)

### Хеши (MUST)

`hash_row` и `hash_business_key` — SHA256 (hex) из [src/bioetl/core/hashing.py](../src/bioetl/core/hashing.py); перед хешированием применять нормализацию типов/локали/регистров, исключить нестабильные поля (время генерации, случайные ID). Каноническая политика описана в [docs/requirements/00-architecture-overview.md](../docs/requirements/00-architecture-overview.md).

### Атомарная запись (MUST)

- Столбцы сортируются в порядке `determinism.column_order` до записи.
- CSV записываются с `lineterminator="\n"`, UTF-8, заданным `delimiter` и `quoting`.
- Пустые значения представлены единообразно (`""` или `null`).
- Запись через временный файл в той же ФС и атомарную замену (replace/move_atomic), с flush+fsync перед коммитом. Реализация — общий writer.

### Линиедж (MUST)

`meta.yaml` фиксирует размеры и хеши артефактов, версия кода/конфигов, длительности шагов, ключ сортировки, сведения о пагинации/курсоре.

## 5. Тестирование

- Unit-тесты покрывают `client`, `parser`, `normalizer`, `schema`.
- `tests/pipelines` содержит e2e сценарии с golden-файлами.
- QC-пороги проверяются в `tests/integration/qc`.
- Property-based тесты (Hypothesis) покрывают пагинацию и нормализацию там,
  где есть нестандартные правила.

## 6. MergePolicy

- Ключи объединения MUST быть задокументированы в `merge/policy.py` (например,
  `doi`, `pmid`, `molecule_chembl_id`).
- Стратегии разрешения конфликтов (`prefer_source`, `prefer_fresh`,
  `concat_unique`, `score_based`) фиксируются в артефактах.
- Объединение происходит после успешной валидации обеих сторон.

## 7. Логирование и наблюдаемость

- `bioetl.core.logger.UnifiedLogger` используется для всех модулей.
- Обязательные поля контекста: `run_id`, `stage`, `source`, `timestamp`.
- Логи проходят через фильтры `RedactSecretsFilter` и `SafeFormattingFilter`.
- Форматы: text (development/testing) и JSON (production, файлы, ротация
  10 MB × 10).
- Секреты (`api_key`, `authorization`, и т.д.) редактируются автоматически.

## 8. HTTP-запросы, rate-limit и retry

- Все HTTP запросы идут через `UnifiedAPIClient`.
- `TokenBucketLimiter` обеспечивает соблюдение rate-limit и логирует ожидания
  >1 секунды.
- `CircuitBreaker` переводится в `half-open` после `cb_timeout` и закрывается
  при успешной попытке.
- `RetryPolicy` учитывает `Retry-After` (date/seconds) и прекращает попытки на
  4xx (кроме 429) или когда достигнут `total`.
- `partial_retry` повторяет полный запрос (payload не изменяется) и
  ограничивается `partial_retry_max`.
- Fallback стратегии: `cache` (возврат данных из TTLCache), `partial_retry`,
  а также стратегии из `FallbackManager` (`network`, `timeout`, `5xx`).

## 9. Документация

- Каждое изменение публичного API источника сопровождается обновлением README
  и соответствующего раздела в `docs/requirements`.
- Отчёты инвентаризации и кластеры поддерживаются актуальными (см. раздел 3
  REFACTOR_PLAN).

## 10. Ошибки и исключения

- Используются типы: `NetworkError`, `RateLimitError`, `ParsingError`,
  `NormalizationError`, `ValidationError`, `WriteError`.
- `PipelineBase.run()` возвращает структурированную сводку. При фатальной
  ошибке артефакты не публикуются.

## 11. Совместимость и версии

- Семантическое версионирование: MINOR для совместимых изменений, MAJOR для
  ломающих.
- Депрекации документируются в `DEPRECATIONS.md` и выдерживаются минимум два
  MINOR-релиза.

## 12. Безопасность и секреты

- Секреты не хранятся в репозитории. Значения считываются из окружения или
  секрет-хранилища.
- Логи и `meta.yaml` не содержат секретов/PII.

## 13. Производительность и параллелизм

- Параллельность ограничивается слоем `client/` и конфигурацией источника.
- При параллельной загрузке сохраняется детерминизм (последующая сортировка).

## 14. Сериализация форматов

- CSV: единый диалект, явные `delimiter`, `quotechar`, `lineterminator`.
- JSON: сортировка ключей, запрет NaN/Infinity (преобразование к строкам или
  null согласно правилу источника).
- Даты/время — только RFC 3339 (UTC).

## 15. Общие компоненты core/

### 15.1 UnifiedLogger (`core/logger.py`)

- Использует `structlog` с процессорами `add_utc_timestamp`, `add_context` и
  `security_processor`.
- Поддерживает запись в файл с ротацией и консольный вывод.
- Контекст хранится в `ContextVar` и объединяется через
  `structlog.contextvars.merge_contextvars`.

### 15.2 UnifiedAPIClient (`core/api_client.py`)

- `requests.Session` с переиспользованием соединений.
- `TokenBucketLimiter` реализует rate-limit и jitter.
- `RetryPolicy` обрабатывает `RequestException`, следит за `Retry-After` и
  логирует попытки через `_RequestRetryContext`.
- `CircuitBreaker` защищает от каскадных ошибок, состояния `closed`, `open`,
  `half-open`.
- TTLCache активируется, если `cache_enabled=True`.
- Fallback стратегии выполняются в порядке, указанном в конфиге: `cache`,
  `partial_retry`, затем стратегии менеджера (`network`, `timeout`, `5xx`).
  `FallbackManager` подключён и возвращает детерминированные заглушки для
  поддерживаемых ошибок.
- `_fallback_partial_retry` повторяет запрос без изменения параметров, число
  попыток ограничено `partial_retry_max`.
- `_execute` повторно получает токен rate-limiter после `Retry-After` и
  выполняет повторный запрос.

Детерминизм результатов при параллельной выборке MUST сохраняться (пост-сортировка и стабильные ключи).

## 14. Правила сериализации форматов

CSV — единый диалект, явный delimiter, quotechar, lineterminator, quoting; недопустимо полагаться на авто-обнаружение. Опции задаются в одном месте и переиспользуются.

JSON — стабильная сортировка ключей, фиксированная стратегия для NaN/Infinity (запрещены, либо преобразуются по правилу).

Даты/время — только RFC 3339, UTC.

## 15. Компоненты core/

Общие компоненты размещаются в `src/bioetl/core/` и используются всеми источниками.

### UnifiedLogger (`src/bioetl/core/logger.py`)

Унифицированная система логирования с обязательными полями контекста.

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

Обязательные поля контекста:

- `run_id`, `stage`, `actor`, `source`, `generated_at` — всегда обязательны
- `endpoint`, `attempt`, `duration_ms`, `params` — для HTTP-запросов
- Автоматическое редактирование секретов
- Режимы: development, production, testing

Режимы работы:
- **Development**: text формат, DEBUG уровень, telemetry off
- **Production**: JSON формат, INFO уровень, telemetry on, rotation
- **Testing**: text формат, WARNING уровень, telemetry off

### UnifiedAPIClient (`core/api_client.py`)

Унифицированный HTTP-клиент с компонентами отказоустойчивости.

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

**Важно:** cachetools.TTLCache не потокобезопасен. Клиент обязан использовать его только из одного потока либо защищать операции
внешним `lock` (например, `threading.RLock`).

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

CircuitBreaker:

Защита от каскадных ошибок:

```python
class CircuitBreaker:
    """Circuit breaker для защиты API."""
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        timeout: float = 60.0
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time: float | None = None
        self.state = "closed"  # closed, open, half-open

    def call(self, func):
        """Выполняет func с circuit breaker."""
        if self.state == "open":
            if time.time() - (self.last_failure_time or 0) > self.timeout:
                self.state = "half-open"
            else:
                raise CircuitBreakerOpenError(f"Circuit breaker for {self.name} is open")

        try:
            result = func()
            if self.state == "half-open":
                self.state = "closed"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = "open"

            raise
```

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

    def get_wait_time(self, attempt: int) -> float:
        """Вычисляет время ожидания для attempt."""
        return self.backoff_factor ** attempt
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
        logger.warning("Rate limited by API",
                      code=429,
                      retry_after=wait,
                      endpoint=endpoint,
                      attempt=attempt,
                      run_id=context.run_id)
        time.sleep(wait)
    raise RateLimitError("Rate limited")
```

TokenBucketLimiter:

Rate limiting с jitter:

```python
class TokenBucketLimiter:
    """Token bucket rate limiter с jitter."""
    def __init__(
        self,
        max_calls: int,
        period: float,
        jitter: bool = True
    ):
        self.max_calls = max_calls
        self.period = period
        self.jitter = jitter
        self.tokens = max_calls
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        """Ожидает и получает token."""
        with self.lock:
            self._refill()
            if self.tokens >= 1:
                self.tokens -= 1
                if self.jitter:
                    # Добавляем случайную задержку до 10% от периода
                    jitter = random.uniform(0, self.period * 0.1)
                    time.sleep(jitter)
            else:
                # Вычисляем время ожидания
                wait_time = self.period - (time.monotonic() - self.last_refill)
                if wait_time > 0:
                    time.sleep(wait_time)
                    self._refill()
                    self.tokens -= 1

    def _refill(self):
        """Пополняет bucket."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        if elapsed >= self.period:
            self.tokens = self.max_calls
            self.last_refill = now
```

FallbackManager:

Управление fallback стратегиями:

```python
class FallbackManager:
    """Управляет fallback стратегиями."""
    def __init__(self, strategies: list[str]):
        self.strategies = strategies
        self.fallback_data: dict[str, Any] = {}

    def execute_with_fallback(
        self,
        func: Callable,
        fallback_data: dict | None = None
    ) -> Any:
        """Выполняет func с fallback."""
        try:
            return func()
        except Exception as e:
            if not self.should_fallback(e):
                raise
            data = fallback_data or self.get_fallback_data()
            logger.warning(
                "Using fallback data",
                error=str(e),
                strategy=self.get_strategy_for_error(e)
            )
            return data

    def should_fallback(self, exc: Exception) -> bool:
        """Определяет, нужно ли использовать fallback."""
        if isinstance(exc, requests.exceptions.ConnectionError):
            return "network" in self.strategies
        if isinstance(exc, requests.exceptions.Timeout):
            return "timeout" in self.strategies
        if isinstance(exc, requests.exceptions.HTTPError):
            if hasattr(exc, 'response') and exc.response:
                if 500 <= exc.response.status_code < 600:
                    return "5xx" in self.strategies
        return False
```

### UnifiedOutputWriter (`core/output_writer.py`)

Унифицированная система записи данных.

UnifiedOutputWriter — детерминированная система записи данных, объединяющая:

- Атомарную запись через временные файлы (bioactivity_data_acquisition5)
- Трехфайловую систему с QC отчетами (ChEMBL_data_acquisition6)
- Автоматическую валидацию через Pandera
- Run manifests для отслеживания пайплайнов

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

OutputArtifacts (dataclass):

Стандартизированные пути к выходным артефактам:

```python
@dataclass(frozen=True)
class OutputArtifacts:
    """Пути к стандартным выходным артефактам."""
    dataset: Path  # Основной датасет
    quality_report: Path  # QC метрики
    correlation_report: Path | None  # Корреляционный анализ (опционально)
    metadata: Path | None  # Метаданные (опционально)
    manifest: Path | None  # Run manifest (опционально)
```

Формат имен:

```
output.{table_name}_{date_tag}.csv
output.{table_name}_{date_tag}_quality_report_table.csv
output.{table_name}_{date_tag}_data_correlation_report_table.csv
output.{table_name}_{date_tag}.meta.yaml  # если extended
run_manifest_{timestamp}.json  # если extended
```

Режимы работы:

**Standard (2 файла, без correlation по умолчанию):**
- `dataset.csv`, `quality_report.csv`
- Correlation отчёт **только** при явном `postprocess.correlation.enabled: true`

**Extended (+ metadata и manifest):**
- Добавляет `meta.yaml`, `run_manifest.json`
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

QualityReportGenerator:

Автоматическая генерация QC метрик:

```python
class QualityReportGenerator:
    """Генератор quality report."""
    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Создает QC отчет."""
        metrics = []
        for column in df.columns:
            null_count = df[column].isna().sum()
            null_fraction = null_count / len(df) if len(df) > 0 else 0
            unique_count = df[column].nunique()
            duplicate_count = df.duplicated(subset=[column]).sum()
            metrics.append({
                "column": column,
                "dtype": str(df[column].dtype),
                "null_count": null_count,
                "null_fraction": f"{null_fraction:.4f}",
                "unique_count": unique_count,
                "duplicate_count": duplicate_count,
                "min": df[column].min() if pd.api.types.is_numeric_dtype(df[column]) else None,
                "max": df[column].max() if pd.api.types.is_numeric_dtype(df[column]) else None,
            })
        return pd.DataFrame(metrics)
```

CorrelationReportGenerator:

Корреляционный анализ (опционально, по умолчанию выключен):

```python
class CorrelationReportGenerator:
    """Генератор correlation report."""
    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Создает корреляционный отчет."""
        # Только числовые колонки
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) < 2:
            return pd.DataFrame()  # Пустой если недостаточно числовых колонок
        corr_matrix = df[numeric_cols].corr()
        # Преобразуем в long format
        correlations = []
        for i, col1 in enumerate(corr_matrix.columns):
            for j, col2 in enumerate(corr_matrix.columns):
                if i <= j:  # Избегаем дубликатов
                    correlations.append({
                        "column_1": col1,
                        "column_2": col2,
                        "pearson_correlation": f"{corr_matrix.loc[col1, col2]:.4f}"
                    })
        return pd.DataFrame(correlations)
```

Условная генерация корреляций:

Согласно инвариантам режима *Standard*, корреляционные отчёты выключены по умолчанию, чтобы сохранить детерминизм и минимальный AC-профиль. Генерация привязана к конфигурации `postprocess.correlation.enabled` и должна явно ветвиться в коде:

```python
def maybe_write_correlation(
    df: pd.DataFrame,
    *,
    config: PipelineConfig,
    correlation_writer: CorrelationReportGenerator,
    atomic_writer: AtomicWriter,
    correlation_path: Path,
    run_logger: BoundLogger,
):
    """Опционально создаёт корреляционный отчёт."""
    if not config.postprocess.correlation.enabled:
        run_logger.info(
            "skip_correlation_report",
            reason="disabled_in_config",
            invariant="determinism"
        )
        return None

    correlation_df = correlation_writer.generate(df)
    if correlation_df.empty:
        run_logger.info("skip_correlation_report", reason="no_numeric_columns")
        return None

    atomic_writer.write(
        correlation_df,
        correlation_path,
        float_format="%.6f",  # соблюдаем форматирование из инвариантов детерминизма
    )
    return correlation_path
```

### UnifiedSchema (`src/bioetl/schemas/registry.py`, `src/bioetl/core/unified_schema.py`)

Унифицированная система нормализации и валидации.

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

NormalizerRegistry:

Централизованный реестр нормализаторов:

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

Категории нормализаторов:

- **StringNormalizer**: нормализация строк (strip, NFC, whitespace)
- **IdentifierNormalizer**: нормализация идентификаторов (DOI, PMID, ChEMBL ID, UniProt, PubChem CID)
- **ChemistryNormalizer**: нормализация химических структур (SMILES, InChI)
- **DateTimeNormalizer**: нормализация дат в ISO8601 UTC
- **NumericNormalizer**: нормализация чисел с точностью
- **BooleanNormalizer**: нормализация логических значений
- **OntologyNormalizer**: нормализация онтологий (MeSH, GO terms)

SchemaRegistry:

Централизованный реестр Pandera-схем с версионированием. Фасад `bioetl.core.unified_schema` предоставляет официальные точки входа `register_schema()`, `get_schema()`, `get_schema_metadata()` и `is_registered()` — модульный код не должен напрямую обращаться к внутренностям `SchemaRegistry`.(см.src/bioetl/core/unified_schema.py†L19-L82】(см.src/bioetl/schemas/registry.py†L22-L211】

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

BaseSchema:

Базовый класс для всех схем:

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

Каждая схема содержит:
- `schema_id`: уникальный идентификатор (например, `document.chembl`)
- `schema_version`: семантическая версия (semver: MAJOR.MINOR.PATCH)
- `column_order`: источник истины для порядка колонок

## 16. Контракты между слоями

### Нормализатор → Схема

Нормализаторы приводят данные к UnifiedSchema, схемы валидируют соответствие:
- Нормализатор НЕ изменяет данные после валидации
- Схема НЕ выполняет нормализацию, только валидацию
- Порядок: extract → normalize → validate → write

### Парсер → Нормализатор

Парсер разбирает ответы API, нормализатор приводит к UnifiedSchema:
- Парсер — чистые функции, без IO и побочных эффектов
- Нормализатор использует результат парсера, не обращаясь к API

### Client → Parser

Client получает ответы API, parser разбирает их:
- Client возвращает сырые ответы (JSON/XML/TSV)
- Parser преобразует сырые данные в структурированные объекты

### Output → Schema

Output использует схемы для валидации перед записью:
- Валидация через `schema.validate(df, lazy=True)`
- Применение `column_order` из схемы
- Генерация метаданных из схемы (schema_id, schema_version)

## 17. Политики (NA, precision, retry)

### Централизованная политика NA-policy и Precision-policy (AUD-2)

Нормативное описание вынесено в [docs/requirements/02-io-system.md](../docs/requirements/02-io-system.md#na-precision-policy). Модульные правила фиксируют применение:

- Pandera-схемы объявляют nullable типы и precision map; нормализаторы не переопределяют значения локально.
- `UnifiedOutputWriter` использует `schema.enforce_na_policy()` и `PrecisionFormatter` до сериализации CSV/JSON/QC.
- `meta.yaml` и `run_manifest.json` копируют актуальные политики для аудита (AUD-2).
- Snapshot/golden тесты подтверждают `""` только для строк и `null` для остальных типов; несоответствия приводят к падению CI.

### Retry-policy

Политика повторных попыток для HTTP-клиентов:

**Правила:**
- 2xx, 3xx: успех, возвращаем response
- 429: respect Retry-After, ретраить
- 4xx (кроме 429): не ретраить, fail-fast
- 5xx: exponential backoff, retry

**Учёт Retry-After:**
```python
if response.status_code == 429:
    retry_after = response.headers.get('Retry-After')
    if retry_after:
        wait = min(int(retry_after), 60)  # Cap at 60s
        time.sleep(wait)
    raise RateLimitError("Rate limited")
```

**Настройка через конфиг:**
- `http.global.retries.total`: количество попыток
- `http.global.retries.backoff_multiplier`: множитель backoff
- `http.global.retries.backoff_max`: максимальная пауза

📄 **Полное описание**: [docs/requirements/03-data-extraction.md](../docs/requirements/03-data-extraction.md)

## 18. Интеграция с UnifiedLogger и UnifiedAPIClient

### Использование UnifiedLogger

Все источники обязаны использовать UnifiedLogger через `src/bioetl/core/logger.py`:

```python
from bioetl.core.logging import get_logger, set_run_context

logger = get_logger(__name__)
set_run_context(
    run_id=run_id,
    stage="extract",
    actor="scheduler",
    source="chembl"
)

logger.info("Fetching data", batch_size=25)
```

**Обязательные поля в логах:**
- Всегда: `run_id`, `stage`, `actor`, `source`, `generated_at`
- Для HTTP: `endpoint`, `attempt`, `duration_ms`, `params`

📄 **Полное описание**: [docs/requirements/01-logging-system.md](../docs/requirements/01-logging-system.md)

### Использование UnifiedAPIClient

Все источники обязаны использовать UnifiedAPIClient через `core/api_client.py`:

```python
from bioetl.core.api_client import UnifiedAPIClient, APIConfig

config = APIConfig(
    name="chembl",
    base_url="https://www.ebi.ac.uk/chembl/api/data",
    cache_enabled=True,
    rate_limit_max_calls=20,
    rate_limit_period=1.0
)
client = UnifiedAPIClient(config)
data = client.get("molecule/CHEMBL25.json")
```

**Компоненты автоматически применяются:**
- CircuitBreaker при превышении порога сбоев
- TokenBucketLimiter для rate limiting
- RetryPolicy с учётом Retry-After
- FallbackManager при сетевых ошибках

📄 **Полное описание**: [docs/requirements/03-data-extraction.md](../docs/requirements/03-data-extraction.md)

## Источники норм и практик (минимум)

- **RFC 2119/BCP 14** — трактовка MUST/SHOULD/MAY. 
- **Pandera** — схемы/валидация датафреймов, fail-fast.
- **Atomic Writes** — временный файл на той же ФС, fsync, атомарная замена.
- **structlog** — структурное логирование (JSON/logfmt), интеграция со stdlib logging.
- **Hypothesis** — property-based тесты.
- **RFC 3339** — формат временных меток (UTC).
- **CSV (stdlib)** — параметры диалектов и quoting.
