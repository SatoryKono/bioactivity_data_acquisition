# 1. Система логирования (UnifiedLogger)

## Обзор

UnifiedLogger — универсальная система логирования, объединяющая:
- Структурированность из **structlog** (bioactivity_data_acquisition5)
- Детерминизм через **UTC timestamps** (ChEMBL_data_acquisition6)
- Контекстное логирование через **ContextVar**
- Автоматическое редактирование секретов

## Архитектура

```text
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

## Компоненты

### 1. LogContext (dataclass)

Унифицированный контекст для всех логов:

```python
@dataclass(frozen=True)
class LogContext:
    """Контекст выполнения для логирования."""
    
    run_id: str  # UUID8 уникальный идентификатор запуска
    stage: str  # Текущий этап пайплайна
    trace_id: str | None  # OpenTelemetry trace ID
    generated_at: str  # UTC timestamp ISO8601
```

**Использование**:

```python
context = LogContext(
    run_id=generate_run_id(),
    stage="extract",
    trace_id=get_current_trace_id(),
    generated_at=datetime.now(UTC).isoformat()
)
set_log_context(context)
```

### 2. SecurityProcessor (structlog processor)

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

### 3. RedactSecretsFilter (logging.Filter)

Фильтрация секретов на уровне стандартного logging:

```python
class RedactSecretsFilter(logging.Filter):
    """Редактирует секреты в log records."""
    
    def __init__(self):
        super().__init__()
        self.patterns = [
            (re.compile(r'(?i)(token|api_key|password)\s*=\s*([^\s,}]+)'), 
             r'\1=[REDACTED]'),
            (re.compile(r'(?i)(authorization)\s*:\s*([^\s,}]+)'), 
             r'\1: [REDACTED]'),
        ]
    
    def filter(self, record):
        if hasattr(record, 'getMessage'):
            message = record.getMessage()
            for pattern, replacement in self.patterns:
                message = pattern.sub(replacement, message)
            record.msg = message
        return True
```

### 4. SafeFormattingFilter (logging.Filter)

Защита от ошибок форматирования в проблемных библиотеках (urllib3, requests):

```python
class SafeFormattingFilter(logging.Filter):
    """Защищает от ошибок форматирования в urllib3."""
    
    def filter(self, record):
        if 'urllib3' in record.name:
            if hasattr(record, 'msg') and isinstance(record.msg, str):
                record.msg = f"urllib3: {record.msg}"
                record.args = ()  # Убираем аргументы
            return True
        
        # Для остальных: проверяем форматирование
        if hasattr(record, 'msg') and isinstance(record.msg, str):
            try:
                if hasattr(record, 'args') and record.args:
                    _ = record.msg % record.args
            except (TypeError, ValueError):
                # Конвертируем проблемные аргументы в строки
                if hasattr(record, 'args') and record.args:
                    safe_args = [str(arg) for arg in record.args]
                    record.args = tuple(safe_args)
        
        return True
```

### 5. LoggerConfig (dataclass)

Единая конфигурация логгера:

```python
@dataclass
class LoggerConfig:
    """Конфигурация UnifiedLogger."""
    
    level: str = "INFO"  # DEBUG, INFO, WARNING, ERROR
    console_format: str = "text"  # text или json
    file_enabled: bool = True
    file_path: Path = Path("logs/app.log")
    file_format: str = "json"  # всегда JSON для файла
    max_bytes: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 10
    telemetry_enabled: bool = False  # OpenTelemetry
    redact_secrets: bool = True
```

## Использование

### Базовое использование

```python
from unified_logger import configure_logging, get_logger

# Инициализация
configure_logging(LoggerConfig(level="INFO", console_format="text"))
logger = get_logger("my_module")

# Логирование
logger.info("Pipeline started", stage="init", row_count=1000)
logger.warning("API rate limit approaching", remaining=5)
logger.error("Failed to fetch data", api="openalex", error=str(e), exc_info=True)
```

### Stage-based логирование

```python
from unified_logger import bind_stage

with bind_stage(logger, "extract", source="chembl"):
    logger.info("Fetching ChEMBL data", batch_size=25)
    # ... выполнение операции ...
    logger.info("Extraction complete", rows=1500)
```

### Контекстные переменные

```python
from unified_logger import set_run_context, generate_run_id

run_id = generate_run_id()
set_run_context(run_id=run_id, stage="extract", actor="scheduler")

# Теперь все логи автоматически содержат run_id, stage и actor
logger.info("Processing", step="first")
# Output: {"run_id": "a3f8d2e1", "stage": "extract", "actor": "scheduler", "step": "first", ...}
```

### Контекст и редактирование секретов

**Обязательный набор полей контекста**

**Критическое изменение:** Все контекстные поля обязательны для каждого события лога.

Каждое событие лога **обязательно** содержит:
- `run_id`: UUID8 уникальный идентификатор запуска
- `stage`: текущий этап пайплайна (extract, normalize, validate, write)
- `trace_id`: OpenTelemetry trace ID (опционально только для development)
- `source`: источник данных (chembl, pubmed, etc.)
- `endpoint`: URL эндпоинта API (для HTTP-запросов)
- `page_state`: состояние пагинации (для пагинированных запросов)
- `attempt`: номер попытки запроса (обязательно для HTTP-запросов)
- `duration_ms`: длительность операции в миллисекундах
- `level`: уровень логирования (DEBUG, INFO, WARNING, ERROR)
- `message`: текстовое описание события
- `timestamp_utc`: UTC timestamp в формате ISO8601
- `actor`: инициатор выполнения (system, scheduler, username)

**Поле actor**

Идентифицирует кто или что инициировало выполнение:
- `system`: автоматические системные запуски
- `scheduler`: запуски по расписанию (cron, Airflow)
- `<username>`: ручные запуски пользователем

```python
# Автоматический запуск
set_run_context(run_id=run_id, stage="extract", actor="system")

# Запуск по расписанию
set_run_context(run_id=run_id, stage="extract", actor="scheduler")

# Ручной запуск
set_run_context(run_id=run_id, stage="extract", actor="fedor")
```

**Правила маскирования секретов**

1. **Словарь чувствительных ключей:**

```python
SENSITIVE_KEYS = [
    "api_key", "token", "password", "secret", "authorization",
    "bearer", "auth", "credential", "access_token", "refresh_token",
    "api_secret", "private_key", "x-api-key"
]
```

2. **Паттерны для маскирования:**

```python
REDACT_PATTERNS = [
    (r'(?i)(token|api_key|password)\s*=\s*([^\s,}]+)', r'\1=[REDACTED]'),
    (r'(?i)(authorization)\s*:\s*([^\s,}]+)', r'\1: [REDACTED]'),
    (r'Bearer\s+[A-Za-z0-9\-._~+/]+', 'Bearer [REDACTED]'),
    (r'api_key":\s*"[^"]+"', 'api_key": "[REDACTED]"')
]
```

3. **Применение маскирования:**

```python
# До: {"api_key": "sk_live_abc123", "user": "john"}
# После: {"api_key": "[REDACTED]", "user": "john"}

def redact_secrets(event_dict: dict) -> dict:
    """Удаляет секреты из event_dict."""
    for key in list(event_dict.keys()):
        if any(s in key.lower() for s in SENSITIVE_KEYS):
            event_dict[key] = "[REDACTED]"
    
    # Паттерны для текстовых полей
    if 'message' in event_dict:
        text = event_dict['message']
        for pattern, replacement in REDACT_PATTERNS:
            text = re.sub(pattern, replacement, text)
        event_dict['message'] = text
    
    return event_dict
```

**Примеры логов с обязательными полями:**

Успешный HTTP-запрос:
```json
{
  "run_id": "a3f8d2e1",
  "stage": "extract",
  "trace_id": "00-a1b2c3d",
  "source": "chembl",
  "endpoint": "/api/molecule",
  "page_state": "offset=100&limit=100",
  "attempt": 1,
  "duration_ms": 1234,
  "level": "info",
  "message": "Successfully fetched 100 molecules",
  "actor": "scheduler",
  "timestamp_utc": "2025-01-28T14:23:15.123Z"
}
```

Retry при 429:
```json
{
  "run_id": "a3f8d2e1",
  "stage": "extract",
  "trace_id": "00-a1b2c3d",
  "source": "pubmed",
  "endpoint": "/api/article",
  "page_state": "offset=500&limit=100",
  "attempt": 2,
  "retry_after": 7,
  "duration_ms": 150,
  "level": "warning",
  "message": "Rate limited by API",
  "actor": "system",
  "timestamp_utc": "2025-01-28T14:23:20.456Z"
}
```

Ошибка с giveup на 4xx:
```json
{
  "run_id": "a3f8d2e1",
  "stage": "extract",
  "trace_id": "00-a1b2c3d",
  "source": "pubmed",
  "endpoint": "/api/article",
  "page_state": null,
  "attempt": 1,
  "code": 400,
  "duration_ms": 500,
  "level": "error",
  "message": "Client error, giving up",
  "error": "Bad Request",
  "actor": "fedor",
  "timestamp_utc": "2025-01-28T14:23:25.789Z"
}
```

### Интеграция с OpenTelemetry

```python
from unified_logger import configure_logging, LoggerConfig

config = LoggerConfig(
    level="INFO",
    telemetry_enabled=True  # Автоматически добавляет trace_id
)

configure_logging(config)
logger = get_logger()

logger.info("API call started", endpoint="/api/data")
# Output включает trace_id из OpenTelemetry span
```

## Режимы работы

### Development

```python
config = LoggerConfig(
    level="DEBUG",
    console_format="text",  # Читаемый вывод
    file_enabled=True,
    file_path=Path("logs/dev.log"),
    telemetry_enabled=False
)
```

### Production

```python
config = LoggerConfig(
    level="INFO",
    console_format="json",  # JSON для парсинга
    file_enabled=True,
    file_path=Path("logs/app_20250128.log"),
    file_format="json",
    max_bytes=10 * 1024 * 1024,
    backup_count=10,
    telemetry_enabled=True  # Полный трейсинг
)
```

### Testing

```python
config = LoggerConfig(
    level="WARNING",  # Только warnings и errors
    console_format="text",
    file_enabled=False,  # Без файлов в тестах
    telemetry_enabled=False
)
```

## Форматы вывода

### Console (text)

```
[2025-01-28 14:23:15] [INFO] [extract] Pipeline started stage=init row_count=1000
[2025-01-28 14:23:20] [WARNING] [extract] API rate limit approaching remaining=5
[2025-01-28 14:23:25] [ERROR] [extract] Failed to fetch data api=openalex error=Timeout
```

### Console (JSON)

```json
{"event": "Pipeline started", "level": "info", "logger": "extract",
 "stage": "init", "row_count": 1000, "timestamp": "2025-01-28T14:23:15.123Z"}

{"event": "API rate limit approaching", "level": "warning", "logger": "extract",
 "stage": "init", "remaining": 5, "timestamp": "2025-01-28T14:23:20.456Z"}

{"event": "Failed to fetch data", "level": "error", "logger": "extract",
 "stage": "init", "api": "openalex", "error": "Timeout",
 "timestamp": "2025-01-28T14:23:25.789Z"}
```

### File (JSON)

Те же JSON строки, одна на строку, с UTC timestamps и полными контекстами.

## Ротация и cleanup

### Автоматическая ротация

Логи ротируются при достижении `max_bytes` (по умолчанию 10MB):

```text
logs/
  app_20250128.log        # Текущий
  app_20250128.log.1      # Предыдущий
  app_20250128.log.2
  ...
  app_20250128.log.10     # Самый старый (удаляется при следующей ротации)
```

### Cleanup старых логов

```python
from unified_logger import cleanup_old_logs

# Удаляет логи старше 14 дней
cleanup_old_logs(older_than_days=14, logs_dir=Path("logs"))
```

## Именование файлов

Следует конвенции из ChEMBL_data_acquisition6:

```python
"{script_name}_{YYYYMMDD}.log"
```

Примеры:

- `get_document_data_20250128.log`
- `get_activity_data_20250128.log`
- `pipeline_20250128.log` (если script_name пустой)

## Best Practices

1.  **Всегда используйте context manager для stages**: `with bind_stage(logger, "stage_name"):`
2.  **Добавляйте структурированные поля**: `logger.info("message", key1=value1, key2=value2)`
3.  **Используйте exc_info для исключений**: `logger.error("message", exc_info=True)`
4.  **Не логируйте секреты**: они автоматически редактируются
5.  **Выбирайте адекватный уровень**: DEBUG для разработки, INFO для production
6.  **Используйте JSON в production**: для парсинга и анализа

## Расширение

Для добавления кастомных процессоров:

```python
def custom_processor(logger, method_name, event_dict):
    """Добавляет custom_field ко всем событиям."""
    event_dict["custom_field"] = compute_custom_value()
    return event_dict

configure_logging(
    LoggerConfig(...),
    additional_processors=[custom_processor]
)
```

## Миграция

### Из стандартного logging

```python
# Было
import logging
logger = logging.getLogger(__name__)
logger.info("message")

# Стало
from unified_logger import get_logger
logger = get_logger(__name__)
logger.info("message")
```

### Из structlog без конфигурации

```python
# Было
import structlog
logger = structlog.get_logger()
logger.info("message")

# Стало
from unified_logger import configure_logging, get_logger
configure_logging(LoggerConfig(level="INFO"))
logger = get_logger(__name__)
logger.info("message")  # Та же API
```

---

**Следующий раздел**: [02-io-system.md](02-io-system.md)
