# Аудит системы логирования

## Резюме текущего состояния

Система логирования в репозитории `bioactivity_data_acquisition` использует `structlog>=24.1` с JSON-форматом вывода в stdout/stderr. Выявлено дублирование конфигурации в двух модулях, отсутствие файлового логирования, ротации и ретенции. Система имеет базовое маскирование секретов, но не использует contextvars для контекстных метаданных. В CI логи не сохраняются как артефакты, что затрудняет отладку. Обнаружено 356 использований `print()` в утилитах, обходящих систему логирования.

## Детальный анализ компонентов

| Компонент | Как сейчас | Проблемы | Доказательства (путь) |
|-----------|------------|----------|----------------------|
| **Инициализация** | Два модуля: `logger.py` и `utils/logging.py` | Дублирование логики, несогласованность конфигурации | `src/library/logger.py:12-36`, `src/library/utils/logging.py:46-60` |
| **Handlers** | Только неявный StreamHandler через `logging.basicConfig` | Нет файлового вывода, нет ротации | `src/library/logger.py:24`, `src/library/utils/logging.py:49` |
| **Ротация/Ретенция** | Отсутствует | Нет управления размером логов, нет автоочистки | — |
| **Формат** | JSON только в stdout через `JSONRenderer` | Нет человекочитаемого формата для консоли | `src/library/logger.py:32`, `src/library/utils/logging.py:54` |
| **Контекст** | Только `bind_stage()` для stage binding | Нет run_id, нет correlation_id через contextvars | `src/library/utils/logging.py:63-66` |
| **CLI флаги** | Только `--set logging.level=DEBUG` | Нет `--log-file`, `--log-format`, `--no-file-log` | `src/library/cli/__init__.py` |
| **CI интеграция** | Логи не сохраняются | Нет артефактов для отладки сбоев | `.github/workflows/ci.yaml` |
| **print() usage** | 356 использований в `tools/*.py` | Обход системы логирования, нет маскирования секретов | `src/library/tools/*.py` |

## Гэп-анализ против best-practice

### Отсутствующие компоненты

1. **Файловое логирование**: Нет `RotatingFileHandler` или `TimedRotatingFileHandler`
2. **Структурированные контексты**: Нет `contextvars` для `run_id`, `stage`, `correlation_id`
3. **Dual-format output**: Нет разделения форматов для консоли (text) и файла (JSON)
4. **CLI управление**: Нет флагов для переопределения конфигурации логов
5. **Ротация и ретенция**: Нет автоматического управления размером и возрастом логов
6. **CI artifacts**: Логи не сохраняются для отладки

### Проблемы архитектуры

1. **Дублирование**: Два модуля конфигурации с разной логикой
2. **Несогласованность**: Разные processors в `logger.py` vs `utils/logging.py`
3. **Отсутствие централизации**: Нет единой точки управления логированием

## Риски

### Высокий приоритет
- **Отладка продакшн-инцидентов**: Логи не сохраняются на диск, затруднена диагностика
- **Безопасность**: `print()` в tools/ обходит маскирование секретов

### Средний приоритет  
- **Несогласованность**: Дублирование конфигурации ведёт к багам
- **CI отладка**: Нет логов для расследования сбоев в CI

### Низкий приоритет
- **Удобство разработки**: Нет человекочитаемого формата для консоли

## Быстрые выигрыши

1. **Унификация конфигурации** (2-4ч): Создать единый модуль `logging_setup.py`
2. **ConsoleHandler с текстом** (1-2ч): Добавить читаемый формат для консоли  
3. **Замена print()** (2-3ч): Мигрировать tools/ на логгеры
4. **CI artifacts** (1ч): Сохранять логи в GitHub Actions

## Доказательства из кода

### Дублирование конфигурации

**Файл**: `src/library/logger.py:12-36`
```python
def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(level),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        cache_logger_on_first_use=True,
    )
```

**Файл**: `src/library/utils/logging.py:46-60`
```python
def configure_logging(level: str = "INFO") -> BoundLogger:
    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            _redact_secrets_processor,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )
```

### Отсутствие файлового вывода

**Доказательство**: Только `logging.basicConfig` без handlers
- `src/library/logger.py:24` — `logging.basicConfig(level=level, format="%(message)s")`
- `src/library/utils/logging.py:49` — `logging.basicConfig(level=level, format="%(message)s")`

### Маскирование секретов (частично реализовано)

**Файл**: `src/library/utils/logging.py:12-43`
```python
def _redact_secrets_processor(logger, method_name, event_dict):
    sensitive_keys = ["authorization", "api_key", "token", "password", "secret", "key"]
    # ... логика маскирования
```

### Trace ID интеграция (есть, но не используется в логах)

**Файл**: `src/library/telemetry.py:111-116`
```python
def get_current_trace_id() -> str | None:
    span = trace.get_current_span()
    if span and span.is_recording():
        return format(span.get_span_context().trace_id, "032x")
```

### Конфигурация уровней

**Файл**: `src/library/config.py:273-277`
```python
class LoggingSettings(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(default="INFO")
```

### Использование в CLI

**Файл**: `src/library/cli/__init__.py:136, 256`
```python
logger = configure_logging(config_model.logging.level)
```

### Статистика использования логгеров

- **Всего вызовов**: 114 в 18 файлах
- **ETL pipeline**: extract (2), transform (1), load (14)
- **HTTP клиенты**: base (12), circuit_breaker (5), fallback (4)
- **Documents pipeline**: 33 вызова
- **Telemetry**: 3 вызова

### print() usage

- **Всего**: 356 вхождений в 15 файлах
- **Основные файлы**: `src/library/tools/*.py` — утилиты мониторинга API
