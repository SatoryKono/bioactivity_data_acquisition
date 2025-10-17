# Логирование в bioactivity-data-acquisition

## Обзор

Система логирования использует `structlog` с поддержкой структурированного JSON-логирования, файлового вывода с ротацией и маскированием секретов. Логи содержат контекстные метаданные (run_id, stage, trace_id) для трассировки выполнения.

## Быстрый старт

### Базовое использование

```bash
# Запуск с логированием по умолчанию (INFO level, файлы в logs/)
bioactivity-data-acquisition pipeline --config config.yaml

# Изменение уровня логирования
bioactivity-data-acquisition pipeline --config config.yaml --log-level DEBUG

# Отключение файлового логирования
bioactivity-data-acquisition pipeline --config config.yaml --no-file-log

# JSON формат в консоли
bioactivity-data-acquisition pipeline --config config.yaml --log-format json
```

### Переменные окружения

```bash
# Уровень логирования
export BIOACTIVITY__LOGGING__LEVEL=DEBUG

# Отключение файлового логирования
export BIOACTIVITY__LOGGING__FILE__ENABLED=false

# Путь к файлу лога
export BIOACTIVITY__LOGGING__FILE__PATH=/var/log/bioactivity/app.log
```

## CLI флаги

| Флаг | Описание | Пример |
|------|----------|--------|
| `--log-level` | Уровень логирования | `--log-level DEBUG` |
| `--log-file` | Путь к файлу лога | `--log-file /tmp/pipeline.log` |
| `--log-format` | Формат консоли (text/json) | `--log-format json` |
| `--no-file-log` | Отключить файловое логирование | `--no-file-log` |

## Конфигурация

### config.yaml

```yaml
logging:
  level: INFO  # DEBUG, INFO, WARNING, ERROR, CRITICAL
  file:
    enabled: true
    path: logs/app.log
    max_bytes: 10485760  # 10MB
    backup_count: 10
    rotation_strategy: size  # size или time
  console:
    format: text  # text или json
```

### Расширенная конфигурация

```yaml
logging:
  level: DEBUG
  file:
    enabled: true
    path: logs/app.log
    max_bytes: 10485760
    backup_count: 10
    rotation_strategy: size
    retention_days: 14
  console:
    format: text
  cleanup_on_start: true
```

## Форматы логов

### Консольный формат (text)

```
2024-01-15 10:30:45 INFO library.etl.extract — Fetching data from ChEMBL API
2024-01-15 10:30:46 DEBUG library.clients.base — HTTP request completed status=200
2024-01-15 10:30:47 ERROR library.etl.transform — Validation failed: missing required field
```

### Файловый формат (JSON)

```json
{
  "timestamp": "2024-01-15T10:30:45.123456Z",
  "level": "info",
  "logger": "library.etl.extract",
  "event": "Fetching data from ChEMBL API",
  "run_id": "a1b2c3d4",
  "stage": "extract",
  "trace_id": "1234567890abcdef1234567890abcdef",
  "source": "chembl",
  "endpoint": "/api/data/activity"
}
```

## Контекстные метаданные

Все логи содержат следующие контекстные поля:

- **run_id**: Уникальный идентификатор запуска pipeline (8 символов)
- **stage**: Текущий этап выполнения (extract, transform, load, etc.)
- **trace_id**: Идентификатор OpenTelemetry trace (если доступен)

### Пример использования в коде

```python
from library.logging_setup import get_logger, bind_stage, set_run_context

# Получение логгера
logger = get_logger(__name__)

# Установка контекста
set_run_context(run_id="abc123", stage="extract")

# Логирование с контекстом
logger.info("Starting data extraction", source="chembl", records_count=1000)

# Привязка к этапу
stage_logger = bind_stage(logger, "transform")
stage_logger.info("Transforming data", input_rows=1000, output_rows=950)
```

## Ротация и ретенция

### Размерная ротация (по умолчанию)

- Максимальный размер файла: 10MB
- Количество backup файлов: 10
- Формат backup файлов: `app.log.1`, `app.log.2`, etc.

### Временная ротация

```yaml
logging:
  file:
    rotation_strategy: time
    when: midnight
    interval: 1
    backup_count: 14
```

- Ротация каждый день в полночь
- Хранение логов за 14 дней
- Формат backup файлов: `app.log.2024-01-15`, `app.log.2024-01-16`, etc.

## Маскирование секретов

Система автоматически маскирует чувствительную информацию:

- API ключи: `api_key=sk-123456` → `api_key=[REDACTED]`
- Токены: `token=abc123` → `token=[REDACTED]`
- Заголовки Authorization: `Authorization: Bearer xyz` → `Authorization: [REDACTED]`

### Настройка маскирования

```python
# В коде можно добавить дополнительные поля для маскирования
logger.info("API request", headers={"Authorization": "Bearer secret"})  # Автоматически замаскируется
```

## Очистка старых логов

### Автоматическая очистка

```yaml
logging:
  cleanup_on_start: true
  retention_days: 14
```

### Ручная очистка

```bash
# Удалить логи старше 14 дней
python scripts/cleanup_logs.py --older-than 14

# Показать что будет удалено (dry-run)
python scripts/cleanup_logs.py --older-than 7 --dry-run

# Очистить конкретную директорию
python scripts/cleanup_logs.py --logs-dir /var/log/bioactivity --older-than 30
```

## CI/CD интеграция

### GitHub Actions

Логи автоматически сохраняются как артефакты в CI:

```yaml
- name: Upload logs
  if: always()
  uses: actions/upload-artifact@v3
  with:
    name: test-logs-${{ matrix.python-version }}
    path: logs/
```

### Переменные окружения для CI

```yaml
env:
  BIOACTIVITY__LOGGING__LEVEL: DEBUG
  BIOACTIVITY__LOGGING__FILE__ENABLED: true
```

## Отладка

### Включение DEBUG логов

```bash
# Через CLI
bioactivity-data-acquisition pipeline --config config.yaml --log-level DEBUG

# Через переменную окружения
export BIOACTIVITY__LOGGING__LEVEL=DEBUG
bioactivity-data-acquisition pipeline --config config.yaml
```

### Поиск по логам

```bash
# Поиск по run_id
grep "run_id.*abc123" logs/app.log

# Поиск ошибок
grep '"level":"error"' logs/app.log

# Поиск по этапу
grep '"stage":"extract"' logs/app.log
```

### Анализ производительности

```bash
# Время выполнения этапов
grep '"stage":"extract"' logs/app.log | jq '.timestamp, .elapsed_ms'

# HTTP запросы
grep '"http"' logs/app.log | jq '.http.status_code, .http.response_time_ms'
```

## Troubleshooting

### Проблема: Логи не записываются в файл

**Решение**:
1. Проверьте права доступа к директории `logs/`
2. Убедитесь, что `logging.file.enabled: true` в конфиге
3. Проверьте, что не используется флаг `--no-file-log`

### Проблема: Секреты не маскируются

**Решение**:
1. Убедитесь, что используете логгер, а не `print()`
2. Проверьте, что секреты передаются как именованные параметры
3. Обновите список чувствительных ключей в `RedactSecretsFilter`

### Проблема: Ротация не работает

**Решение**:
1. Проверьте размер файла лога
2. Убедитесь, что `max_bytes` настроен корректно
3. Проверьте права на запись в директорию логов

## Примеры использования

### ETL Pipeline

```python
from library.logging_setup import get_logger, bind_stage

logger = get_logger(__name__)

def extract_data():
    with bind_stage(logger, "extract"):
        logger.info("Starting data extraction", source="chembl")
        # ... extraction logic
        logger.info("Extraction completed", records=1000)

def transform_data(data):
    with bind_stage(logger, "transform"):
        logger.info("Starting transformation", input_rows=len(data))
        # ... transformation logic
        logger.info("Transformation completed", output_rows=len(transformed_data))
```

### HTTP клиенты

```python
from library.logging_setup import get_logger

logger = get_logger(__name__)

def make_request(url, headers=None):
    logger.info("Making HTTP request", url=url, headers=headers)
    try:
        response = requests.get(url, headers=headers)
        logger.info("Request completed", status_code=response.status_code)
        return response
    except Exception as e:
        logger.error("Request failed", error=str(e), exc_info=True)
        raise
```
