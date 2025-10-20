## Logging & Metrics

### Логирование
- Конфигурация: `library.logging_setup` (+ optional YAML `configs/logging.yaml`)
- Управление через CLI: `--log-level`, `--log-file`, `--log-format`, `--no-file-log`
- Через env: `BIOACTIVITY__LOGGING__LEVEL`
- Редация секретов: фильтры `RedactSecretsFilter`
- Контекст: `run_id`, `stage`, `trace_id`

### Метрики
- Счётчики по стадиям: извлечено/валидно/сохранено, per-source counts
- Тайминги и объём: логируются в info/debug
- Артефакты CI: логи собираются и публикуются как artifacts

---

## Архитектура

Система логирования построена на `structlog` с JSON-форматом и поддержкой файлового вывода с ротацией и интеграцией с OpenTelemetry.

### Компоненты

- Единый модуль настройки: `library.logging_setup` (configure_logging, set_run_context, bind_stage, cleanup_old_logs)
- Контекстные переменные: `run_id`, `stage` (contextvars) и `trace_id` (OpenTelemetry)
- Процессоры structlog: TimeStamper → добавление контекста → редактирование секретов → уровень → stack info → форматирование (JSON/Console)

### Конфигурация (иерархия)

1. YAML `configs/logging.yaml`
2. ENV `BIOACTIVITY__LOGGING__*`
3. CLI `--log-level`, `--log-file`, `--log-format`, `--no-file-log`

Пример YAML:

```yaml
logging:
  level: INFO
  file:
    enabled: true
    path: logs/app.log
    max_bytes: 10485760
    backup_count: 10
    rotation_strategy: size
  console:
    format: text
  cleanup_on_start: true
```

### Handlers

- ConsoleHandler: человекочитаемый формат для разработки
- FileHandler: JSON-логи для анализа (ротация size/time)

### Интеграция с OpenTelemetry

- Добавление `trace_id` в логи
- Привязка span-атрибутов в HTTP клиентах

### Маскирование секретов

Regex-паттерны для маскирования ключей/токенов в полях и заголовках (Authorization, token, api_key и т.д.).

### Производительность

- Кэширование логгеров, условная инициализация
- Асинхронные экспортеры для трассировки

### Troubleshooting

- Диагностика конфигурации и текущего контекста
- Примеры для CI (upload logs как artifact)

