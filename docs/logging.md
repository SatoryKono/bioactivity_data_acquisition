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

