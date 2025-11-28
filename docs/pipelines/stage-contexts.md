# Контексты стадий пайплайна

Новая связка `StageContextProtocol` + `StageRuntimeContext` разделяет стабильные зависимости (логер, клиенты, конфиг, метрики) и мутабельные параметры исполнения (входные данные, артефакты, тайм-ауты).

## StageContextProtocol
- `logger`: структурированный логер, общий для всех стадий.
- `request_id` / `trace_id`: идентификаторы трассировки для корреляции событий.
- `get_client(name: str)`: лениво выдает заранее сконфигурированного клиента по имени.
- `get_config(key: str)`: возвращает значение конфигурации по ключу (например, из `PipelineConfig.metadata`).
- `emit_metric(name, value, tags=None)`: безопасный канал для публикации служебных метрик без прямой зависимости от конкретного бекенда.

### Доступные клиенты и ключи конфигурации
- Фабрика `default_chembl_factory` (см. `bioetl.clients.chembl.factories.default_chembl_factory`) регистрирует клиентов ChEMBL: `activity`, `assay`, `document`, `target`, `testitem`. Все они строятся на основе общего HTTP-клиента с ретраями и rate-limit.
- Ключи конфигурации `metadata.chembl_api.*` влияют на параметры HTTP-доступа: `base_url`, `timeout_sec`, `max_retries`, `backoff_factor`, `max_backoff_sec`, `rate_limit_calls`, `rate_limit_period_sec`, `cache_enabled`, `cache_ttl_sec`, `circuit_breaker_fail_max`, `circuit_breaker_reset_sec`, `default_headers`, `user_agent`.

### Работа с секретами
- Не сохраняйте секреты в репозитории и не логируйте их через `logger.info`/`emit_metric`.
- Подставляйте токены и ключи только через переменные окружения или внешние секрет-менеджеры, а в конфиге держите лишь ссылки (`env_var`, `vault_path`).
- Передайте чувствительные данные в клиенты через `get_config` и не сохраняйте их в `StageRuntimeContext.attributes`.

## StageRuntimeContext
- `options`: экземпляр `StageExecutionOptions` с флагами запуска.
- `input_data`: текущие входные данные стадии (меняются после выполнения каждого шага).
- `attributes`: словарь для артефактов (`artifacts`), путей (`output_dir`), агрегированных метаданных и др.
- `cancellation_token`: опциональный коллбек для раннего завершения.
- `timeout`: пользовательский тайм-аут стадии в секундах.

Результаты преобразований передаются между стадиями через возвращаемые значения хендлеров и `runtime.input_data`, а артефакты/метаданные — через явные записи в `attributes`. Это помогает избегать скрытых сайд-эффектов и упрощает тестирование.
