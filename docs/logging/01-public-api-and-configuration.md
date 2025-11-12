# 1. Public API and Configuration

## Обзор

`UnifiedLogger` обеспечивает единый детерминированный слой логирования,
обобщающий настройки `structlog` и стандартного `logging`. Для конфигурирования
используется типизированный класс `LoggerConfig`, который задаёт формат, уровень
и стратегию редактирования чувствительных полей.

## Публичный API `UnifiedLogger`

Все методы определены как `@staticmethod` и не требуют инициализации экземпляра.

### `UnifiedLogger.configure(config: LoggerConfig | None = None, *, additional_processors: Sequence[Any] | None = None) -> None`

**Источник**: `src/bioetl/core/logger.py`

- Выполняет полную инициализацию `structlog` и стандартного `logging`.
- При `config=None` используется `LoggerConfig()` с настройками по умолчанию.
- `additional_processors` добавляются в конец конвейера после базовых
  процессоров.

**Исключения**:

- `ValueError` — недопустимый уровень логирования (см. `_coerce_log_level`).

### `UnifiedLogger.get(name: str | None = None) -> BoundLogger`

**Источник**: `src/bioetl/core/logger.py`

- Возвращает заранее сконфигурированный `structlog.BoundLogger`.
- По умолчанию используется имя `"bioetl"`, чтобы обеспечить стабильные ключи в
  JSON-сообщениях.

**Исключения**:

- `TypeError` — если `structlog.get_logger` вернул объект без метода `bind`.

### `UnifiedLogger.bind(**context: Any) -> None`

- Привязывает переданный контекст к глобальным `contextvars`; значения
  автоматически появятся в каждом событии.
- Рекомендуется вызывать на старте пайплайна для установки `run_id`, `pipeline`,
  `stage`, `dataset`.

### `UnifiedLogger.reset() -> None`

- Полностью очищает глобальный контекст, удаляя ранее привязанные значения.
- Используется в тестах и изолированных задачах для предотвращения утечек
  контекста.

### `UnifiedLogger.scoped(**context: Any) -> AbstractContextManager[None]`

- Возвращает контекстный менеджер, временно добавляющий/заменяющий значения в
  контексте.
- После выхода из блока исходные значения восстанавливаются.

### `UnifiedLogger.stage(stage: str, **context: Any) -> AbstractContextManager[None]`

- Упрощённый адаптер над `scoped`, который гарантированно задаёт ключ `stage`.
- Полезен для обёртывания этапов `extract/transform/load/validate`.

## Конфигурация (`LoggerConfig`)

`LoggerConfig` — это алиас для `LogConfig`; оба имени указывают на один и тот же
`@dataclass` в модуле логирования. Все поля строго типизированы и совместимы с
`mypy --strict`.

```python
from bioetl.core.logger import LogConfig, LoggerConfig, LogFormat
```

### Поля `LoggerConfig`

| Поле            | Тип             | Значение по умолчанию                     | Описание                                                                         |
| --------------- | --------------- | ----------------------------------------- | -------------------------------------------------------------------------------- |
| `level`         | `int \| str`    | `DEFAULT_LOG_LEVEL`                       | Уровень логирования, принимает числовые значения `logging` или строковые алиасы. |
| `format`        | `LogFormat`     | `LogFormat.JSON`                          | Формат вывода: структурированный JSON или key-value вывод для локальной отладки. |
| `redact_fields` | `Sequence[str]` | `("api_key", "access_token", "password")` | Список ключей, которые будут замещены `***REDACTED***` перед рендерингом.        |

### `LogFormat`

- `LogFormat.JSON` — JSON-рендерер с сортировкой ключей и ISO-8601 UTC
  таймштампами.
- `LogFormat.KEY_VALUE` — человекочитаемый вывод с фиксированным порядком
  ключей, полезен локально.

## Пример использования

```python
from bioetl.core.logger import LogFormat, LoggerConfig, UnifiedLogger

UnifiedLogger.configure(
    config=LoggerConfig(
        level="DEBUG",
        format=LogFormat.JSON,
        redact_fields=("api_key", "session_token"),
    ),
)

logger = UnifiedLogger.get(__name__)
logger.info("pipeline_started", run_id="2025-10-29T00-00Z", pipeline="chembl-activity")
```

> Примечание: Поддерживается повторный вызов `UnifiedLogger.configure`, однако
> он заменяет все обработчики `logging` с флагом `force=True`, поэтому вызывайте
> его строго один раз в процессе.
