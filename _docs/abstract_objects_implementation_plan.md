# План реализации абстрактных объектов и структуры пайплайна

Этот план описывает, как внедрить абстрактные интерфейсы и файловую структуру из документа "Структура директорий для пайплайна обработки данных" в кодовую базу `bioetl`. Он опирается на существующие соглашения (см. `docs/pipeline_structure.md` и `docs/architecture_plan.md`) и фиксирует порядок действий, зависимости и ожидаемые артефакты.

## Цели
- Согласовать дерево каталогов для абстрактных слоёв пайплайна с текущей структурой пакета `bioetl`.
- Реализовать обязательные интерфейсы (ABC) для оркестрации, источников, обработки, валидации, вывода и утилит.
- Обеспечить готовность к последующей реализации конкретных пайплайнов (например, ChEMBL) без изменения контрактов.

## Целевая структура каталогов
Все модули располагаются в `bioetl/core/pipeline` и соседних подпакетах, чтобы не дублировать существующие доменные и инфраструктурные компоненты.

```
bioetl/core/pipeline/
├── __init__.py
├── base.py            # StageABC, базовый конвейер и соглашения по run/close
├── hook.py            # PipelineHookABC
├── cli_command.py     # CLICommandABC
├── source/
│   ├── __init__.py
│   ├── client.py      # SourceClientABC, RateLimiterABC, RetryPolicyABC
│   ├── request_builder.py
│   └── parser.py      # ResponseParserABC, PaginatorABC
├── processing/
│   ├── __init__.py
│   ├── transform.py   # TransformerABC
│   ├── enrich.py      # LookupEnricherABC, SideInputProviderABC
│   └── dedup.py       # DeduplicatorABC, BusinessKeyDeriverABC, MergeStrategyABC, HasherABC
├── validation/
│   ├── __init__.py
│   ├── schema.py      # SchemaProviderABC
│   ├── validator.py   # ValidatorABC, ValidationResult, ValidationError
│   └── dq_rules.py    # DQRuleABC, DQIssue
├── output/
│   ├── __init__.py
│   ├── writer.py      # WriterABC, MetadataWriterABC
│   └── path_strategy.py
└── utils/
    ├── __init__.py
    ├── config.py      # ConfigResolverABC, SecretProviderABC
    ├── cache.py       # CacheABC
    ├── logging.py     # LoggerAdapterABC
    ├── tracing.py     # TracerABC
    ├── error.py       # ErrorPolicyABC, ErrorAction
    └── progress.py    # ProgressReporterABC
```

## Порядок реализации
1. **Подготовка пакетов**
   - Создать подпакеты `source`, `processing`, `validation`, `output`, `utils` внутри `bioetl/core/pipeline` с `__init__.py` и ссылками на экспортируемые интерфейсы.
   - В `__init__.py` верхнего уровня экспортировать ключевые ABC для удобного импорта (например, `from .base import StageABC`).

2. **Оркестрация (`pipeline/`)**
   - Реализовать `StageABC` с протоколом `run(context)` и обязательным `close()`/контекстным менеджером.
   - Добавить `PipelineHookABC` с событиями начала/завершения стадии и ошибками.
   - Определить `CLICommandABC` с методами регистрации команд и запуска пайплайна, ориентируясь на будущий CLI на `click/typer`.

3. **Источник (`source/`)**
   - Задать интерфейсы `RequestBuilderABC`, `PaginatorABC`, `SourceClientABC`, `RateLimiterABC`, `RetryPolicyABC`, `ResponseParserABC`.
   - Фиксировать контракт по работе с HTTP (например, возврат сырых JSON-объектов, интеграция с `backoff` и `requests`).

4. **Обработка (`processing/`)**
   - Реализовать `TransformerABC` для детерминированных преобразований DataFrame.
   - Добавить `LookupEnricherABC` и `SideInputProviderABC` для обогащения справочниками.
   - В `dedup.py` задать контракты вычисления бизнес-ключей (`BusinessKeyDeriverABC`), хеширования (`HasherABC`), стратегии слияния (`MergeStrategyABC`) и дедупликации (`DeduplicatorABC`).

5. **Валидация (`validation/`)**
   - Определить `SchemaProviderABC` для поставки Pandera-схем.
   - Реализовать базовые DTO `ValidationResult`, `ValidationError` и интерфейс `ValidatorABC` для выполнения схем и DQ-правил.
   - Описать `DQRuleABC` и `DQIssue` для правил качества данных и их результатов.

6. **Вывод (`output/`)**
   - Задать `PathStrategyABC` для детерминированного формирования путей вывода.
   - Определить `WriterABC` и `MetadataWriterABC` для атомарной записи CSV и метаданных запуска.

7. **Утилиты (`utils/`)**
   - Реализовать интерфейсы конфигурации (`ConfigResolverABC`, `SecretProviderABC`), кеша (`CacheABC`), логирования (`LoggerAdapterABC`), трассировки (`TracerABC`), управления ошибками (`ErrorPolicyABC`, `ErrorAction`) и прогресса (`ProgressReporterABC`).

8. **Интеграция с существующими документами и кодом**
   - Выравнивать сигнатуры и названия с описаниями в `docs/*.md` (например, `StageABC.md`, `WriterABC.md`).
   - Подготовить таблицу соответствия между документацией и фактическими классами (в будущих коммитах).
   - При реализации конкретных пайплайнов использовать эту иерархию как единственный источник контрактов, избегая дублирования в других пакетах.

## Результат
После выполнения шагов в кодовой базе появится согласованный набор абстрактных классов и единообразная структура директорий, готовая к подключению конкретных источников, трансформаций и CLI-команд без изменения доменных моделей.
