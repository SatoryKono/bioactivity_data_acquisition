# Структура директорий для пайплайна обработки данных

Этот документ описывает рекомендуемую файловую и папочную структуру для хранения абстрактных классов и компонентов конвейера обработки данных на Python. Структура организована по функциональным слоям для повышения удобства навигации, расширяемости и поддержки проекта.

## Общая структура

```text
mypipeline/
├── pipeline/
│   ├── __init__.py
│   ├── base.py
│   ├── hook.py
│   └── cli_command.py
├── source/
│   ├── __init__.py
│   ├── client.py
│   ├── request_builder.py
│   └── parser.py
├── processing/
│   ├── __init__.py
│   ├── transform.py
│   ├── enrich.py
│   └── dedup.py
├── validation/
│   ├── __init__.py
│   ├── schema.py
│   ├── validator.py
│   └── dq_rules.py
├── output/
│   ├── __init__.py
│   ├── writer.py
│   └── path_strategy.py
└── utils/
    ├── __init__.py
    ├── config.py
    ├── cache.py
    ├── logging.py
    ├── tracing.py
    ├── error.py
    └── progress.py
```

## Описание директорий и файлов

### `pipeline/`

Ядро конвейера:

- `base.py`
  - содержит `StageABC` как абстракцию отдельной стадии;
  - может содержать тонкую обёртку/адаптер над конкретной реализацией базового пайплайна (оркестратора), если она используется в данном проекте.

  Универсальный «каркас» пайплайна может жить в отдельном пакете (например, табличный `bioetl.core.PipelineBase` для DataFrame-ориентированных задач) и импортироваться здесь. В этом случае `base.py` ограничивается экспортом и минимальной интеграцией, не размазывая прикладную логику по абстрактной библиотеке.

- `hook.py` — `PipelineHookABC`
- `cli_command.py` — `CLICommandABC`

### `source/`

Компоненты для работы с внешними источниками данных:

- `client.py` — `SourceClientABC`, `RateLimiterABC`, `RetryPolicyABC`
- `request_builder.py` — `RequestBuilderABC`
- `parser.py` — `ResponseParserABC`, `PaginatorABC`

### `processing/`

Трансформация и обогащение данных:

- `transform.py` — `TransformerABC`
- `enrich.py` — `LookupEnricherABC`
- `dedup.py` — `DeduplicatorABC`, `BusinessKeyDeriverABC`, `MergeStrategyABC`, `HasherABC`

### `validation/`

Валидация и контроль качества данных:

- `schema.py` — `SchemaProviderABC`
- `validator.py` — `ValidatorABC`, `ValidationError`, `ValidationResult`
- `dq_rules.py` — `DQRuleABC`, `DQIssue`

Абстрактный слой не навязывает конкретную библиотеку валидации. В прикладных проектах может использоваться адаптер к Pandera, Pydantic или другой системе, при этом интерфейсы остаются прежними.

### `output/`

Сохранение данных и генерация путей:

- `writer.py` — `WriterABC`, `MetadataWriterABC`
- `path_strategy.py` — `PathStrategyABC`

### `utils/`

Инфраструктурные и общие компоненты:

- `config.py` — `ConfigResolverABC`, `SecretProviderABC`
- `cache.py` — `CacheABC`
- `logging.py` — `LoggerAdapterABC`
- `tracing.py` — `TracerABC`
- `error.py` — `ErrorPolicyABC`, `ErrorAction`
- `progress.py` — `ProgressReporterABC`

## Назначение

Такая структура:

- разделяет код по функциональным областям;
- упрощает навигацию и переиспользование компонентов;
- позволяет подключать разные прикладные реализации базового пайплайна (например, табличный `bioetl.core.PipelineBase`) без загрязнения абстрактного слоя.

Абстрактные интерфейсы остаются стабильными, а конкретные реализации могут развиваться независимо в отдельных пакетах.
