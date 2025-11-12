# Спецификация базового класса и миксинов PipelineBase

## 1. Введение

- **Цель унификации.** Новый базовый класс `BasePipeline[TRecord]` стандартизирует жизненный цикл и контракт пайплайнов, обеспечивая повторяемость и переносимость для сущностей Assay, Activity, Testitem, Target и Document. Текущая реализация `PipelineBase` уже задаёт общий каркас стадий extract → transform → validate → write и управление артефактами.【src/bioetl/pipelines/base.py:120-137】【docs/etl_contract/01-pipeline-contract.md:5-90】
- **Область ответственности.** Базовый класс оркестрирует стадии, связанные с загрузкой, нормализацией, валидацией и детерминированной записью. Конкретные адаптеры API, схемы и трансформации остаются на стороне наследников. Миксины расширяют базовый класс типовыми поведениями (handshake, пагинация, валидация, запись, логирование), которые сегодня разбросаны по коду и тестам.

## 2. Инварианты

- **Сигнатуры и чистота.** Все публичные методы базового класса должны иметь фиксированные сигнатуры (см. §3). Запрещены скрытые побочные эффекты вне документированных стадий; текущее ядро хранит состояние только через конфиг и кэшированные вычисления (например, `_extract_metadata`).【src/bioetl/pipelines/base.py:129-170】【src/bioetl/pipelines/base.py:408-460】
- **Детерминизм вывода.** Порядок строк и столбцов фиксируется через сортировку и переупорядочивание перед записью; запись выполняется атомарно через `write_dataset_atomic`/`write_yaml_atomic` и сериализует данные в каноническом формате.【src/bioetl/core/output.py:113-208】【src/bioetl/core/output.py:323-337】
- **Структурное логирование.** Все стадии обязаны логировать события с полями `pipeline`, `stage`, `dataset`, `page`, `records`, `duration_ms`, `run_id`; существующая реализация привязывает контекст через `UnifiedLogger.bind` и `UnifiedLogger.stage`, требуя обязательных полей `run_id`, `pipeline`, `stage`, `dataset`, `component`, `trace_id`, `span_id`. Дополнительные поля (`page`, `records`, `duration_ms`) вводятся спецификацией как расширение обязательного набора.【src/bioetl/pipelines/base.py:1389-1488】【src/bioetl/core/logger.py:52-142】
- **Политика ошибок.** Исключения типов `ValueError`, `TypeError`, `KeyError` используются для нарушения предусловий (например, отсутствующие столбцы), все неожиданные исключения транслируются наверх с логированием. CLI различает коды выхода: 0 – успех, 1 – ошибка пайплайна, 2 – некорректная конфигурация/ввод, 3 – сбой внешнего API.【src/bioetl/pipelines/base.py:1497-1509】【src/bioetl/cli/command.py:198-345】

## 3. Интерфейс `BasePipeline[TRecord]`

### 3.1 Публичные методы

```python
class BasePipeline(Generic[TRecord], Protocol):
    def build_query(self, config: PipelineConfig) -> Mapping[str, Any]: ...
    def extract_pages(self, client: UnifiedAPIClient, query: Mapping[str, Any]) -> Iterable[TRecord]: ...
    def normalize(self, raw: Iterable[TRecord]) -> pd.DataFrame: ...
    def map_schema(self, record: pd.Series) -> pd.Series: ...
    def row_id(self, record: pd.Series) -> str: ...
    def validate(self, df: pd.DataFrame) -> pd.DataFrame: ...
    def write(self, df: pd.DataFrame, output_path: Path, *, extended: bool = False, include_correlation: bool | None = None, include_qc_metrics: bool | None = None) -> RunResult: ...
    def run(self, config: PipelineConfig, output_path: Path, *, extended: bool = False, include_correlation: bool | None = None, include_qc_metrics: bool | None = None) -> RunResult: ...
```

### 3.2 Сопоставление с текущей реализацией

| Новый метод            | Текущее покрытие | Комментарий |
|-----------------------|------------------|-------------|
| `build_query`         | нет в ветке      | Конфигурация запросов сейчас выполняется внутри конкретных пайплайнов; требуется стандартизованный шаг. |
| `extract_pages`       | частично (`extract`, `extract_all`, `extract_by_ids`)【src/bioetl/pipelines/base.py:326-356】 | Необходимо унифицировать пагинацию с обработкой метаданных (см. миксин §4.2). |
| `normalize`           | реализовано как `transform` с подготовкой DataFrame【src/bioetl/pipelines/base.py:358-363】 | Требуется развести этапы нормализации и схемного маппинга. |
| `map_schema`          | частично через `_ensure_schema_columns`/`_reorder_columns`【src/bioetl/pipelines/base.py:1020-1178】 | Нужен явный контракт для пост-обработки записей. |
| `row_id`              | частично через `ensure_hash_columns`【src/bioetl/core/output.py:43-110】 | Следует предоставить явный способ вычисления ключа до записи. |
| `validate`            | уже реализовано (см. текущую реализацию)【src/bioetl/pipelines/base.py:1511-1678】 | Новый контракт расширяет обязательные лог-метрики. |
| `write`               | уже реализовано (см. текущую реализацию)【src/bioetl/pipelines/base.py:1203-1377】 | Требуется привязка к детерминированному писателю из миксина. |
| `run`                 | уже реализовано (см. текущую реализацию)【src/bioetl/pipelines/base.py:1379-1509】 | Новый интерфейс добавляет обязательное использование миксинов. |

### 3.3 Контракты методов

- **`build_query`**: предусловие – валидированный `PipelineConfig`; постусловие – словарь с параметрами запроса, отсортированными по ключам (детерминизм). Нарушения – `ValueError` при отсутствии обязательных полей (сравнить с проверками `_read_input_ids`).【src/bioetl/pipelines/base.py:573-650】
- **`extract_pages`**: обязателен вызов handshake (см. миксин §4.1) и ведение учёта страниц. Постусловие – итератор в фиксированном порядке страниц; ожидаемая сложность зависит от API, но минимально O(n) по числу записей. Ошибки API пробрасываются как `RequestException` через `UnifiedAPIClient` с логом `cli_pipeline_api_error`.【src/bioetl/cli/command.py:331-345】【src/bioetl/clients/chembl.py:83-159】
- **`normalize`**: должен приводить типы и очищать данные без изменения исходного порядка страниц; допускается использование Pandas. Обязательный лог `normalize_completed` с числом записей.
- **`map_schema`**: для каждой строки обеспечивает соответствие целевому столбцу `row_id`; отсутствие обязательных колонок вызывает `KeyError` по аналогии с `_ensure_schema_columns`.【src/bioetl/pipelines/base.py:1020-1100】
- **`row_id`**: возвращает канонический идентификатор, совместимый с `ensure_hash_columns`; должен быть детерминированным и стабильно сериализированным.【src/bioetl/core/output.py:43-110】
- **`validate`**: использует `SchemaValidationMixin` (см. §4.3) и обязан логировать итоги валидации (успех/ошибка), наполняя `self._validation_summary`. Нарушения схемы при строгом режиме – `pandera.errors.SchemaErrors`.【src/bioetl/pipelines/base.py:1511-1678】
- **`write`**: обязан сортировать и сериализовать данные через миксин детерминизма; возвращает `RunResult` с путями артефактов. Ошибочные типы – `TypeError`; отсутствие колонок – `ValueError`.【src/bioetl/pipelines/base.py:1203-1377】
- **`run`**: orchestration extract→normalize→map_schema→validate→write с замером времени стадий, привязкой контекста и финальной очисткой ресурсов. Все стадии логируются через `UnifiedLogger.stage`; при отсутствии ошибок метод возвращает `RunResult`. Пойманные исключения логируются `pipeline_failed` и пробрасываются. В блоке `finally` выполняется очистка клиентов и ресурсов.【src/bioetl/pipelines/base.py:1379-1510】

### 3.4 Хуки

- `on_start(config: PipelineConfig) -> None`: вызывается перед `build_query`, логирует запуск и инициализирует внешние ресурсы. Текущее ядро использует `UnifiedLogger.bind` и `plan_run_artifacts` для аналогичных задач.【src/bioetl/pipelines/base.py:231-278】【src/bioetl/pipelines/base.py:1389-1442】
- `on_page(index: int, page_meta: Mapping[str, Any]) -> None`: уведомление о начале обработки страницы. Сегодняшняя реализация хранит метаданные через `LoadMetaStore` при пагинации.【src/bioetl/clients/chembl.py:98-151】
- `on_error(exc: BaseException, ctx: Mapping[str, Any]) -> None`: добавляет контекст к ошибкам до повторного выброса; текущая реализация логирует и пробрасывает в `run`.【src/bioetl/pipelines/base.py:1497-1509】
- `on_finish(stats: Mapping[str, Any]) -> None`: вызывается после `write` и политики удержания (`apply_retention_policy`) для метрик выполнения.【src/bioetl/pipelines/base.py:1492-1495】

## 4. Миксины

> Ветка не содержит целевых миксинов; спецификация описывает ожидаемое поведение. (`нет в ветке`)

### 4.1 `ReleaseHandshakeMixin`

- **Назначение.** Кэширует результат handshake с TTL и пробрасывает `chembl_release`/`api_version` в метаданные, наследуя от практики `ChemblClient.handshake`.【src/bioetl/clients/chembl.py:59-77】【src/bioetl/clients/chembl_iterator.py:100-141】
- **Контракт.** Метод `perform_handshake(endpoint: str, *, enabled: bool = True, ttl_seconds: int = 3600) -> Mapping[str, Any]` обязан:
  - Пропустить handshake при `enabled=False`, залогировав `phase="skip"`.
  - Выполнить HTTP-запрос через `UnifiedAPIClient` и кэшировать результат до истечения TTL.
  - Обогащать метаданные `self.record_extract_metadata` версией релиза и временем запроса.
  - Логировать события `handshake_started`/`handshake_completed` с полями `pipeline`, `stage="handshake"`, `endpoint`, `duration_ms`, `run_id`.

### 4.2 `PaginatedExtractorMixin`

- **Назначение.** Оборачивает `extract_pages`, управляя пагинацией, retry/backoff и логированием. Поведение базируется на `ChemblClient.paginate` (параметр `limit`, учёт `page_meta`, фиксация load_meta).【src/bioetl/clients/chembl.py:83-159】
- **Контракт.**
  - Метод `iterate_pages(endpoint: str, params: Mapping[str, Any], *, items_key: str, page_size: int, retries: RetryConfig, backoff: RetryConfig) -> Iterator[Page]`.
  - Каждый заход логирует `page_started`/`page_completed` с `page=index`, `records=len(items)`, `duration_ms`.
  - При сетевых ошибках активируется политика `UnifiedAPIClient` (CircuitBreaker, TokenBucket).【src/bioetl/core/api_client.py:1-200】
  - После каждой страницы вызывается хук `on_page`.

### 4.3 `SchemaValidationMixin`

- **Назначение.** Инкапсулирует работу с Pandera: загрузку схем, strict/coerce, fallback при дрейфе и сбор `self._validation_summary`. Основано на текущей реализации `PipelineBase.validate`.【src/bioetl/pipelines/base.py:1511-1678】
- **Контракт.**
  - Метод `validate_dataframe(df: pd.DataFrame, schema_identifier: str, *, fail_open: bool, dataset_name: str = "primary") -> pd.DataFrame`.
  - Логирует `schema_validation_started`/`schema_validation_completed`/`schema_validation_failed`.
  - При `fail_open=True` возвращает исходный DataFrame и регистрирует предупреждение; при `False` пробрасывает `SchemaErrors`.
  - Обновляет `self._validation_summary` и, при успехе, переупорядочивает колонки.

### 4.4 `DeterministicWriterMixin`

- **Назначение.** Реализует сортировку, хеширование и атомарную запись артефактов, используя `prepare_dataframe`, `ensure_hash_columns`, `write_dataset_atomic`, `write_yaml_atomic`, `emit_qc_artifact`.【src/bioetl/core/output.py:113-337】
- **Контракт.**
  - Метод `write_deterministic(df: pd.DataFrame, artifacts: RunArtifacts, *, stage_durations_ms: Mapping[str, float]) -> WriteResult`:
    - Применяет сортировку и порядок колонок согласно `PipelineConfig.determinism`.
    - Гарантирует запись через временные файлы и `os.replace`.
    - Заполняет `meta.yaml` с полями, перечисленными в `serialise_metadata` (UTC, sorted keys).【src/bioetl/core/output.py:234-296】
    - Возвращает `WriteResult` с путями датасета, отчётов QC и метаданных.

### 4.5 `UnifiedLoggerMixin`

- **Назначение.** Привязывает структурный логер, добавляя обязательные поля и stage-контексты. Базируется на `UnifiedLogger` и его процессорах. Требует JSON-формат с сортировкой ключей.【src/bioetl/core/logger.py:52-191】
- **Контракт.**
  - Метод `logger(self) -> BoundLogger` возвращает привязанный логер.
  - Метод `log_stage(stage: str)` реализует контекстный менеджер, делегирующий `UnifiedLogger.stage`.
  - Обеспечивает автоматическое добавление `duration_ms`, `records` и `page` перед записью сообщения.

## 5. Совместимость с CLI и миграция конфигов

- **Команды CLI.** Базовый класс должен оставаться совместимым с регистрацией Typer-команд через `create_pipeline_command`; сигнатуры `run` и `write` обязаны принимать `output_path` и флаги `extended`, `include_correlation`, `include_qc_metrics`, чтобы существующий фабричный код не менялся.【src/bioetl/cli/app.py:21-67】【src/bioetl/cli/command.py:93-349】
- **Маппинг конфигов.** Миграция YAML-конфигов выполняется слоем сопоставления, зафиксированным в `CONFIG_SCHEMA.yaml` (`api.endpoint` → `client.base_url`, `output.dir` → `output_path` и т.д.).【REPORT/CONFIG_SCHEMA.yaml:1-109】
- **Шим профилей.** Существующие профили (`base.yaml`, `determinism.yaml`) остаются источником истины; новый базовый класс обязан поддерживать включение профилей через `PipelineConfig.extends`. Нарушение схемы должно приводить к ошибке валидации (см. §2 и §4.3).

## 6. Ограничения и допущения

- Миксины `ReleaseHandshakeMixin`, `PaginatedExtractorMixin`, `SchemaValidationMixin`, `DeterministicWriterMixin`, `UnifiedLoggerMixin` отсутствуют в текущей ветке (нет в ветке); спецификация определяет требования к их реализации.
- Разделение стадий `normalize` и `map_schema` – новое требование; текущие пайплайны выполняют обе функции в `transform`. Необходимо адаптировать наследников.
- TTL-кэш handshake и расширенные поля логов – новые обязательства; существующий код кэширует handshake без TTL и не логирует `duration_ms`, поэтому внедрение потребует доработки `UnifiedLoggerMixin` и клиентов.【src/bioetl/clients/chembl.py:59-77】

## 7. Примеры последовательностей вызовов

```python
def run_pipeline(pipeline: BasePipeline[dict], config: PipelineConfig, output_path: Path) -> RunResult:
    pipeline.on_start(config)
    query = pipeline.build_query(config)
    client = pipeline.init_client(config)  # предоставляется наследником
    handshake_meta = pipeline.perform_handshake(endpoint="/status.json")
    pages = pipeline.extract_pages(client, query)
    raw_records = pipeline.collect_pages(pages)  # реализует миксин пагинации
    df_raw = pipeline.normalize(raw_records)
    df_mapped = df_raw.apply(pipeline.map_schema, axis=1)
    df_ready = pipeline.validate(df_mapped)
    result = pipeline.write(df_ready, output_path)
    pipeline.on_finish({"rows": len(df_ready), "handshake": handshake_meta})
    return result
```

## 8. Риски и значения по умолчанию

- **Retry/Backoff.** Использовать `RetryConfig` и `RateLimitConfig` из `PipelineConfig.http`, включая экспоненциальный backoff и jitter по умолчанию.【src/bioetl/config/models.py:13-119】
- **Политика NA.** Стратегия заполнения пропусков определяется `configs/defaults/normalize` (при отсутствии – `drop` по спецификации); необходимо фиксировать поведение в normalize.
- **Контроль порядка.** Параметры `determinism.sort` и `determinism.column_order` обязательны для финальной таблицы; отсутствие колонок приводит к исключениям, как в `_enforce_column_order`.【src/bioetl/core/output.py:147-156】
- **Стабилизация чисел.** Использовать `float_format` из `determinism.float_precision` при записи CSV.【src/bioetl/core/output.py:190-208】
- **TTL handshake.** Риск устаревших релизов при длительных запусках – требуется параметризованный TTL, иначе возможна рассинхронизация с QC.

## 9. Требования к тестам

- **Юнит-тесты.** Проверять порядок вызовов стадий и инварианты детерминизма, включая сортировку и атомарную запись (сравните с `TestPipelineLifecycle`).【tests/bioetl/pipelines/common/test_pipeline_lifecycle.py:21-176】
- **Тесты миксинов.** Handshake – проверка кэширования и логов; пагинация – подсчёт страниц и повторное использование backoff (по аналогии с существующими тестами клиентов ChEMBL).【tests/bioetl/clients/test_chembl_client.py:38-142】
- **Golden-тесты.** Сравнивать наборы артефактов и `meta.yaml` с эталоном (см. политику детерминизма).【docs/determinism/01-determinism-policy.md:1-195】
- **CLI интеграция.** Проверять `--dry-run`, коды выхода и отсутствие побочных эффектов при dry run (опираясь на поведение `create_pipeline_command`).【src/bioetl/cli/command.py:198-258】

## 10. Глоссарий

- **RunResult** – структура результатов запуска, содержащая пути артефактов и метрики (совпадает с текущей реализацией).【src/bioetl/pipelines/base.py:95-118】
- **WriteArtifacts** – набор путей артефактов записи, включая dataset, QC и метаданные.【src/bioetl/pipelines/base.py:53-74】
- **LoadMetaStore** – хранилище метаданных пагинации и загрузок ChEMBL, используемое при извлечении страниц.【src/bioetl/clients/chembl.py:100-151】
- **UnifiedAPIClient** – обёртка HTTP-клиента с retry/backoff/circuit breaker, гарантирует выполнение политики внешних вызовов.【src/bioetl/core/api_client.py:1-200】
