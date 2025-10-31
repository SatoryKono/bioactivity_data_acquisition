# REFACTOR_PLAN.md

> Срез текущего поведения пайплайнов на март 2025 года. Документ служит
> контрольной точкой перед крупными изменениями: он описывает то, что уже
> работает в `test_refactoring_32`, и помогает выявлять регрессии.

## 0) Методика инвентаризации (MUST)

* Скрипт: `python src/scripts/run_inventory.py --config configs/inventory.yaml`.
* Выходные артефакты фиксируются в `docs/requirements/PIPELINES.inventory.csv`
  и `docs/requirements/PIPELINES.inventory.clusters.md`.
* Режим `--check` используется в CI. Он сравнивает срез с рабочим деревом и
  завершает выполнение с ошибкой при расхождениях.
* Запись выполняется детерминированно (`lineterminator="\n"`, сортировка
  записей в обработчиках). Таймстемп для отчёта о кластерах вычисляется из
  максимального `mtime` среди собранных модулей.

## 1) Семьи файлов и текущее состояние

### 1.1 HTTP-клиенты

* Все сетевые вызовы проходят через `bioetl.core.api_client.UnifiedAPIClient`.
  Клиент создаётся фабрикой `bioetl.core.client_factory.APIClientFactory` и
  использует `requests.Session` с повторным использованием соединений.
* Поверх `Session` настроены:
  * `TokenBucketLimiter` для мягкого rate-limit (jitter активирован по
    умолчанию и логирует ожидания длиннее секунды).
  * `CircuitBreaker` со счётчиком ошибок и состояниями `closed → half-open →
    open`. Порог и таймаут читаются из `APIConfig.cb_failure_threshold` и
    `APIConfig.cb_timeout`.
  * `RetryPolicy`, который обрабатывает `requests.exceptions.RequestException`.
    По умолчанию повторяются `429` и `5xx`, а 4xx (кроме 429) завершают
    попытки. Значения `total`, `backoff_factor` и `backoff_max` приходят из
    конфигурации (см. раздел 3 ниже).
  * Обработка `Retry-After`: заголовок парсится в секунды, лимитируются большие
    значения и повторный запрос выполняется только после повторного получения
    токена из `TokenBucketLimiter`.
* Кэш (TTLCache) и fallback стратегии включаются на уровне конфигурации. По
  умолчанию активны `cache`, `partial_retry`, `network`, `timeout`, `5xx`.
* `fallback_manager` применяется для стратегий `network`, `timeout`, `5xx`.
  Он возвращает детерминированные заглушки, если ошибка совпадает со
  стратегией. Остальные стратегии обрабатываются напрямую в клиенте.
* `partial_retry` повторяет тот же запрос без изменения payload. Количество
  дополнительных попыток ограничено `partial_retry_max` (по умолчанию 3 из
  `FallbackOptions`, для `target` — 2 согласно
  `src/bioetl/configs/pipelines/target.yaml`).
* ChEMBL-пайплайны используют helper
  `bioetl.core.chembl.build_chembl_client_context`, который:
  * Подтягивает значения по умолчанию из `includes/chembl_source.yaml`.
  * Ограничивает размер батча (`batch_size_cap`) и длину URL, которые затем
    учитываются в `DocumentChEMBLClient`/`ActivityClient` и т.д.
* Пайплайны enrichment (`document`, `target`, `activity`) регистрируют клиентов
  через `PipelineBase.register_client`, что гарантирует корректное закрытие
  сессии в `PipelineBase.close_resources`.
* `DocumentChEMBLClient.fetch_documents` рекурсивно делит запрос на меньшие
  чанки при превышении лимита URL или таймауте чтения, поддерживает кэш по
  ключу `document:{release}:{chembl_id}` и создаёт fallback-строки через
  `DocumentFetchCallbacks.create_fallback`.

#### Рекомендации по ретраям и лимитам (MUST)

| Источник / профиль | total | backoff_multiplier | backoff_max | statuses |
|--------------------|:-----:|:------------------:|:-----------:|:--------:|
| `http.global` (`src/bioetl/configs/base.yaml`) | 5 | 2.0 | 120.0 | 408, 425, 429, 500, 502, 503, 504 |
| `target.chembl` (`src/bioetl/configs/pipelines/target.yaml`) | 5 | 2.0 | 120.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 |
| `target.uniprot*` (`target.yaml`) | 4 | 2.0 | 90.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 |
| `target.iuphar` (`target.yaml`) | 4 | 2.0 | 60.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 |

* Новые источники должны синхронизировать таблицу выше с фактическими YAML,
  чтобы документация и конфиги совпадали.
* Если профиль HTTP отсутствует, `APIClientFactory` подставляет `_DEFAULT_*`
  (3 попытки, `backoff_max=60` и `statuses=[429, 500, 502, 503, 504]`). Такие
  значения нужно отражать в документации источника до добавления явной записи в
  конфиг.

### 1.2 Пагинация и повторное извлечение

* OpenAlex и Crossref используют cursor-based пагинацию через
  `sources/<source>/pagination.CursorPaginator`. Реализация сохраняет набор
  уникальных ключей и прекращает работу при пустой выборке или отсутствии
  `next_cursor`.
* PubMed применяет `WebEnvPaginator`: сначала выполняется `esearch` для
  получения `WebEnv`/`QueryKey`, затем итеративно вызывается `efetch` с
  детерминированным `retmax`. Ответы парсятся функциями из
  `sources/pubmed/parser`.
* Пайплайны ChEMBL (`document`, `activity`, `target`) делят входные списки на
  батчи фиксированного размера (применяя ограничение `batch_size_cap` и
  `max_url_length`) и повторно вызывают клиент при пустой выдаче.
* Все пагинаторы логируют ключевые события (`no_results`, `page_limit`), что
  используется QC-отчётами.

### 1.3 Парсеры и нормализаторы

* `sources/<source>/parser` содержат чистые функции, возвращающие
  `dict[str, Any]`/`list[dict[str, Any]]`. Они используют helpers из
  `bioetl.utils` и Pandas только для приведения типов.
* Нормализация реализована в `bioetl.normalizers`:
  * `NormalizerRegistry` регистрирует базовые нормализаторы (строки, числа,
    идентификаторы, библиографию).
  * Источники вызывают `registry.normalize("identifier", value)` и другие
    обобщённые операции. Пример: документ-пайплайн формирует поля
    `chembl_title`, `chembl_doi`, приводит идентификаторы к каноническому виду
    и переносит булевые признаки через `coerce_optional_bool`.
* Pandera-схемы расположены в `bioetl/schemas`. Регистрация выполняется через
  `bioetl.schemas.registry`. `DocumentPipeline` и другие пайплайны валидируют
  как «сырые», так и нормализованные фреймы и собирают ошибки через
  `_summarize_schema_errors`.

### 1.4 Координация пайплайна и вывод

* `PipelineBase` централизует чтение входных файлов (`read_input_table`),
  работу с режимом `limit`, учёт `run_id` и регистрацию клиентов.
* Материализация организована через `bioetl.core.output_writer`. Он создаёт
  временные файлы, фиксирует `meta.yaml` (хеши, длительности, версии
  конфигов), поддерживает дополнительный вывод QC и параллельные таблицы.
* `finalize_output_dataset` гарантирует сортировку и порядок колонок согласно
  `determinism.column_order` из конфигурации.

## 2) Контрольные точки и артефакты

* Базовый CSV и отчёт по кластерам: `docs/requirements/PIPELINES.inventory.*`.
* Golden-тесты пайплайнов (pytest + coverage): `artifacts/baselines/golden_tests/`.
* QC отчёты и показатели: формируются в каталоге `data/output/<pipeline>/qc`
  через хелперы `append_qc_sections` и `persist_rejected_inputs`.

## 3) Приёмочные критерии

* Все источники используют `UnifiedAPIClient`/`APIClientFactory`; конфигурация
  ретраев и rate-limit совпадает с YAML.
* Пагинация не содержит ручных «while True» без контроля курсора; источники
  опираются на общие классы из `sources/<source>/pagination` или деление батча
  в клиентах.
* Нормализация проходит через `NormalizerRegistry`, а Pandera схемы
  зарегистрированы и задействованы в тестах.
* Детерминизм выгрузок (порядок, кодировка, хеши) обеспечивается через общий
  writer и проверяется golden-тестами.
