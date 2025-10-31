# MODULE_RULES.md

> Базовые правила для кодовой базы `src/bioetl/sources/*` и связанных
> компонентов `core/`, `config/`, `pipelines/`. Документ отражает текущее
> состояние ветки `test_refactoring_32` и используется как эталон.

Нормативные термины MUST/SHOULD/MAY трактуются по RFC 2119/BCP 14.

## 1. Раскладка и именование

### 1.1 Дерево каталога на источник (MUST)

Каждый источник располагается в `src/bioetl/sources/<source>/` и содержит
следующие подпапки:

- `client/` — сетевые вызовы и политики отказоустойчивости.
- `request/` — сборка параметров запроса, headers, etiquette.
- `parser/` — парсинг ответов API, чистые функции без IO.
- `normalizer/` — приведение данных к единой схеме, вызовы реестра
  нормализаторов.
- `schema/` — Pandera-схемы и вспомогательные валидаторы.
- `output/` — материализация, QC-отчёты, meta.yaml.
- `merge/` — политика объединения с внешними источниками (если применимо).
- `pagination/` — стратегия пагинации для API с курсорами/offset.
- `pipeline.py` — координация этапов пайплайна.

Фактическое дерево поддерживается `src/scripts/run_inventory.py` и
публикуется в `docs/requirements/PIPELINES.inventory.csv`.

### 1.2 Именование файлов (MUST)

Имена модулей описательные: `<source>_client.py`, `<source>_parser.py`,
`<source>_normalizer.py`. Экспортируемые символы фиксируются через `__all__`.
Стиль имен — PEP 8: `snake_case` для функций и переменных, `CapWords` для
классов.

### 1.3 Тесты и документация (MUST)

- `tests/sources/<source>/` содержит модульные тесты для клиента, парсера,
  нормализатора и e2e пайплайна.
- Для каждого источника есть README в `docs/requirements/sources/<source>/`
  (API, config_keys, merge_policy, сценарии тестирования, golden-наборы).

### 1.4 Отсутствие побочных эффектов (MUST)

Импорт модулей не должен инициировать HTTP-запросы, запись на диск или менять
глобальное состояние. Допускается подготовка констант и dataclass-объектов.

## 2. Границы слоёв и зависимости (MUST)

| From \ To | core/* | client | request | pagination | parser | normalizer | schema | merge | output | pipeline |
|-----------|--------|--------|---------|------------|--------|------------|--------|-------|--------|----------|
| client    | ✔︎     | —      | —       | —          | —      | —          | —      | —     | —      | —        |
| request   | ✔︎     | ✔︎     | —       | ✔︎         | —      | —          | —      | —     | —      | —        |
| pagination| ✔︎     | —      | —       | —          | —      | —          | —      | —     | —      | —        |
| parser    | ✔︎     | —      | —       | —          | —      | —          | —      | —     | —      | —        |
| normalizer| ✔︎     | —      | —       | —          | ✔︎     | —          | ✔︎     | —     | —      | —        |
| schema    | ✔︎     | —      | —       | —          | —      | —          | —      | —     | —      | —        |
| merge     | ✔︎     | —      | —       | —          | —      | ✔︎         | ✔︎     | —     | —      | —        |
| output    | ✔︎     | —      | —       | —          | —      | —          | ✔︎     | —     | —      | —        |
| pipeline  | ✔︎     | ✔︎     | ✔︎      | ✔︎         | ✔︎     | ✔︎         | ✔︎     | ✔︎    | ✔︎     | —        |

Правила:

- `parser` MUST NOT выполнять IO; только преобразования данных.
- `normalizer` приводит единицы измерения и идентификаторы через
  `NormalizerRegistry` (MUST).
- `schema` содержит только определение схем и helper-валидаторы (MUST).
- `output` обеспечивает детерминизм и атомарную запись (MUST).
- `pipeline.py` агрегирует стадии, не дублируя логику нижних слоёв (MUST).

## 3. Конфигурация

### 3.1 Размещение (MUST)

- Общий конфиг: `src/bioetl/configs/base.yaml`.
- Расширения и include-файлы: `src/bioetl/configs/includes/`.
- Конкретные пайплайны: `src/bioetl/configs/pipelines/<pipeline>.yaml`.
- Профили HTTP (`http.<profile>`) и fallback-опции описываются в YAML и
  валидируются `bioetl.config.models`.

### 3.2 Retry/Backoff и Rate Limit (MUST)

| Профиль | total | backoff_multiplier | backoff_max | statuses | rate_limit |
|---------|:-----:|:------------------:|:-----------:|:--------:|-----------|
| `http.global` (`base.yaml`) | 5 | 2.0 | 120.0 | 408, 425, 429, 500, 502, 503, 504 | 5 calls / 15s |
| `chembl` (`pipelines/target.yaml`) | 5 | 2.0 | 120.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 | 12 calls / 1s |
| `uniprot*` (`target.yaml`) | 4 | 2.0 | 90.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 | 2–3 calls / 1s |
| `iuphar` (`target.yaml`) | 4 | 2.0 | 60.0 | 404, 408, 409, 425, 429, 500, 502, 503, 504 | 6 calls / 1s |

Рекомендации:

- Документация и конфиги должны оставаться синхронизированными. При изменении
  YAML обновляйте таблицу выше и README источника.
- Если явный профиль не указан, `APIClientFactory` использует значения по
  умолчанию (`total=3`, `backoff_max=60`, `statuses=[429, 500, 502, 503, 504]`,
  `rate_limit.max_calls=1`, `period=1.0`). Такие случаи необходимо явно
  документировать и, по возможности, добавлять профиль.
- `FallbackOptions.partial_retry_max` по умолчанию равен 3 и наследуется
  источниками; `target` переопределяет до 2. Значения должны совпадать между
  конфигом и кодом.

### 3.3 Дополнительные требования

- Параметры etiquette (например, `mailto` для Crossref) задаются в YAML и
  обрабатываются сборщиками запросов (MUST).
- Секреты читаются из окружения через конструкции `${ENV_NAME}` и валидируются
  `TargetSourceConfig` (MUST).
- Материализация datasets управляется `materialization.*` и должна иметь
  явные имена файлов и форматы.

## 4. Детерминизм и хеши (MUST)

- Столбцы сортируются в порядке `determinism.column_order` до записи.
- CSV записываются с `lineterminator="\n"`, UTF-8, заданным `delimiter` и
  `quoting`.
- Пустые значения представлены единообразно (`""` или `null`).
- Хеши вычисляются алгоритмом BLAKE2 (`hash_row`, `hash_business_key`).
- Запись выполняется через временный файл с `fsync` перед заменой.
- `meta.yaml` фиксирует размеры, хеши, версии кода/конфигов, длительность
  стадий и сведения о пагинации.

## 5. Тестирование

- Unit-тесты покрывают `client`, `parser`, `normalizer`, `schema`.
- `tests/pipelines` содержит e2e сценарии с golden-файлами.
- QC-пороги проверяются в `tests/integration/qc`.
- Property-based тесты (Hypothesis) покрывают пагинацию и нормализацию там,
  где есть нестандартные правила.

## 6. MergePolicy

- Ключи объединения MUST быть задокументированы в `merge/policy.py` (например,
  `doi`, `pmid`, `molecule_chembl_id`).
- Стратегии разрешения конфликтов (`prefer_source`, `prefer_fresh`,
  `concat_unique`, `score_based`) фиксируются в артефактах.
- Объединение происходит после успешной валидации обеих сторон.

## 7. Логирование и наблюдаемость

- `bioetl.core.logger.UnifiedLogger` используется для всех модулей.
- Обязательные поля контекста: `run_id`, `stage`, `source`, `timestamp`.
- Логи проходят через фильтры `RedactSecretsFilter` и `SafeFormattingFilter`.
- Форматы: text (development/testing) и JSON (production, файлы, ротация
  10 MB × 10).
- Секреты (`api_key`, `authorization`, и т.д.) редактируются автоматически.

## 8. HTTP-запросы, rate-limit и retry

- Все HTTP запросы идут через `UnifiedAPIClient`.
- `TokenBucketLimiter` обеспечивает соблюдение rate-limit и логирует ожидания
  >1 секунды.
- `CircuitBreaker` переводится в `half-open` после `cb_timeout` и закрывается
  при успешной попытке.
- `RetryPolicy` учитывает `Retry-After` (date/seconds) и прекращает попытки на
  4xx (кроме 429) или когда достигнут `total`.
- `partial_retry` повторяет полный запрос (payload не изменяется) и
  ограничивается `partial_retry_max`.
- Fallback стратегии: `cache` (возврат данных из TTLCache), `partial_retry`,
  а также стратегии из `FallbackManager` (`network`, `timeout`, `5xx`).

## 9. Документация

- Каждое изменение публичного API источника сопровождается обновлением README
  и соответствующего раздела в `docs/requirements`.
- Отчёты инвентаризации и кластеры поддерживаются актуальными (см. раздел 3
  REFACTOR_PLAN).

## 10. Ошибки и исключения

- Используются типы: `NetworkError`, `RateLimitError`, `ParsingError`,
  `NormalizationError`, `ValidationError`, `WriteError`.
- `PipelineBase.run()` возвращает структурированную сводку. При фатальной
  ошибке артефакты не публикуются.

## 11. Совместимость и версии

- Семантическое версионирование: MINOR для совместимых изменений, MAJOR для
  ломающих.
- Депрекации документируются в `DEPRECATIONS.md` и выдерживаются минимум два
  MINOR-релиза.

## 12. Безопасность и секреты

- Секреты не хранятся в репозитории. Значения считываются из окружения или
  секрет-хранилища.
- Логи и `meta.yaml` не содержат секретов/PII.

## 13. Производительность и параллелизм

- Параллельность ограничивается слоем `client/` и конфигурацией источника.
- При параллельной загрузке сохраняется детерминизм (последующая сортировка).

## 14. Сериализация форматов

- CSV: единый диалект, явные `delimiter`, `quotechar`, `lineterminator`.
- JSON: сортировка ключей, запрет NaN/Infinity (преобразование к строкам или
  null согласно правилу источника).
- Даты/время — только RFC 3339 (UTC).

## 15. Общие компоненты core/

### 15.1 UnifiedLogger (`core/logger.py`)

- Использует `structlog` с процессорами `add_utc_timestamp`, `add_context` и
  `security_processor`.
- Поддерживает запись в файл с ротацией и консольный вывод.
- Контекст хранится в `ContextVar` и объединяется через
  `structlog.contextvars.merge_contextvars`.

### 15.2 UnifiedAPIClient (`core/api_client.py`)

- `requests.Session` с переиспользованием соединений.
- `TokenBucketLimiter` реализует rate-limit и jitter.
- `RetryPolicy` обрабатывает `RequestException`, следит за `Retry-After` и
  логирует попытки через `_RequestRetryContext`.
- `CircuitBreaker` защищает от каскадных ошибок, состояния `closed`, `open`,
  `half-open`.
- TTLCache активируется, если `cache_enabled=True`.
- Fallback стратегии выполняются в порядке, указанном в конфиге: `cache`,
  `partial_retry`, затем стратегии менеджера (`network`, `timeout`, `5xx`).
  `FallbackManager` подключён и возвращает детерминированные заглушки для
  поддерживаемых ошибок.
- `_fallback_partial_retry` повторяет запрос без изменения параметров, число
  попыток ограничено `partial_retry_max`.
- `_execute` повторно получает токен rate-limiter после `Retry-After` и
  выполняет повторный запрос.

