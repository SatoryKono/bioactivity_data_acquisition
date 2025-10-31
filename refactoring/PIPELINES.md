Единый принцип: один внешний источник данных соответствует одному публичному пайплайну с минимальным набором модулей и стабильным контрактом. Все пути и ссылки указываются на ветку @test_refactoring_32.

> **Примечание:** Структура `src/bioetl/sources/` — правильная организация для внешних источников данных. Внешние источники (crossref, pubmed, openalex, semantic_scholar, iuphar, uniprot) имеют правильную структуру с подпапками (client/, request/, pagination/, parser/, normalizer/, schema/, merge/, output/, pipeline.py). Для ChEMBL существует дублирование между `src/bioetl/pipelines/` (монолитные файлы) и `src/bioetl/sources/chembl/` (прокси).

## Источники истины (@test_refactoring_32)

- [ref: repo:docs/requirements/PIPELINES.inventory.csv@test_refactoring_32] — детерминированный CSV-слепок пайплайнов.
- [ref: repo:docs/requirements/PIPELINES.inventory.clusters.md@test_refactoring_32] — кластерный отчёт по составу пайплайнов.
- [ref: repo:configs/inventory.yaml@test_refactoring_32] — конфигурация генератора инвентаризации.
- [ref: repo:src/scripts/run_inventory.py@test_refactoring_32] — CLI для генерации и проверки артефактов.
- [ref: repo:tests/unit/test_inventory.py@test_refactoring_32] — тесты, защищающие инвентаризацию.

## 1) Контракт и жизненный цикл (@test_refactoring_32)

### Базовый интерфейс: (@test_refactoring_32)
**PipelineBase:**
- `extract() -> pd.DataFrame`
- `transform(df: pd.DataFrame) -> pd.DataFrame`
- `validate(df: pd.DataFrame) -> pd.DataFrame`
- `export(df: pd.DataFrame, output_path: Path, extended: bool = False) -> OutputArtifacts`
- `run(output_path: Path, extended: bool = False, *args, **kwargs) -> OutputArtifacts`

[ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]

**Нормативный язык требований:** во всём документе используется RFC 2119/BCP 14 (MUST/SHOULD/MAY в верхнем регистре). Это фиксирует проверяемые обязательства и исключает двусмысленности интерпретации требований. [datatracker.ietf.org @test_refactoring_32](https://datatracker.ietf.org)

### Семантика и инварианты шагов (@test_refactoring_32)

#### extract (@test_refactoring_32)
**Назначение:** сетевое извлечение сырья из API источника с контролем отказов и лимитов.

**Требования:**
- Выходной контракт — `pd.DataFrame` с именованными колонками, совместимый со Schema Registry и `PipelineBase.run()`. [ref: repo:refactoring/IO.md@test_refactoring_32]
- Политики retry/backoff и обработка HTTP 429/5xx обязательны; Retry-After учитывается при наличии.
- Пагинация инкапсулируется в стратегиях: Page/Size, Cursor, Offset/Limit, Token.
- Запросы маркируются request_id; на выход добавляются метаданные: request_id, page|cursor, retry_count, elapsed_ms, status.
- Для источников, где это явно требуется или повышает квоту, в запрос добавляется идентификатор клиента (например, mailto=<email>): Crossref рекомендует mailto, OpenAlex переводит клиента в «polite pool» с более высокими лимитами. [www.crossref.org @test_refactoring_32](https://www.crossref.org), [OpenAlex @test_refactoring_32](https://docs.openalex.org)

**Специфика по API (минимум):**
- **Crossref:** REST, фильтры/фасеты; следовать «API etiquette» и указывать контакт. [www.crossref.org @test_refactoring_32](https://www.crossref.org)
- **NCBI E-utilities (PubMed):** фиксированный URL-синтаксис; пайплайн должен «склеивать» esearch → (post) → efetch для батч-выборки. [ncbi.nlm.nih.gov @test_refactoring_32](https://www.ncbi.nlm.nih.gov/books/NBK25497/), [nlm.nih.gov @test_refactoring_32](https://www.nlm.nih.gov/bsd/mms/medlineelements.html)
- **OpenAlex:** дневной лимит и «polite pool» при указании email, курсорная пагинация. [OpenAlex @test_refactoring_32](https://docs.openalex.org)
- **Semantic Scholar:** REST к авторам/публикациям/цитированиям, выбор полей через fields. [Semantic Scholar API @test_refactoring_32](https://api.semanticscholar.org/api-docs/graph)
- **UniProt:** программный REST-доступ к UniProtKB. [UniProt @test_refactoring_32](https://www.uniprot.org/help/api)
- **PubChem (PUG REST/PUG View):** REST-интерфейсы, множество пространств идентификаторов; использовать официальные эндпоинты и руководства. [PubChem @test_refactoring_32](https://pubchem.ncbi.nlm.nih.gov/docs/pug-rest), [PubChem @test_refactoring_32](https://pubchem.ncbi.nlm.nih.gov/docs/pug-view)
- **IUPHAR/BPS GtoP:** REST-сервисы, JSON-ответы. [Guide to Pharmacology @test_refactoring_32](https://www.guidetopharmacology.org/DATA/)

#### transform (@test_refactoring_32)
**Назначение:** приведение сырых датафреймов к UnifiedSchema.

**Требования:**
- Стандартизация идентификаторов (doi, pmid, cid, uniprot_id, molecule_chembl_id, и т.п.).
- Нормализация единиц, дат, имен авторов/аффилиаций, ссылок на полнотексты.
- Семантические маппинги (например, онтологии) фиксируются явными таблицами соответствий.
- Поля, не попадающие в контракт, переносятся в extras без потери информации; порядок ключей в итоговом объекте стабилен.
- Результат шага — `pd.DataFrame`, готовый для строгой валидации Pandera и экспорта. [ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]

#### validate (@test_refactoring_32)
**Назначение:** раннее обнаружение отклонений от контракта.

**Требования:**
- Жёсткая проверка типов, диапазонов, обязательных полей и категориальных множества через Pandera; ошибки блокируют прохождение шага.
- Схемы регистрируются централизованно и переиспользуются; допускаются backend-совместимые датафреймы. [pandera.readthedocs.io @test_refactoring_32](https://pandera.readthedocs.io)

#### export (@test_refactoring_32)
**Назначение:** детерминированная фиксация вывода.

**Требования:**
- Фиксированный column_order, стабильная сортировка по бизнес-ключам, форматы чисел/дат и сериализация строк без неоднозначности.
- Контрольные хеши на строку и бизнес-ключ (например, BLAKE2) включаются в метаданные.
- Запись выполняется атомарно: временный файл на той же ФС, затем атомарная замена (replace/move_atomic); синхронизация буферов перед коммитом. [python-atomicwrites.readthedocs.io @test_refactoring_32](https://python-atomicwrites.readthedocs.io)
- Экспорт возвращает `OutputArtifacts`, включающий ссылки на основной датасет, QC-артефакты и meta.yaml. [ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]

#### run (@test_refactoring_32)
Оркестрация extract → transform → validate → export. Возвращает агрегированную сводку: числа вход/выход, ошибки валидации, контрольные суммы, путь и размеры артефактов; эта сводка пишется в meta.yaml.

## 2) Минимальный состав модулей на один источник (MUST) (@test_refactoring_32)

- **client/** — HTTP-клиент с ретраями, экспоненциальным бэкоффом, распознаванием Retry-After, ограничением RPS; запреты на сетевые вызовы вне этого слоя.
- **request/** — билдер URL/параметров/заголовков; единая точка для mailto/User-Agent, фильтров и полей. Для Crossref/OpenAlex наличие mailto контролируется здесь. [www.crossref.org @test_refactoring_32](https://www.crossref.org)
- **pagination/** — стратегии: PageNumber, Cursor, OffsetLimit, Token; инварианты порядка страниц и дедупликация.
- **parser/** — чистые функции разбора; никаких побочных эффектов и IO.
- **normalizer/** — единицы, онтологии, ID-маппинги; любое «угадывание» форматов запрещено, допускается только явная логика и таблицы соответствий.
- **schema/** — Pandera-схемы и helper-валидаторы; никакой трансформации данных. [pandera.readthedocs.io @test_refactoring_32](https://pandera.readthedocs.io)
- **merge/** — MergePolicy с явными ключами слияния, стратегиями конфликтов (prefer_source, prefer_fresh, concat_unique).
- **output/** — детерминизм, атомарная запись, контрольные хеши, meta.yaml. [python-atomicwrites.readthedocs.io @test_refactoring_32](https://python-atomicwrites.readthedocs.io)
- **pipeline.py** — реализация PipelineBase, CLI-вход: `python -m bioetl.sources.<source>.pipeline --config ....`

Конфигурация пайплайна описывается файлом `src/bioetl/configs/pipelines/<source>.yaml` (MUST); допускаются include-блоки из `src/bioetl/configs/includes/`.

**Публичный API (MUST):**
```python
from bioetl.sources.<source>.pipeline import <Source>Pipeline
```
CLI: `python -m bioetl.sources.<source>.pipeline --config ...`

## 3) Детерминизм и идемпотентность (@test_refactoring_32)

**Детерминизм:** одинаковые входы дают бит-идентичный вывод; фиксируются: сортировка по бизнес-ключам, порядок столбцов, правила сериализации и форматы, конфигурация CSV/Parquet, а также хеши. Это устраняет ложные diffs в VCS и стабилизирует golden-снимки.

**Идемпотентность:** повторный запуск с теми же входами и конфигом не меняет артефактов и meta.yaml. Это критично для отката и безопасного повторного прогона.

**Атомарные записи:** запись во временный файл и замена целевого файлом в одной файловой системе; гарантируется библиотекой atomicwrites, где commit() выполняет replace_atomic/move_atomic с предварительным fsync. [python-atomicwrites.readthedocs.io @test_refactoring_32](https://python-atomicwrites.readthedocs.io)

## 4) Пагинация, лимиты и «этикет» API (@test_refactoring_32)

Пайплайн обязан официально «идентифицироваться» там, где это предписано или улучшает квоту:

- **Crossref:** включать mailto и корректный User-Agent; следовать «Tips for using the REST API». [www.crossref.org @test_refactoring_32](https://www.crossref.org)
- **OpenAlex:** добавлять mailto для попадания в «polite pool» и более стабильных лимитов. [OpenAlex @test_refactoring_32](https://docs.openalex.org)
- **NCBI E-utilities:** соблюдать фиксированный синтаксис URL и сценарии ESearch/EFetch/ESummary; при массовом извлечении использовать батчи. [ncbi.nlm.nih.gov @test_refactoring_32](https://www.ncbi.nlm.nih.gov/books/NBK25497/)
- Остальные источники — использовать их официальные REST-доки, выбирать поддерживаемые поля и модели пагинации (см. ссылки): Semantic Scholar, UniProt, PubChem, IUPHAR/BPS GtoP. [Guide to Pharmacology @test_refactoring_32](https://www.guidetopharmacology.org/DATA/), [Semantic Scholar API @test_refactoring_32](https://api.semanticscholar.org/api-docs/graph), [UniProt @test_refactoring_32](https://www.uniprot.org/help/api)

## 5) Конфигурация и валидация конфигов (@test_refactoring_32)

Конфиг каждого источника: `src/bioetl/configs/pipelines/<source>.yaml` (MUST). Общие блоки допускается выносить в include-модули из `src/bioetl/configs/includes/`, например `_shared/chembl_source.yaml`, чтобы исключить дублирование параметров. Итоговый YAML автоматически валидируется через
`PipelineConfig`; несоответствие схеме немедленно завершает запуск с ошибкой (MUST NOT продолжать работу).

Обязательные ключи: сетевые таймауты/повторы/лимиты, параметры пагинации, поле идентификации клиента (где требуется), фильтры/поля.

Для Crossref/OpenAlex в конфиге хранится mailto; билдер запросов обеспечивает его присутствие. [www.crossref.org](https://www.crossref.org)
Валидация конфигов строго типизирована; несовместимые ключи/значения вызывают ошибку запуска (MUST NOT продолжать работу).

Для Crossref/OpenAlex в конфиге хранится mailto; билдер запросов обеспечивает его присутствие. [www.crossref.org @test_refactoring_32](https://www.crossref.org)

## 6) Наблюдаемость и диагностика (@test_refactoring_32)

- **Структурные логи:** source, request_id, page|cursor, status_code, retry, elapsed_ms, rows_in/out. Уровень логирования задаётся per-source в конфиге.
- **Метаданные экспорта:** meta.yaml фиксирует: количество записей, контрольные суммы, версии кода/конфигов, длительности шагов, дату/время, хеш бизнес-ключей.
- **Golden-снимки:** e2e-тесты сравнивают нормализованный вывод с эталоном; различия должны быть осмысленными и детерминированными.
- **Трассировка:** допускается корреляция request_id через все шаги.

## 7) Иерархия каталогов (MUST) (@test_refactoring_32)

Структура `src/bioetl/sources/<source>/` — **нормативное требование (MUST)** согласно `MODULE_RULES.md`. Референс-макет:

```
src/bioetl/sources/<source>/
 client/http_client.py
 request/builder.py
 pagination/strategy.py
 parser/<source>_parser.py
 normalizer/<source>_normalizer.py
 schema/<source>_schema.py
 merge/policy.py
 output/writer.py
 pipeline.py
src/bioetl/configs/
 pipelines/
  <source>.yaml
 includes/
  _shared_blocks.yaml
tests/sources/<source>/
 test_client.py
 test_parser.py
 test_normalizer.py
 test_schema.py
 test_pipeline_e2e.py
docs/requirements/sources/<source>/
 README.md
```

Дополнительно для общих компонентов:
```
src/bioetl/core/
 http/...
 pagination/...
 schema_registry.py
 unified_schema.py
 output/writer.py
 logging/logger.py
```

[ref: repo:src/bioetl/core/output/writer.py@test_refactoring_32]
[ref: repo:src/bioetl/core/pagination/strategy.py@test_refactoring_32]
[ref: repo:src/bioetl/core/schema_registry.py@test_refactoring_32]

## 8) Инварианты (MUST) (@test_refactoring_32)

- Любая сетевая активность выполняется только из client/.
- parser/ и normalizer/ не выполняют IO и не читают конфигов напрямую.
- schema/ содержит только спецификации/валидаторы; изменения данных в нём запрещены. [pandera.readthedocs.io @test_refactoring_32](https://pandera.readthedocs.io)
- output/ полностью отвечает за детерминизм, атомарную запись и meta.yaml. [python-atomicwrites.readthedocs.io @test_refactoring_32](https://python-atomicwrites.readthedocs.io)
- Публичный API стабильный; устаревшие символы реэкспортируются и удаляются через два минорных релиза (см. DEPRECATIONS.md).
- Любое поведение, влияющее на квоты/этикет API (например, mailto), фиксируется в request/ и тестируется контрактно. [www.crossref.org @test_refactoring_32](https://www.crossref.org)

## 9) Качество данных, слияния и совместимость (@test_refactoring_32)

- UnifiedSchema задаёт минимальные обязательные поля; источники с неполными данными должны явно документировать деградацию (например, «авторы без ORCID»).
- MergePolicy определяет ключи объединения (doi, pmid, cid, и т.п.) и стратегию разрешения конфликтов; слияние выполняется после валидации обеих сторон.
- Неоднозначные атрибуты, специфичные для источника, не теряются: они попадают в extras и отражаются в документации источника.

### Приоритезация, слияние и разрешение конфликтов (@test_refactoring_32)

**Документы (`documents`):**
- `doi`, `title`, `container_title`/`journal`, `published_(print|online)_date`: Crossref > PubMed > OpenAlex > ChEMBL.
- `authors`: PubMed > Crossref; дедупликация по (`surname`, `initials`); порядок как в источнике с наибольшей полнотой.
- `year`: из приоритетной даты публикации; при расхождении берётся год от источника, предоставившего `doi`.
- Политика отказа: источник понижается, если `doi` некорректен или отсутствует минимальный набор (`title` и `date|year`).

**Таргеты (`targets`):**
- Номенклатура: `name`, `gene_symbol`, `organism` из UniProt; при отсутствии — из ChEMBL.
- Классификация/семейство: IUPHAR > ChEMBL.
- Отказ: если `uniprot_accession` некорректен, унификация сводится к ChEMBL, поле помечается как «требует уточнения».

**Ассайы (`assays`):**
- `assay_type`/`category`/`format`: соответствие BAO; при конфликте BAO-карта перекрывает `assay_type` из сырых данных ChEMBL.
- Привязки к `document_chembl_id` и `target_chembl_id` обязательны, если указаны в ChEMBL.

**Тест-айтемы (`testitems`):**
- Имена/синонимы: PubChem > ChEMBL.
- `salt` и `parent`-связи: пересечение карт соответствий; при конфликте отбрасывается источник без валидационного признака (например, отсутствие подтверждения `parent`).

**Активности (`activities`):**
- `standard_type`, `standard_units`, `standard_value`: выбирается запись с корректной единицей, требующей минимального преобразования к целевым; если есть `pchembl_value` и валидные исходные параметры, он сохраняется.
- При равной уверенности tie-breaker по полноте обязательных полей и свежести релиза источника (см. SRC-04).


## 10) Отказоустойчивость и предсказуемость (@test_refactoring_32)

- Границы ретраев/бэкоффа фиксируются в конфиге источника; превышение лимитов переводит пайплайн в явный FAIL с диагностикой (MUST).
- Ошибки делятся на сетевые, парсинга, нормализации, валидации и записи; в RunResult отражается категория и контекст.
- Частичные повторные прогоны допустимы только если не нарушают инвариант идемпотентности и детерминизма.


## 11) UnifiedLogger: обязательные поля и режимы (@test_refactoring_32)

**Обязательные поля контекста (MUST):**

Все логи должны содержать минимальный набор полей для трассируемости:

| Поле | Обязательность | Описание |
|------|----------------|----------|
| `run_id` | Всегда | UUID идентификатор запуска пайплайна |
| `stage` | Всегда | Текущий этап (extract, transform, validate, export) |
| `actor` | Всегда | Инициатор (system, scheduler, username) |
| `source` | Всегда | Источник данных (chembl, pubmed, и т.п.) |
| `generated_at` | Всегда | UTC timestamp ISO8601 |

**Для HTTP-запросов дополнительно обязательны:**
- `endpoint`: URL эндпоинта
- `attempt`: номер попытки повтора
- `duration_ms`: длительность операции
- `params`: параметры запроса (если есть)
- `retry_after`: планируемая пауза (сек) при 429

**Режимы работы:**
- **Development**: text формат, DEBUG уровень, telemetry off
- **Production**: JSON формат, INFO уровень, telemetry on, rotation
- **Testing**: text формат, WARNING уровень, telemetry off

**Безопасность:**
- Автоматическое редактирование секретов (api_key, token, password и т.п.)
- Фильтры на уровне structlog и standard logging

📄 **Полное описание**: [docs/requirements/01-logging-system.md @test_refactoring_32](../docs/requirements/01-logging-system.md)


## 12) UnifiedAPIClient: компоненты отказоустойчивости (@test_refactoring_32)

**Архитектура компонентов:**

UnifiedAPIClient объединяет следующие слои защиты:

1. **CircuitBreaker** — защита от каскадных ошибок
  - Состояния: closed, open, half-open
  - Порог сбоев и таймаут восстановления настраиваются

2. **TokenBucketLimiter** — rate limiting с jitter
  - Token bucket алгоритм с периодическим пополнением
  - Опциональный jitter для предотвращения thundering herd

3. **RetryPolicy** — экспоненциальный backoff
  - Учёт Retry-After заголовка для HTTP 429
  - Fail-fast на 4xx (кроме 429)
  - Giveup условия для невосстановимых ошибок

4. **FallbackManager** — стратегии отката
  - Обработка network errors, timeouts, 5xx
  - Использование кэшированных или fallback данных

5. **TTL-кэш** (опционально)
  - Run-scoped и release-scoped кэширование
  - Автоматическая инвалидация при смене версии источника

6. **PaginationHandler** — унифицированные стратегии пагинации
  - Page/Size, Cursor, Offset/Limit
  - Автоматическая обработка PartialFailure через requeue

**Протокол для HTTP 429:**
```python
if response.status_code == 429:
  retry_after = response.headers.get('Retry-After')
  if retry_after:
    wait = min(int(retry_after), 60) # Cap at 60s
    time.sleep(wait)
  raise RateLimitError("Rate limited")
```

**Политика ретраев:**
- 2xx, 3xx: успех, возвращаем response
- 429: respect Retry-After, ретраить
- 4xx (кроме 429): не ретраить, fail-fast
- 5xx: exponential backoff, retry

📄 **Полное описание**: [docs/requirements/03-data-extraction.md @test_refactoring_32](../docs/requirements/03-data-extraction.md)


## 13) Архитектурные компоненты core/ (@test_refactoring_32)

**Важно:** Все источники (`src/bioetl/sources/<source>/`) используют унифицированные компоненты из `src/bioetl/core/`. Архитектура следует принципам из 99-final-tech-spec.md:

- **Архитектурные принципы**: композиция над наследованием, детерминизм, безопасность и расширяемость определяют построение всех подсистем (UnifiedLogger, UnifiedOutputWriter, UnifiedAPIClient, UnifiedSchema).
- **Компонентное взаимодействие**: пайплайн использует последовательную цепочку логирования → HTTP-клиенты → нормализаторы/схемы → детерминированный вывод.

Все источники должны сосуществовать с сущностными пайплайнами (documents, targets, assays, testitems, activities) через общие компоненты core/.


## 14) UnifiedOutputWriter: режимы и метаданные (@test_refactoring_32)

**Режимы работы:**

**Standard (2 файла, без correlation по умолчанию):**
- `dataset.csv`, `quality_report.csv`
- Correlation отчёт **только** при явном `postprocess.correlation.enabled: true`

**Extended (+ metadata и manifest):**
- Добавляет `meta.yaml`, `run_manifest.json`
- Полные метаданные: lineage, checksums, git_commit

**Инварианты детерминизма:**
- Checksums стабильны при одинаковом вводе (SHA256)
- Порядок строк фиксирован (deterministic sort)
- Column order **только** из Schema Registry
- NA-policy: `""` для строк, `null` для чисел
- Каноническая сериализация (JSON+ISO8601, float=%.6f)

**Атомарная запись:**

Запись выполняется через run-scoped временные директории с `os.replace()`:

1. Запись во временный файл в `.tmp_run_{run_id}/`
2. Валидация checksums (опционально)
3. Атомарный `os.replace()` (Windows-compatible)
4. Guaranteed cleanup при любой ошибке

**Метаданные в meta.yaml:**
- `run_id`, `pipeline_version`, `config_hash`
- `row_count`, `column_count`, `column_order`
- `checksums`: dataset, quality, correlation (если включен)
- `lineage`: source_files, transformations
- `git_commit` (в production)

**Запрет частичных артефактов:**
- CSV с неполными данными недопустимы
- `meta.yaml` без checksums или lineage недопустимы
- Пустые файлы (размер = 0) недопустимы

📄 **Полное описание**: [docs/requirements/02-io-system.md @test_refactoring_32](../docs/requirements/02-io-system.md)


## 15) Ссылки на детальные спецификации (@test_refactoring_32)

Все компоненты имеют детальные спецификации в `docs/requirements/`:

| Документ | Описание |
|----------|----------|
| [00-architecture-overview.md @test_refactoring_32](../docs/requirements/00-architecture-overview.md) | Архитектурные принципы и обзор компонентов |
| [01-logging-system.md @test_refactoring_32](../docs/requirements/01-logging-system.md) | UnifiedLogger: структура, контекст, режимы |
| [02-io-system.md @test_refactoring_32](../docs/requirements/02-io-system.md) | UnifiedOutputWriter: атомарная запись, QC, метаданные |
| [03-data-extraction.md @test_refactoring_32](../docs/requirements/03-data-extraction.md) | UnifiedAPIClient: отказоустойчивость, пагинация |
| [04-normalization-validation.md @test_refactoring_32](../docs/requirements/04-normalization-validation.md) | UnifiedSchema: нормализаторы, Pandera схемы |
| [05-assay-extraction.md @test_refactoring_32](../docs/requirements/05-assay-extraction.md) | Assay pipeline спецификация |
| [06-activity-data-extraction.md @test_refactoring_32](../docs/requirements/06-activity-data-extraction.md) | Activity pipeline спецификация |
| [07a-testitem-extraction.md @test_refactoring_32](../docs/requirements/07a-testitem-extraction.md) | Testitem extraction спецификация |
| [07b-testitem-data-extraction.md @test_refactoring_32](../docs/requirements/07b-testitem-data-extraction.md) | Testitem data extraction спецификация |
| [08-target-data-extraction.md @test_refactoring_32](../docs/requirements/08-target-data-extraction.md) | Target pipeline спецификация |
| [09-document-chembl-extraction.md @test_refactoring_32](../docs/requirements/09-document-chembl-extraction.md) | Document pipeline спецификация |
| [10-configuration.md @test_refactoring_32](../docs/requirements/10-configuration.md) | Конфигурация: YAML, Pydantic, CLI |
| [99-data-sources-and-data-spec.md @test_refactoring_32](../docs/requirements/99-data-sources-and-data-spec.md) | Спецификация источников данных и требований |
| [99-final-tech-spec.md @test_refactoring_32](../docs/requirements/99-final-tech-spec.md) | Итоговая техническая спецификация |

## Ссылки на первичные источники (минимальный список) (@test_refactoring_32)

- **RFC 2119/BCP 14:** ключевые слова требований. [datatracker.ietf.org @test_refactoring_32](https://datatracker.ietf.org/doc/html/rfc2119), [IETF @test_refactoring_32](https://www.ietf.org/rfc/rfc2119.txt)
- **Pandera:** схемы и валидация датафреймов (включая поддержку нескольких бэкендов). [pandera.readthedocs.io @test_refactoring_32](https://pandera.readthedocs.io)
- **Atomic writes:** документация и механика replace_atomic/move_atomic и fsync. [python-atomicwrites.readthedocs.io @test_refactoring_32](https://python-atomicwrites.readthedocs.io)
- **Crossref:** mailto и «API etiquette». [www.crossref.org @test_refactoring_32](https://www.crossref.org)
- **OpenAlex:** лимиты, «polite pool», mailto. [OpenAlex @test_refactoring_32](https://docs.openalex.org)
- **NCBI E-utilities:** параметры и сценарии ESearch/EFetch. [ncbi.nlm.nih.gov @test_refactoring_32](https://www.ncbi.nlm.nih.gov/books/NBK25497/)
- **Semantic Scholar API.** [Semantic Scholar API @test_refactoring_32](https://api.semanticscholar.org/api-docs/graph)
- **UniProt REST.** [UniProt @test_refactoring_32](https://www.uniprot.org/help/api)
- **PubChem PUG REST/PUG View.** [PubChem @test_refactoring_32](https://pubchem.ncbi.nlm.nih.gov/docs/pug-rest)
- **IUPHAR/BPS GtoP web services.** [Guide to Pharmacology @test_refactoring_32](https://www.guidetopharmacology.org/DATA/)
