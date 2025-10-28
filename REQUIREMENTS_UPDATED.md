# Updated Requirements Package

## 1. Глоссарий и соглашения
| Термин | Определение | Ссылка |
|---|---|---|
| `chembl_release` | Версия ChEMBL, фиксируемая через `/status`, используется для scope кэша и метаданных | [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11] |
| `run_id` | UUID идентификатор запуска, пробрасывается в логирование, атомарные записи и метаданные | [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/02-io-system.md@test_refactoring_11] |
| `column_order` | Канонический порядок колонок, задаваемый Pandera-схемой и используемый при записи и хешировании | [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/02-io-system.md@test_refactoring_11] |
| `NA-policy` | Политика заполнения пропусков: строки → `""`, числовые поля → `null`, datetime → ISO8601, JSON → canonical dump | [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/02-io-system.md@test_refactoring_11] |
| `UnifiedAPIClient` | Слой устойчивых HTTP-запросов с кэшем, rate limit, backoff, circuit breaker, fallback | [ref: repo:docs/requirements/03-data-extraction.md@test_refactoring_11] |
| `UnifiedOutputWriter` | Подсистема атомарной записи CSV/Parquet, QC, корреляций, meta.yaml, manifest | [ref: repo:docs/requirements/02-io-system.md@test_refactoring_11] |
| `UnifiedSchema` | Реестр нормализаторов и Pandera DataFrameSchema для входных/выходных наборов | [ref: repo:docs/requirements/04-normalization-validation.md@test_refactoring_11] |
| `Fallback record` | Стандартная запись при ошибках внешнего источника, включает error_code/http_status/retry metadata | [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11] |

## 2. Политика детерминизма и качества
1. **Сортировки**: все пайплайны обязаны выполнять сортировку перед записью: Assay (`assay_chembl_id,row_subtype,row_index`), Activity (`activity_id`), Testitem (`molecule_chembl_id`), Target (`target_chembl_id` и вторичные ключи по таблице), Document (`document_chembl_id`). [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
2. **Типизация**: использовать nullable Pandas dtypes (`StringDtype`, `Int64Dtype`, `Float64Dtype`, `BooleanDtype`) без `object`; Pandera схемы помечаются `strict=True`. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/04-normalization-validation.md@test_refactoring_11]
3. **NA-policy**: применять правила канонической сериализации — пустые строки для текстов, `null` для чисел, ISO8601 UTC для дат, JSON `sort_keys=True` для структур; бизнес-хеши завязаны на этой политике. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/02-io-system.md@test_refactoring_11]
4. **Хеши и манифесты**: расчёт `hash_row` и `hash_business_key` выполняется после нормализации, meta.yaml хранит SHA256 артефактов и копирует `column_order` из схемы; manifest формируется в Extended режиме. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/02-io-system.md@test_refactoring_11]
5. **Атомарная запись**: все пайплайны используют `UnifiedOutputWriter` с `os.replace` и run-scoped temp директориями `.tmp_run_{run_id}`; частичные файлы очищаются в `finally`. [ref: repo:docs/requirements/02-io-system.md@test_refactoring_11] [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11]
6. **Sidecars**: стандартный режим — dataset + quality_report; Extended — добавляет correlation_report (по feature flag), meta.yaml и run_manifest. [ref: repo:docs/requirements/02-io-system.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
7. **QC**: каждое требование должно фиксировать нулевую толерантность к дубликатам PK, допустимые пороги пропусков и отчёты по referential integrity; QC отчёты сохраняются рядом с основным датасетом. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]

## 3. Контракты источников данных
### 3.1 ChEMBL Data Web Services
- **Базовый URL**: `https://www.ebi.ac.uk/chembl/api/data` для ресурсов `assay`, `activity`, `molecule`, `target`, `document`. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Пагинация**: использовать `limit` + `offset` или batch `__in` (размер ≤25 ID; при превышении — POST + `X-HTTP-Method-Override: GET`). [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Лимиты**: `batch_size` ≤25 из-за ограничения URL (~2000 символов); `chunk_size` Document по умолчанию 10 с рекурсивным делением. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Ошибки/ретраи**: обработка 429/5xx через экспоненциальный backoff, уважение `Retry-After`, circuit breaker с threshold 5 и timeout 60 секунд, fallback записи с расширенными полями. [ref: repo:docs/requirements/03-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Partial failures**: при сбое батча выполняется recursive split до единичных ID; неуспешные элементы попадают в fallback и QC. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]

### 3.2 PubChem PUG-REST (Testitem enrichment)
- **Endpoints**: CID properties (batch до 100), InChIKey/SMILES/Name lookup, synonyms/xrefs. [ref: repo:docs/requirements/07b-testitem-data-extraction.md@test_refactoring_11]
- **Rate limit**: ≤5 req/sec, retry на 429/5xx с экспоненциальным backoff, сервис outage tracking. [ref: repo:docs/requirements/07b-testitem-data-extraction.md@test_refactoring_11]
- **Graceful degradation**: ошибки PubChem не блокируют пайплайн; поля `pubchem_*` заполняются null. [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11]

### 3.3 UniProt REST (Target enrichment)
- **Режимы**: `idmapping/run`, `status`, `stream`, `search`, `entry`; batch до 10000 ID; polling каждые 5 секунд. [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11]
- **Rate limit**: ≤3 запросов в секунду, backoff base 2s, max 5 повторов, jitter. [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11]
- **Partial failures**: fallback через secondaryAccessions, orthologs, gene symbol merge; все случаи логируются и попадают в QC. [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11]

### 3.4 IUPHAR/GtoPdb (Target classification)
- **Endpoints**: `/targets`, `/targets/families`, fields type/class/subclass; rate limit наследует default HTTP (использовать UnifiedAPIClient). [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11]
- **Fallback**: отсутствие записи → использование ChEMBL protein_classification c пометкой `classification_source="chembl"`. [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11]

### 3.5 PubMed, Crossref, OpenAlex, Semantic Scholar (Document mode=all)
- **PubMed**: EPost/EFetch с batch 200, history API, обязательный identify email; retry/backoff аналогичен ChEMBL. [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Crossref**: cursor pagination, rows≤1000, `mailto` идентификатор. [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **OpenAlex**: cursor-based, per_page≤200, NDJSON stream. [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Semantic Scholar**: ограниченные ответы, возможны `access_denied`; ошибки фиксируются в QC. [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Partial failures**: каждый адаптер независим, объединение по DOI/PMID с приоритетами; отсутствие enrichment не ломает пайплайн. [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]

## 4. Требования к логированию и аудиту
1. **Контекст**: каждый лог содержит `run_id`, `stage`, `actor`, `source`, `generated_at`, а при HTTP-запросах — `endpoint`, `params`, `attempt`, `retry_after`. [ref: repo:docs/requirements/01-logging-system.md@test_refactoring_11]
2. **Безопасность**: RedactSecretsFilter и security_processor обязательны; секреты заменяются на `[REDACTED]`. [ref: repo:docs/requirements/01-logging-system.md@test_refactoring_11]
3. **Формат**: console → text в dev/test, JSON в prod; файловый вывод JSON с ротацией 10×10MB. [ref: repo:docs/requirements/01-logging-system.md@test_refactoring_11]
4. **Структура сообщений**: начало и завершение стадий (`extract`, `transform`, `validate`, `load`) логируются с числовыми метриками (успешные/ошибочные батчи, cache hits). [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11]
5. **Аудит**: QC отчёты и meta.yaml фиксируют run метрики, fallback counts, coverage, duplicates; при нарушении порогов пайплайн завершается с ошибкой. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]

## 5. Пайплайны
### 5.1 Assay Pipeline
- **Input**: CSV/DataFrame с `assay_chembl_id` (обязательно, regex `^CHEMBL\d+$`), опционально `target_chembl_id`; валидация Pandera `AssayInputSchema`. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11]
- **Extract**: batch `assay_chembl_id__in` по 25 ID; cache с ключом `assay:{chembl_release}:{id}`; fallback при circuit breaker/timeout/5xx. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11]
- **Transform**: long-format expand (`row_subtype`, `row_index`), whitelist enrichment для target/assay_class, canonical serialization для hash. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11]
- **Validate/QC**: referential integrity для target/assay_class, QC профиль (`missing_assay_chembl_id`, `duplicate_primary_keys`, `invalid_chembl_id_pattern`, `referential_integrity_loss`). [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11]
- **Load**: `UnifiedOutputWriter`, column order из конфигурации (95+ полей), атомарная запись, meta.yaml с run/git/config/hash. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11]

### 5.2 Activity Pipeline
- **Input**: список `activity_id`/фильтров, маппинг API → CSV (таблица FIELD_MAPPING). [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11]
- **Extract**: batch `_extract_from_chembl` по 25 ID, с cache release scope и recursive split. [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11]
- **Transform**: нормализация стандартных значений, unit normalization, derived поля (`compound_key`, citation flags), BAO поля допускают null. [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11]
- **Validate/QC**: Pandera schema (строгая), проверка `duplicated(activity_id)==0`, QC отчёт с duplicates и validity metrics, UNCERTAIN BAO coverage отмечается для исследований. [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11]
- **Load**: атомарная запись `activity_{date}.csv`, QC, correlation report (по feature flag), meta.yaml/manifest в Extended режиме. [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11]

### 5.3 Testitem Pipeline
- **Input**: CSV/DataFrame с `molecule_chembl_id` (уникально, regex), опционально `nstereo`, `salt_chembl_id`. [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11]
- **Extract**: ChEMBL `/molecule.json` batching ≤25, optional PubChem enrichment (CID resolution workflow, batch properties до 100). [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/07b-testitem-data-extraction.md@test_refactoring_11]
- **Transform**: flatten moleculer structures, hierarchies, properties, synonyms, optional PubChem join по `standard_inchi_key`; strict NA policy. [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11]
- **Validate/QC**: Pandera schema для 95+ полей, QC (`missing_molecule_chembl_id`, `duplicate_primary_keys`, enrichment coverage), fallback/opt-out для PubChem фиксируется в QC. [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11]
- **Load**: сортировка по `molecule_chembl_id`, column_order из конфигурации, hash generation, atomic write, optional correlation report. [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11]

### 5.4 Target Pipeline
- **Input**: CSV/DataFrame с `target_chembl_id`, опциональные фильтры `organism`, `target_type`. [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11]
- **Extract**: ChEMBL (`/target`, `/target_component`, `/protein_classification`, `/target_relation`) с пагинацией; adaptive chunking. [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11]
- **Enrichment**: UniProt ID mapping + stream (≤3 req/sec), isoform expansion, ortholog fallback; IUPHAR classification merge. [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11]
- **Post-process**: merge с приоритетами (`chembl` > `uniprot` > `iuphar` > `ortholog`), derived поля (`component_enrichment`, `merge_rank`, `data_origin`). [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11]
- **Outputs**: `targets.parquet` (PK `target_chembl_id`), `target_components.parquet` (PK `target_chembl_id,component_id`), `protein_class.parquet` (PK `target_chembl_id,class_level,class_name`), `xref.parquet` (PK `target_chembl_id,xref_src_db,xref_id`). [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11]
- **QC**: coverage (UniProt/IUPHAR), fallback counts, duplicates, taxonomy alignment; ortholog usage логируется как WARNING. [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11]

### 5.5 Document Pipeline
- **Modes**: `chembl` (только ChEMBL) и `all` (ChEMBL + PubMed/Crossref/OpenAlex/S2). [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Input**: `document_chembl_id` CSV/DataFrame, Pandera InputSchema (`strict=True`, уникальность). [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Extract**: batch ≤25 ID, recursive split на timeout, respect Retry-After, circuit breaker 5/60. [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Normalize**: DOI, PMID, авторы, журналы; merge field-level с приоритетами (ChEMBL > PubMed > Crossref > OpenAlex > S2). [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Validate/QC**: Pandera raw/normalized схемы, QC (coverage, conflicts, invalid DOI, duplicates by CHEMBL/DOI/PMID, access_denied). [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]
- **Load**: deterministic CSV/Parquet, QC, optional correlation, meta.yaml с coverage metrics, atomic write. [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11]

## 6. Матрица соответствия
| requirement_id | description | verification | evidence |
|---|---|---|---|
| REQ-1 | Сортировки и column_order из единого источника (UnifiedSchema + конфиг) применяются во всех пайплайнах | Проверка Pandera схем и deterministic сортировки перед записью | [ref: repo:docs/requirements/02-io-system.md@test_refactoring_11] [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11] |
| REQ-2 | UnifiedAPIClient применяет rate limit, backoff, circuit breaker, fallback во всех источниках | pytest/integration тесты клиента с симуляцией 429/5xx | [ref: repo:docs/requirements/03-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/07b-testitem-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11] |
| REQ-3 | Pandera Input/Output схемы покрывают ключевые и derived поля для всех пайплайнов | Pandera validation отчёты и schema registry | [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11] |
| REQ-4 | Логи содержат обязательный контекст и безопасны относительно секретов | Лог-тесты и проверка structlog processors | [ref: repo:docs/requirements/01-logging-system.md@test_refactoring_11] |
| REQ-5 | QC отчёты фиксируют coverage, duplicates и fallback для всех пайплайнов | QC regression тесты, анализ CSV sidecars | [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_11] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11] |
| REQ-6 | Document mode=all агрегирует внешние источники с приоритетами и graceful degradation | Интеграционные тесты с моками PubMed/Crossref/OpenAlex/S2 | [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_11] |
| REQ-7 | Target pipeline формирует четыре согласованные таблицы с PK/FK и логикой enrichment | Contract тесты parquet схем и referential integrity | [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_11] |
