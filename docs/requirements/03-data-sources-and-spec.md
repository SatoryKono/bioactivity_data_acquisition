# Источники данных и спецификация {#data-sources-and-spec}

## Сводка источников {#sources-summary}
| Источник | Конечная точка | Пайплайны | Пагинация/батчи | Лимиты и ретраи | Конфигурация |
| --- | --- | --- | --- | --- | --- |
| ChEMBL REST | `/activity.json`, `/assay.json`, `/target.json`, `/molecule.json` | Activity, Assay, Target, TestItem | Батчи по ID, split по `max_url_length` | Batch size 20 (Activity/TestItem), 50 (Target), 10 (Document); backoff через `UnifiedAPIClient` | [`includes/chembl_source.yaml`][ref: repo:src/bioetl/configs/includes/chembl_source.yaml@test_refactoring_32] |
| PubMed E-utilities | `efetch.fcgi`, `esummary.fcgi` | Document | Page-пагинация с `retstart` | Rate limit 3 rps без API key, 10 rps с ключом | [`pipelines/document.yaml`][ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32] |
| Crossref Works | `/works` | Document | Cursor-пагинация | Rate limit 2 rps | [`pipelines/document.yaml`][ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32] |
| OpenAlex Works | `/works` | Document | Cursor-пагинация | Rate limit 10 rps | [`pipelines/document.yaml`][ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32] |
| Semantic Scholar Graph | `/paper/search` | Document | Cursor-пагинация | Rate limit 1 rps (10 с API key) | [`pipelines/document.yaml`][ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32] |
| PubChem PUG REST | `/pug_view/data/compound/` | PubChem, TestItem | Paging по CID и чанки | Retry через backoff, rate limit 5 rps | [`pipelines/pubchem.yaml`][ref: repo:src/bioetl/configs/pipelines/pubchem.yaml@test_refactoring_32] |
| UniProt REST | `/uniprotkb/search` | Target, UniProt | Cursor-пагинация | Rate limit 15 rps | [`pipelines/uniprot.yaml`][ref: repo:src/bioetl/configs/pipelines/uniprot.yaml@test_refactoring_32] |
| Guide to Pharmacology | `/targets` | Target, GtP IUPHAR | Offset-пагинация | API key required, rate limit 60/min | [`pipelines/iuphar.yaml`][ref: repo:src/bioetl/configs/pipelines/iuphar.yaml@test_refactoring_32] |

## ChEMBL-ориентированные пайплайны {#chembl-pipelines}
### Activity {#source-activity}
- Конечная точка: `/activity.json`, формируется через
  [`ActivityRequestBuilder.build_url`][ref: repo:src/bioetl/sources/chembl/activity/request/activity_request.py@test_refactoring_32].
- Батчи: максимум 20 ID, дополнительные split по `max_url_length=2000`.
- Бизнес-ключи MUST включать `activity_id` и поля перечисленные в
  [`ACTIVITY_FALLBACK_BUSINESS_COLUMNS`][ref: repo:src/bioetl/sources/chembl/activity/parser/activity_parser.py@test_refactoring_32].
- Дедупликация MUST обеспечивать уникальность `activity_id`
  ([`duplicate_check`][ref: repo:src/bioetl/configs/pipelines/activity.yaml@test_refactoring_32]).
- Валидация выполняется схемой `ActivitySchema` с приведением типов в
  [`ActivityPipeline.transform`][ref: repo:src/bioetl/sources/chembl/activity/pipeline.py@test_refactoring_32].

### Assay {#source-assay}
- Конечная точка: `/assay.json`, собирается билдером
  [`AssayRequestBuilder`][ref: repo:src/bioetl/sources/chembl/assay/request/assay_request.py@test_refactoring_32].
- Батчи: конфиг `batch_size` 20, split по URL-лимиту 2000 символов.
- Бизнес-ключи: `assay_chembl_id` + whitelists из
  [`ASSAY_FALLBACK_BUSINESS_COLUMNS`][ref: repo:src/bioetl/sources/chembl/assay/constants.py@test_refactoring_32].
- Инвариант: `assay_chembl_id` MUST быть уникален (QC duplicates=0).
- Нормализация и fallback обрабатываются в
  [`AssayPipeline`][ref: repo:src/bioetl/sources/chembl/assay/pipeline.py@test_refactoring_32].

### Target {#source-target}
- Основной поток: ChEMBL `/target.json` + enrichment UniProt/IUPHAR через
  зарегистрированные стадии
  ([`enrichment_stage_registry`][ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32],
   [`TargetPipeline`][ref: repo:src/bioetl/sources/chembl/target/pipeline.py@test_refactoring_32]).
- Батчи: `batch_size=25`, `max_url_length=2000` из
  [`target.yaml`][ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32].
- Business key MUST включать `target_chembl_id` и UniProt accession при наличии.
- Валидация схемой `TargetSchema` (Pandera) гарантирует типы и nullable-поля.
- Инвариант: enrichment стадии SHOULD добавлять `iuphar_id` и `uniprot_accession`
  только при успешной валидации ответа.

### TestItem {#source-testitem}
- Основной источник: ChEMBL `/molecule.json`, дополнение PubChem через
  [`TestItemPipeline._enrich_pubchem`][ref: repo:src/bioetl/sources/chembl/testitem/pipeline.py@test_refactoring_32].
- Батчи: `batch_size=20`, `max_url_length=2000`.
- Business key MUST содержать `chembl_id` и структурные поля
  [`_CHEMBL_STRUCTURE_FIELDS`][ref: repo:src/bioetl/sources/chembl/testitem/pipeline.py@test_refactoring_32].
- Dedup политика: уникальный `chembl_id`, PubChem collisions логируются как QC предупреждения.

## Документный пайплайн {#document-pipeline-sources}
| Источник | Endpoint | Параметры | Пагинация | Политика ретраев | Бизнес-ключи |
| --- | --- | --- | --- | --- | --- |
| ChEMBL documents | `/document.json` | `document_chembl_id__in` | ID-батчи по 10 | Авто-backoff | `document_chembl_id` |
| PubMed | `efetch.fcgi` / `esummary.fcgi` | `db=pubmed`, `retmode=json`, `tool`, `email`, `api_key` | `retstart`+`retmax` | Экспоненциальный backoff, Retry-After | PMID, DOI |
| Crossref | `/works` | `filter=doi:...`, `mailto` | Cursor `next-cursor` | Backoff на HTTP429 | DOI |
| OpenAlex | `/works` | `filter=doi.search` | Cursor `cursor` | Backoff на HTTP429 | DOI |
| Semantic Scholar | `/paper/search` | `fields=...`, `query=doi:` | Cursor `next` | Backoff + API key | DOI |

- Конфигурация источников задана в
  [`pipelines/document.yaml`][ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32].
- Обогащения регистрируются как enrichment стадии в
  [`DocumentPipeline`][ref: repo:src/bioetl/pipelines/document.py@test_refactoring_32].
- Business key документа MUST включать `document_chembl_id` и стабильный DOI/PMID,
  что отражено в схеме `DocumentSchema`
  ([ref: repo:src/bioetl/schemas/document.py@test_refactoring_32]).
- Инвариант: при конфликте DOI из разных источников поле `conflict_doi` MUST быть
  заполнено, а запись остаётся в отчёте QC.

## Внешние самостоятельные источники {#external-standalone}
### PubChem {#source-pubchem}
- Endpoint: `/pug_view/data/compound/{cid}/JSON` с построением URL в
  [`PubChemClient._build_url`][ref: repo:src/bioetl/sources/pubchem/client.py@test_refactoring_32].
- Батчи: списки CID из входного CSV, чанки по 25.
- Business key: `cid`, `inchikey` MUST быть детерминированы.
- QC: отсутствующие поля фиксируются в `fallback_reason`.

### UniProt {#source-uniprot}
- Endpoint: `/uniprotkb/search`, параметры `query`, `fields`, `compressed=false`.
- Пагинация: cursor из ответа, обрабатывается в
  [`UniProtClient.iter_query`][ref: repo:src/bioetl/sources/uniprot/client.py@test_refactoring_32].
- Business key: `accession`.
- Инвариант: ответ MUST содержать `sequence.length`; иначе запись помечается как invalid.

### Guide to Pharmacology {#source-iuphar}
- Endpoint: `/targets`, параметры `?page={n}`.
- Аутентификация через API key в заголовке, разрешается моделью
  [`Source.resolve_contact_secrets`][ref: repo:src/bioetl/configs/models.py@test_refactoring_32].
- Business key: `iuphar_target_id`.
- Инвариант: при 401 ответе пайплайн MUST остановиться с ошибкой валидации ключа.

## Инварианты и правила валидации {#source-invariants}
- Все источники MUST соблюдать сортировку и хеширование из `determinism` профиля.
- HTTP-параметры `tool`, `email`, `mailto` SHOULD поставляться из окружения через
  `env:` ссылки, валидация реализована в
  [`Source.resolve_contact_secrets`][ref: repo:src/bioetl/configs/models.py@test_refactoring_32].
- Пайплайны MAY добавлять fallback записи, но они MUST содержать `fallback_reason`
  и `fallback_timestamp`.
- Валидация MUST происходить минимум дважды: после извлечения и после обогащения
  (см. `validate()` в [`PipelineBase`][ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]).
