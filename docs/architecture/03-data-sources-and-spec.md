# data-sources-and-spec

## источники-и-api

| Источник | Каталог | Основные конечные точки | Пагинация | Лимиты и квоты | Ретраи и таймауты | Примечания |
| --- | --- | --- | --- | --- | --- | --- |
| ChEMBL Activity | `sources/chembl/activity` | `/activity.json?activity_id__in=` | Батчи по ID, деление по длине URL | batch_size 20, `max_url_length=2000` | Глобальные ретраи из `base.yaml`, 5 попыток | Fallback-записи формируются при отказах.[ref: repo:src/bioetl/pipelines/chembl_activity.py@test_refactoring_32]
| ChEMBL Assay | `sources/chembl/assay` | `/assay.json`, `/assay_type.json` | Смещение по offset/limit | batch_size 50 (по умолчанию) | Ретраи 5 попыток, rate limit 12 req/s | BAO-нормализация через словари.[ref: repo:src/bioetl/pipelines/chembl_assay.py@test_refactoring_32]
| ChEMBL Target | `sources/chembl/target` | `/target.json`, `/target_component.json` | Постраничная пагинация | batch_size 25 | Персональные таймауты и circuit breaker в `target.yaml` | Обогащение UniProt/IUPHAR в отдельных стадиях.[ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32]
| ChEMBL Document | `sources/chembl/document` | `/document.json` | Батчи по `document_chembl_id` | batch_size 10 | Таймауты 60s, ретраи 5 | Поддерживает режимы `chembl`/`all` и внешние адаптеры.[ref: repo:src/bioetl/sources/chembl/document/pipeline.py@test_refactoring_32]
| ChEMBL Test Item | `sources/chembl/testitem` | `/molecule.json`, `/compound_records.json` | Батчи по molecule ID | batch_size 25 | Ретраи 5, rate limit 12 req/s | Пайплайн активирует PubChem для синонимов.[ref: repo:src/bioetl/sources/chembl/testitem/pipeline.py@test_refactoring_32]
| PubChem PUG-REST | `sources/pubchem` | `/compound/inchikey/{}/cids`, `/compound/cid/{}/property` | Батчи по списку CID | batch_size 50 | Rate limit 5 req/15s (из базового профиля) | Повторные запросы с backoff при 5xx.[ref: repo:src/bioetl/sources/pubchem/request/builder.py@test_refactoring_32]
| UniProt REST | `sources/uniprot` | `/uniprotkb/{id}`, `/uniprotkb/stream` | Cursor (stream) | rate_limit 3 req/s | Таймаут 60s, 4 попытки | Используется для таргетов и отдельного пайплайна.[ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32]
| UniProt ID Mapping | `sources/uniprot` | `/idmapping/run`, `/idmapping/status` | Пулинг статуса | rate_limit 2 req/s | Таймаут 60s, 4 попытки | Кэширование включено для повторного использования.[ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32]
| IUPHAR | `sources/iuphar` | `/targets`, `/targets/families` | PageNumberPaginator (size 200) | rate_limit 6 req/s | Таймаут 45s, 4 попытки | Требует `x-api-key` из окружения.[ref: repo:src/bioetl/sources/iuphar/pagination.py@test_refactoring_32]
| PubMed E-utilities | `sources/pubmed` | `/efetch.fcgi`, `/esearch.fcgi` | Батчи по 200 ID | 3 req/s без ключа, 10 с ключом | Таймауты глобальные; backoff при ошибках | Добавляет `tool`, `email`, `api_key` в запрос.[ref: repo:src/bioetl/sources/pubmed/request/builder.py@test_refactoring_32]
| Crossref REST | `adapters/crossref` | `/works` | Cursor (mailto) | rate_limit 2 req/s | Таймауты по профилю, ретраи 5 | Требует `mailto` в User-Agent.[ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32]
| OpenAlex | `adapters/openalex` | `/works` | Cursor `cursor=*` | rate_limit 10 req/s | Таймауты по профилю | Требует `mailto` параметр и заголовок.[ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32]
| Semantic Scholar | `adapters/semantic_scholar` | `/paper/batch` | Батчи по 50 ID | rate_limit 1 req/1.25s (10 с ключом) | Ретраи по базовой политике | API key опциональный, передаётся в заголовках.[ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32]

## извлекаемые-сущности-и-цели

| Сущность | Основной источник | Целевая схема | Обогащение | Обязательные поля | Формат вывода |
| --- | --- | --- | --- | --- | --- |
| `activity` | ChEMBL | `ActivitySchema` | — | `activity_id`, `assay_chembl_id`, `molecule_chembl_id`, измерения | CSV + JSON отладочный дамп |[ref: repo:src/bioetl/pipelines/chembl_activity.py@test_refactoring_32]
| `assay` | ChEMBL | `AssaySchema` | BAO lookup | `assay_chembl_id`, `assay_type`, `assay_category` | CSV/Parquet |[ref: repo:src/bioetl/pipelines/chembl_assay.py@test_refactoring_32]
| `target` | ChEMBL | `TargetSchema` | UniProt, IUPHAR | `target_chembl_id`, `organism`, `gene_symbol` | Parquet (gold), CSV (qc) |[ref: repo:src/bioetl/sources/chembl/target/pipeline.py@test_refactoring_32]
| `document` | ChEMBL | `DocumentNormalizedSchema` | PubMed, Crossref, OpenAlex, Semantic Scholar | `document_chembl_id`, `title`, `doi_clean` или `pmid` | CSV с QC-отчётами |[ref: repo:src/bioetl/sources/chembl/document/pipeline.py@test_refactoring_32]
| `testitem` | ChEMBL | `TestItemSchema` | PubChem | `molecule_chembl_id`, связи с родителями/солями | CSV |[ref: repo:src/bioetl/sources/chembl/testitem/pipeline.py@test_refactoring_32]
| `pubchem_enrichment` | PubChem | `PubChemEnrichmentSchema` | — | `inchikey`, `cid`, набор свойств | CSV |[ref: repo:src/bioetl/sources/pubchem/pipeline.py@test_refactoring_32]
| `gtp_iuphar_targets` | IUPHAR | `IupharTargetSchema` | Семейства и gold-классы | `targetId`, `iuphar_target_id`, `name` | CSV + дополнительные таблицы |[ref: repo:src/bioetl/sources/iuphar/pipeline.py@test_refactoring_32]
| `uniprot` | UniProt | `UniProtSchema` | — | `accession`, биотип, гены | Parquet |[ref: repo:src/bioetl/sources/uniprot/pipeline.py@test_refactoring_32]

## бизнес-ключи-и-дедуп

| Сущность | Бизнес-ключ | Дедупликация | Приоритет источников |
| --- | --- | --- | --- |
| `activity` | (`activity_id`) и хеш бизнес-ключа | Fallback и QC фиксируют дубликаты `activity_id` | Чистые данные ChEMBL, fallback только при ошибках |[ref: repo:src/bioetl/sources/chembl/activity/parser/activity_parser.py@test_refactoring_32]
| `assay` | `assay_chembl_id` | Проверка `duplicate_check` в конфиге (`threshold=0`) | ChEMBL → BAO |[ref: repo:src/bioetl/configs/pipelines/assay.yaml@test_refactoring_32]
| `target` | `target_chembl_id` + `uniprot_accession` (если есть) | `MaterializationManager` отслеживает уникальность по стадиям | UniProt > IUPHAR > ChEMBL |[ref: repo:src/bioetl/sources/chembl/target/merge/policy.py@test_refactoring_32]
| `document` | `document_chembl_id` + нормализованный `doi_clean` или `pmid` | Merge-policy устраняет коллизии, QC считает конфликты | Crossref > PubMed > OpenAlex > ChEMBL |[ref: repo:src/bioetl/sources/chembl/document/merge/policy.py@test_refactoring_32]
| `testitem` | `molecule_chembl_id` | Хеш строки и проверка с PubChem CID | PubChem > ChEMBL |[ref: repo:src/bioetl/sources/chembl/testitem/merge/policy.py@test_refactoring_32]
| `gtp_iuphar_targets` | `iuphar_target_id` | `PageNumberPaginator` удаляет дубль по `unique_key` | Семейства IUPHAR > сырые данные |[ref: repo:src/bioetl/sources/iuphar/service.py@test_refactoring_32]

## инварианты-и-валидационные-правила

- Pandera-схемы MUST совпадать с версией в `schema_registry`; drift блокирует

  запись.[ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]

- Единицы измерения активностей нормализуются в `ActivityNormalizer`; неизвестные

  единицы MUST приводить к ошибке валидации.[ref: repo:src/bioetl/transform/adapters/chembl_activity.py@test_refactoring_32]

- DOI и идентификаторы документов нормализуются в lowercase и без префиксов

  URL; конфликты фиксируются в полях `conflict_doi`/`conflict_pmid`.[ref: repo:src/bioetl/sources/chembl/document/normalizer.py@test_refactoring_32]

- Таргеты MUST иметь `organism` и валидный `gene_symbol`, иначе запись

  попадает в QC с уровнем `warning` и не исключается из датасета.[ref: repo:src/bioetl/sources/chembl/target/service.py@test_refactoring_32]

- Все пайплайны сортируют вывод в соответствии с `determinism.column_order`

  конфигурации; отсутствие колонки приводит к вставке `pd.NA` и логированию.[ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]

## политика-повторов-и-ошибок

- HTTP статусы 408, 425, 429, 5xx MUST запускать экспоненциальный backoff

  согласно `RetryConfig` (максимум 5 попыток).[ref: repo:src/bioetl/config/models.py@test_refactoring_32]

- Circuit breaker в таргет-пайплайне SHOULD переводить источник в деградирующий

  режим после 5 подряд ошибок, сохраняя fallback-данные.[ref: repo:src/bioetl/configs/pipelines/target.yaml@test_refactoring_32]

- Пайплайны MAY продолжить работу при non-critical QC ошибках, но запись в

  `validation_issues` обязательна для последующего анализа.[ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]
