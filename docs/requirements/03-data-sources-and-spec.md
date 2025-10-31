# Требования к данным и источникам

## сводная-матрица

| Сущность | Основной источник | Обогащения | Бизнес-ключ | Примечания |
| --- | --- | --- | --- | --- |
| `activities` | ChEMBL Activity API | — | `activity_id` | Fallback-записи допускаются, но **MUST** быть помечены `fallback_reason`. |
| `assays` | ChEMBL Assay API | BAO словари | `assay_chembl_id` | Нормализация типов/категорий по BAO. |
| `targets` | ChEMBL Target API | UniProt, GtoP (IUPHAR) | `target_chembl_id` | UniProt приоритетен для номенклатуры; IUPHAR для классификации. |
| `testitems` | ChEMBL Molecule API | PubChem | `molecule_chembl_id` | Сопоставление солей/parent по таблицам PubChem. |
| `documents` | ChEMBL Documents | PubMed, Crossref, OpenAlex, Semantic Scholar | `document_chembl_id` (fallback `doi_clean`/`pmid`) | Merge-приоритет: Crossref > PubMed > OpenAlex > Semantic Scholar > ChEMBL. |

## источники

### chembl

| Параметр | Значение |
| --- | --- |
| Базовый URL | `https://www.ebi.ac.uk/chembl/api/data` ([ref: repo:src/bioetl/configs/includes/chembl_source.yaml@test_refactoring_32]) |
| Пагинация | Offset/Limit (`limit=batch_size`, `offset=true`) |
| Лимиты | `batch_size` по сущности (обычно 20); `max_url_length=2000` |
| Аутентификация | Нет; User-Agent **MUST** идентифицировать пайплайн |
| Повторы | Управляются глобальными настройками `http.global` |

### pubmed (NCBI E-utilities)

| Параметр | Значение |
| --- | --- |
| Базовый URL | `https://eutils.ncbi.nlm.nih.gov/entrez/eutils` ([ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32]) |
| Endpoint | `esearch`, `efetch`, `esummary` (последовательные батчи) |
| Идентификация | `tool=${PUBMED_TOOL}` и `email=${PUBMED_EMAIL}` **MUST** быть заданы; `api_key` увеличивает лимит до 10 req/s |
| Лимиты | 3 запроса/с без ключа; соблюдается параметрами `rate_limit_max_calls`, `rate_limit_period` |
| Пагинация | `retstart`/`retmax`; батч 200 |

### crossref

| Параметр | Значение |
| --- | --- |
| Базовый URL | `https://api.crossref.org` |
| Идентификация | `mailto` в query и `User-Agent` |
| Пагинация | `cursor` + `rows`; конфиг использует `batch_size=100` |
| Лимиты | 50 req/s в “polite pool”; локально 2 req/s для устойчивости |

### openalex

| Параметр | Значение |
| --- | --- |
| Базовый URL | `https://api.openalex.org` |
| Пагинация | Cursor API (`cursor=*`), `per-page=100` |
| Идентификация | `mailto` обязателен для polite pool |
| Лимиты | 10 req/s (конфиг), workers=4 |

### semantic-scholar

| Параметр | Значение |
| --- | --- |
| Базовый URL | `https://api.semanticscholar.org/graph/v1` |
| Пагинация | Cursor (`next`) |
| Лимиты | 1 req/1.25s без API ключа; до 10 c API ключом |
| Поля | Конфигирует `fields` для выборки DOI, цитирований, авторов |

### uniprot

| Параметр | Значение |
| --- | --- |
| Базовый URL | см. `src/bioetl/configs/pipelines/uniprot.yaml` |
| Пагинация | Cursor, параметры `size` |
| Нормализация | `normalizer_service.py` конвертирует accession, отделяет версии |

### pubchem

| Параметр | Значение |
| --- | --- |
| Базовый URL | `https://pubchem.ncbi.nlm.nih.gov/rest/pug` |
| Неймспейс | CID/SID; поддержка batch запросов |
| Формат | JSON (`Accept: application/json`) |

### iuphar (Guide to Pharmacology)

| Параметр | Значение |
| --- | --- |
| Базовый URL | см. `src/bioetl/configs/pipelines/iuphar.yaml` |
| Аутентификация | API ключ обязателен (`IUPHAR_API_KEY`) |
| Цель | Классификации мишеней, связи с UniProt |

## бизнес-ключи-и-дедуп

| Сущность | Ключ | Дедупликация |
| --- | --- | --- |
| `activities` | `activity_id` | При отсутствии данных об активности формируется fallback с `hash_business_key`; повторы запрещены (`fallback.rate` контролируется QC). |
| `assays` | `assay_chembl_id` | Дубликаты блокируются на уровне QC (`duplicates=0`). |
| `targets` | `target_chembl_id` | При объединении с UniProt/IUPHAR приоритет: UniProt > ChEMBL для номенклатуры, IUPHAR > ChEMBL для классификации. |
| `testitems` | `molecule_chembl_id` | Дополнительно контролируется пара `parent`/`salt`; PubChem-соответствия должны быть детерминированы. |
| `documents` | `document_chembl_id` (fallback `doi_clean`, `pmid`) | Merge policy фиксирован: Crossref > PubMed > OpenAlex > Semantic Scholar > ChEMBL; конфликты фиксируются в колонках `*_source`. |

## инварианты-данных

- Идентификаторы **MUST** соответствовать канону: `CHEMBL\d+`, `^10\..+` для DOI, `^\d+$` для PMID, верхний регистр для UniProt accession.
- Даты **MUST** сериализоваться в ISO 8601, зона — UTC. При пониженной точности указываются поля `date_precision`.
- Числовые значения активностей **MUST** храниться в целевых единицах (`standard_units`), нормализация выполняется в нормализаторах (`ActivityNormalizer`).
- Все дополнительные поля, не входящие в Schema Registry, **MUST** попадать в `extras` или `*_source` колоноки, чтобы не нарушать стабильность контрактов.
- Пропуски классифицируются на `structural` (нет в источнике) и `missing` (источник явно сообщает); политика зафиксирована в схемах Pandera.

## ссылки-на-реализацию

- Пайплайны: [ref: repo:src/bioetl/sources/chembl/activity/pipeline.py@test_refactoring_32], [ref: repo:src/bioetl/sources/chembl/assay/pipeline.py@test_refactoring_32], [ref: repo:src/bioetl/sources/chembl/target/pipeline.py@test_refactoring_32], [ref: repo:src/bioetl/sources/chembl/testitem/pipeline.py@test_refactoring_32], [ref: repo:src/bioetl/sources/chembl/document/pipeline.py@test_refactoring_32].
- Конфиги источников: [ref: repo:src/bioetl/configs/pipelines/@test_refactoring_32].
- Тесты: `tests/sources/<source>/`, `tests/integration/pipelines/`, `tests/golden/`.


