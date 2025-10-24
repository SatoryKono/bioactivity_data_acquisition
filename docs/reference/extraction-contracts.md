# Extraction Contracts

## Обзор

Контракты извлечения данных определяют правила взаимодействия с внешними API, включая параметры запросов, обработку ошибок, кэширование и детерминизм результатов.

## ChEMBL API Contract

### Базовые параметры

| Параметр | Значение | Описание |
|----------|----------|----------|
| **Base URL** | `https://www.ebi.ac.uk/chembl/api/data` | Базовый URL API |
| **Timeout** | 60 секунд | Таймаут запроса |
| **Retries** | 5 попыток | Количество повторных попыток |
| **Backoff** | Экспоненциальный (2.0x) | Стратегия задержки |
| **Rate Limit** | 5 req/s | Рекомендуемая частота запросов |
| **Cache TTL** | 24 часа | Время жизни кэша |

### Endpoint Contracts

#### Document Endpoint
```yaml
endpoint: /document/{document_chembl_id}
method: GET
parameters:
  document_chembl_id:
    type: string
    pattern: "^CHEMBL\\d+$"
    required: true
    description: "ChEMBL ID документа"
response_schema:
  document_chembl_id: string
  document_type: string
  title: string
  doi: string
  pubmed_id: integer
  journal: string
  year: integer
  abstract: string
  authors: string
error_handling:
  - status: 404
    action: "log_warning"
    fallback: "skip_record"
  - status: 429
    action: "retry_with_backoff"
    max_retries: 5
  - status: 5xx
    action: "retry_with_backoff"
    max_retries: 3
```

#### Target Endpoint
```yaml
endpoint: /target/{target_chembl_id}
method: GET
parameters:
  target_chembl_id:
    type: string
    pattern: "^CHEMBL\\d+$"
    required: true
    description: "ChEMBL ID мишени"
response_schema:
  target_chembl_id: string
  pref_name: string
  target_type: string
  organism: string
  tax_id: integer
  target_components: array
    items:
      component_id: integer
      component_type: string
      uniprot_id: string
```

#### Activity Endpoint
```yaml
endpoint: /activity
method: GET
parameters:
  limit:
    type: integer
    default: 20
    maximum: 1000
    description: "Количество записей"
  offset:
    type: integer
    default: 0
    description: "Смещение для пагинации"
  assay_chembl_id:
    type: string
    pattern: "^CHEMBL\\d+$"
    description: "Фильтр по ассаю"
  molecule_chembl_id:
    type: string
    pattern: "^CHEMBL\\d+$"
    description: "Фильтр по молекуле"
  target_chembl_id:
    type: string
    pattern: "^CHEMBL\\d+$"
    description: "Фильтр по мишени"
pagination:
  strategy: "offset_limit"
  max_page_size: 1000
  total_count_field: "page_meta.total_count"
```

## Crossref API Contract

### Базовые параметры

| Параметр | Значение | Описание |
|----------|----------|----------|
| **Base URL** | `https://api.crossref.org` | Базовый URL API |
| **Timeout** | 30 секунд | Таймаут запроса |
| **Retries** | 3 попытки | Количество повторных попыток |
| **Rate Limit** | 50 req/s | Частота запросов |
| **Cache TTL** | 7 дней | Время жизни кэша |

### Works Search Contract
```yaml
endpoint: /works
method: GET
parameters:
  query:
    type: string
    required: true
    description: "Поисковый запрос"
  filter:
    type: string
    description: "Фильтры (from-pub-date, to-pub-date)"
  select:
    type: string
    default: "DOI,title,author"
    description: "Поля для возврата"
  rows:
    type: integer
    default: 20
    maximum: 1000
    description: "Количество результатов"
  cursor:
    type: string
    description: "Курсор для пагинации"
pagination:
  strategy: "cursor_based"
  cursor_field: "message.next-cursor"
  total_count_field: "message.total-results"
```

### Work by DOI Contract
```yaml
endpoint: /works/{DOI}
method: GET
parameters:
  DOI:
    type: string
    pattern: "^10\\.\\d+/.*"
    required: true
    description: "DOI документа"
response_schema:
  DOI: string
  title: array
  author: array
  published-print:
    date-parts: array
  container-title: array
  abstract: string
```

## OpenAlex API Contract

### Базовые параметры

| Параметр | Значение | Описание |
|----------|----------|----------|
| **Base URL** | `https://api.openalex.org` | Базовый URL API |
| **Timeout** | 30 секунд | Таймаут запроса |
| **Retries** | 3 попытки | Количество повторных попыток |
| **Rate Limit** | 10 req/s | Частота запросов |
| **Cache TTL** | 7 дней | Время жизни кэша |

### Works Search Contract
```yaml
endpoint: /works
method: GET
parameters:
  filter:
    type: string
    description: "Фильтры (doi:, title:, author:)"
  search:
    type: string
    description: "Поисковый запрос"
  per-page:
    type: integer
    default: 25
    maximum: 200
    description: "Количество результатов на страницу"
pagination:
  strategy: "page_based"
  page_field: "meta.page"
  total_count_field: "meta.count"
```

## PubMed API Contract

### Базовые параметры

| Параметр | Значение | Описание |
|----------|----------|----------|
| **Base URL** | `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/` | Базовый URL API |
| **Timeout** | 60 секунд | Таймаут запроса |
| **Retries** | 10 попыток | Количество повторных попыток |
| **Rate Limit** | 3 req/s (без ключа), 10 req/s (с ключом) | Частота запросов |
| **Cache TTL** | 7 дней | Время жизни кэша |

### ESearch Contract
```yaml
endpoint: /esearch.fcgi
method: GET
parameters:
  db:
    type: string
    default: "pubmed"
    description: "База данных"
  term:
    type: string
    required: true
    description: "Поисковый запрос"
  retmax:
    type: integer
    default: 20
    maximum: 10000
    description: "Максимальное количество результатов"
  retmode:
    type: string
    default: "json"
    description: "Формат ответа"
  api_key:
    type: string
    description: "API ключ (опционально)"
response_schema:
  esearchresult:
    count: string
    retmax: string
    retstart: string
    idlist: array
```

### EFetch Contract
```yaml
endpoint: /efetch.fcgi
method: GET
parameters:
  db:
    type: string
    default: "pubmed"
    description: "База данных"
  id:
    type: string
    required: true
    description: "PMID или список PMID"
  retmode:
    type: string
    default: "json"
    description: "Формат ответа"
  rettype:
    type: string
    default: "abstract"
    description: "Тип данных"
  api_key:
    type: string
    description: "API ключ (опционально)"
```

## Semantic Scholar API Contract

### Базовые параметры

| Параметр | Значение | Описание |
|----------|----------|----------|
| **Base URL** | `https://api.semanticscholar.org/graph/v1` | Базовый URL API |
| **Timeout** | 60 секунд | Таймаут запроса |
| **Retries** | 15 попыток | Количество повторных попыток |
| **Rate Limit** | 100 req/5min, burst до 2 | Частота запросов |
| **Cache TTL** | 7 дней | Время жизни кэша |

### Paper Search Contract
```yaml
endpoint: /paper/search
method: GET
parameters:
  query:
    type: string
    required: true
    description: "Поисковый запрос"
  fields:
    type: string
    default: "paperId,title,abstract,venue,year"
    description: "Поля для возврата"
  limit:
    type: integer
    default: 10
    maximum: 100
    description: "Количество результатов"
  offset:
    type: integer
    default: 0
    description: "Смещение для пагинации"
```

## UniProt API Contract

### Базовые параметры

| Параметр | Значение | Описание |
|----------|----------|----------|
| **Base URL** | `https://rest.uniprot.org` | Базовый URL API |
| **Timeout** | 45 секунд | Таймаут запроса |
| **Retries** | 3 попытки | Количество повторных попыток |
| **Rate Limit** | ~10 req/s | Частота запросов (политес) |
| **Cache TTL** | 14 дней | Время жизни кэша |

### UniProtKB Entry Contract
```yaml
endpoint: /uniprotkb/{accession}
method: GET
parameters:
  accession:
    type: string
    pattern: "^[OPQ][0-9][A-Z0-9]{3}[0-9]$"
    required: true
    description: "UniProt accession"
  format:
    type: string
    default: "json"
    description: "Формат ответа"
  fields:
    type: string
    description: "Поля для возврата"
```

## PubChem API Contract

### Базовые параметры

| Параметр | Значение | Описание |
|----------|----------|----------|
| **Base URL** | `https://pubchem.ncbi.nlm.nih.gov/rest/pug` | Базовый URL API |
| **Timeout** | 30 секунд | Таймаут запроса |
| **Retries** | 3 попытки | Количество повторных попыток |
| **Rate Limit** | 5 req/s | Частота запросов |
| **Cache TTL** | 7 дней | Время жизни кэша |

### Compound Properties Contract
```yaml
endpoint: /compound/cid/{cid}/property/{properties}/JSON
method: GET
parameters:
  cid:
    type: integer
    required: true
    description: "PubChem CID"
  properties:
    type: string
    required: true
    description: "Список свойств через запятую"
    example: "MolecularFormula,MolecularWeight,CanonicalSMILES"
```

## Общие принципы контрактов

### Обработка ошибок

#### HTTP Status Codes
```yaml
error_handling:
  200-299:
    action: "process_response"
  400-499:
    action: "log_error"
    fallback: "skip_record"
  429:
    action: "retry_with_backoff"
    max_retries: 5
    backoff_multiplier: 2.0
  500-599:
    action: "retry_with_backoff"
    max_retries: 3
    backoff_multiplier: 1.5
  timeout:
    action: "retry_with_backoff"
    max_retries: 3
```

#### Retry Strategy
```yaml
retry_config:
  total_retries: 5
  backoff_multiplier: 2.0
  initial_delay: 1.0
  max_delay: 60.0
  jitter: true
  retryable_errors:
    - "timeout"
    - "connection_error"
    - "rate_limit"
    - "server_error"
```

### Кэширование

#### Cache Strategy
```yaml
cache_config:
  enabled: true
  ttl:
    chembl: 86400  # 24 часа
    crossref: 604800  # 7 дней
    openalex: 604800  # 7 дней
    pubmed: 604800  # 7 дней
    semantic_scholar: 604800  # 7 дней
    uniprot: 1209600  # 14 дней
    pubchem: 604800  # 7 дней
  key_generation:
    algorithm: "sha256"
    include_headers: false
    include_auth: false
  storage:
    backend: "filesystem"
    directory: "data/cache"
    max_size: "1GB"
```

### Детерминизм

#### Request Ordering
```yaml
determinism:
  sort_parameters: true
  stable_sorting: true
  parameter_order:
    - "limit"
    - "offset"
    - "filter"
    - "select"
    - "query"
```

#### Response Processing
```yaml
response_processing:
  sort_results: true
  stable_sorting: true
  sort_keys:
    - "id"
    - "title"
    - "date"
  normalize_whitespace: true
  normalize_unicode: true
```

### Мониторинг и телеметрия

#### Метрики
```yaml
metrics:
  request_count:
    labels: ["source", "endpoint", "status"]
  request_duration:
    labels: ["source", "endpoint"]
  cache_hit_rate:
    labels: ["source", "endpoint"]
  error_rate:
    labels: ["source", "endpoint", "error_type"]
  rate_limit_hits:
    labels: ["source"]
```

#### Алерты
```yaml
alerts:
  high_error_rate:
    threshold: 0.1
    duration: "5m"
  rate_limit_exceeded:
    threshold: 1
    duration: "1m"
  cache_miss_rate:
    threshold: 0.5
    duration: "10m"
```

### Валидация контрактов

#### Schema Validation
```yaml
validation:
  request_schema: true
  response_schema: true
  parameter_validation: true
  type_checking: true
  range_checking: true
```

#### Quality Gates
```yaml
quality_gates:
  min_success_rate: 0.95
  max_response_time: 30.0
  min_cache_hit_rate: 0.8
  max_error_rate: 0.05
```

## Версионирование контрактов

### Semantic Versioning
- **Major:** Изменения, нарушающие обратную совместимость
- **Minor:** Новые параметры или поля
- **Patch:** Исправления ошибок

### Миграция
```yaml
migration:
  version: "2.0.0"
  breaking_changes:
    - "Изменение формата ответа"
    - "Удаление устаревших параметров"
  deprecation_notice: "90 дней"
  fallback_support: true
```

## Тестирование контрактов

### Contract Testing
```yaml
testing:
  unit_tests: true
  integration_tests: true
  contract_tests: true
  load_tests: true
  chaos_tests: true
```

### Test Data
```yaml
test_data:
  mock_responses: true
  test_scenarios:
    - "successful_request"
    - "rate_limited_request"
    - "timeout_request"
    - "invalid_parameters"
    - "empty_response"
```
