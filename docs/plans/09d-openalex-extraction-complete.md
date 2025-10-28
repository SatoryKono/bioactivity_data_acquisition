# План: Извлечение метаданных из OpenAlex API для обогащения документов ChEMBL

## 0. Цель и артефакты

### Цель

Детерминированно извлекать метаданные научных публикаций из OpenAlex Works API по DOI, PMID, title search и обогащать document-измерение ChEMBL с фокусом на классификацию публикаций, типы, Open Access статус и venue metadata.

### Артефакты на выходе

**Landing (сырые данные):**
- `data/landing/openalex_raw_{yyyymmdd}.json` — сырые JSON записи для audit trail

**Bronze (нормализованные):**
- `data/bronze/openalex_records.parquet` — нормализованные записи, одна строка на work

**Silver (денормализованные справочники):**
- `data/silver/openalex_authors.parquet` — авторы с аффилиациями
- `data/silver/openalex_concepts.parcrete` — тематические концепты (Subject Areas)
- `data/silver/openalex_venues.parquet` — информацию о местах публикации

**Метаданные:**
- `meta/openalex/meta.yaml` — run_id, pipeline_version, row_count, hash_business_key, входы/выходы

**QC отчёты:**
- `qc/openalex/quality_report.csv` — пропуски полей, конфликты с другими источниками
- `qc/openalex/errors.csv` — все аварии и ошибки извлечения

**Логи:**
- `logs/openalex/*.jsonl` — структурированные логи в JSON Lines формате

## 1. Соответствие политике OpenAlex и режимы доступа

### Open Data и бесплатный доступ

OpenAlex — полностью открытый сервис:
- ✅ **Бесплатный**: нет API ключей
- ✅ **Щедрые лимиты**: "reasonable use"
- ✅ **Открытые данные**: под CC0 лицензией
- ✅ **Публичный код**: GitHub repository

### Rate Limiting

**Официальные рекомендации:**
- Не более 100K запросов/день на IP
- Минимум 100ms между запросами
- Не использовать параллельные запросы в агрессивном режиме
- Использовать API ключ для увеличенных лимитов (до 10 rps)

**Наша консервативная стратегия:**
- 10 запросов/секунду с token bucket
- 2-4 workers для параллелизма
- Exponential backoff при any errors
- Respectful behavior

### API Key (опционально)

Для увеличенных лимитов и лучшего обслуживания:
```
Authorization: Bearer <api_key>
```

Получить через: https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities-with-the-api#authentication

### User-Agent рекомендуется

```http
GET /works/W1234567890 HTTP/1.1
Host: api.openalex.org
User-Agent: bioactivity_etl/1.0 (contact: owner@example.org)
```

## 2. OpenAlex REST API: endpoints и возможности

### GET /works/{work_id} - извлечение по OpenAlex ID

```bash
curl "https://api.openalex.org/works/W1234567890"
```

OpenAlex ID префиксы:
- `W` — Works (публикации)
- `A` — Authors
- `V` — Venues (журналы)
- `I` — Institutions
- `C` — Concepts

### GET /works?filter=... - множественные критерии поиска

**По DOI:**
```bash
curl "https://api.openalex.org/works?filter=doi:10.1371/journal.pone.0000000"
```

**По PMID:**
```bash
curl "https://api.openalex.org/works?filter=pmid:12345678"
```

**По заголовку (title search):**
```bash
curl "https://api.openalex.org/works?filter=title.search:prostaglandin"
```

**Комбинированные фильтры:**
```bash
curl "https://api.openalex.org/works?filter=publication_year:2020,institutions.id:I123456789"
```

### Pagination

**Cursor-based (рекомендуется):**
```bash
# Первый запрос
curl "https://api.openalex.org/works?filter=doi:10.1371/*&per-page=100&cursor=*"

# Следующая страница
curl "https://api.openalex.org/works?filter=doi:10.1371/*&per-page=100&cursor=MjAyM..."
```

**Offset-based (legacy):**
```bash
curl "https://api.openalex.org/works?filter=doi:10.1371/*&page=2&per-page=200"
```

**Рекомендация:** всегда используйте cursor pagination для детерминизма!

### Select fields - оптимизация ответа

Уменьшить размер ответа:
```bash
curl "https://api.openalex.org/works/W1234567890?select=id,title,doi,publication_year,open_access"
```

### Sorting

```bash
curl "https://api.openalex.org/works?filter=publication_year:2020&sort=publication_date:desc"
```

## 3. Рейт-лимитинг и стратегия запросов

### Rate Limits

**Без API ключа (public):**
- "Reasonable use" (~100 requests/second на практике)
- Может тротлиться при злоупотреблении

**С API ключом:**
- До 10 requests/секунду гарантированно
- Приоритет в очереди

### Наша стратегия (консервативная)

- **По умолчанию**: 10 запросов/секунду
- **С API ключом**: 10 запросов/секунду гарантированно
- **Workers**: 4 параллельных потока
- **Timeout**: 30s для всех запросов

### Обработка ошибок

**429 Too Many Requests:**
```python
def handle_rate_limit(response):
    retry_after = response.headers.get('Retry-After', 10)
    time.sleep(float(retry_after))
```

**404 Not Found:**
- Work не существует
- Fallback на альтернативные источники

**5xx Server Errors:**
- Retry с exponential backoff
- Max 5 попыток

## 4. Извлекаемые поля и структура данных

### Основные метаданные

```json
{
  "id": "https://openalex.org/W1234567890",
  "doi": "https://doi.org/10.1371/journal.pone.0000000",
  "title": "Article Title Here",
  "display_name": "Article Title Here",
  "publication_date": "2023-03-15",
  "publication_year": 2023,
  "type": "article",
  "type_crossref": "journal-article",
  "language": "en",
  "is_oa": true,
  "open_access": {
    "is_oa": true,
    "oa_status": "gold",
    "oa_url": "https://journals.plos.org/..."
  },
  "primary_location": {
    "source": {
      "id": "https://openalex.org/V1234567890",
      "display_name": "PLoS ONE",
      "type": "journal",
      "issn_l": "1932-6203"
    },
    "landing_page_url": "https://journals.plos.org/...",
    "pdf_url": null
  }
}
```

### Авторы с аффилиациями

```json
{
  "authorships": [
    {
      "author": {
        "id": "https://openalex.org/A1234567890",
        "display_name": "John Doe",
        "orcid": "https://orcid.org/0000-0001-2345-6789"
      },
      "institutions": [
        {
          "id": "https://openalex.org/I1234567890",
          "display_name": "University Example",
          "ror": "https://ror.org/..."
        }
      ],
      "is_corresponding": true,
      "raw_affiliation_string": "Department of Biology, University Example",
      "author_position": "first"
    }
  ]
}
```

### Тематические концепты (Subject Areas)

```json
{
  "concepts": [
    {
      "id": "https://openalex.org/C1234567890",
      "display_name": "Medicine",
      "score": 0.9234,
      "level": 0
    },
    {
      "id": "https://openalex.org/C2345678901",
      "display_name": "Biology",
      "score": 0.8123,
      "level": 1
    }
  ]
}
```

### Venue (место публикации)

```json
{
  "primary_location": {
    "source": {
      "id": "https://openalex.org/V1234567890",
      "display_name": "PLoS ONE",
      "issn_l": "1932-6203",
      "issn": ["1932-6203"],
      "is_in_doaj": true,
      "type": "journal",
      "publisher": "Public Library of Science"
    }
  },
  "locations": [
    {
      "source": {...},
      "landing_page_url": "...",
      "pdf_url": "...",
      "is_oa": true,
      "version": "publishedVersion",
      "license": "https://creativecommons.org/licenses/by/4.0/"
    }
  ]
}
```

### Референсы и цитирования

```json
{
  "referenced_works": [
    "https://openalex.org/W9876543210"
  ],
  "related_works": [
    "https://openalex.org/W1111111111"
  ],
  "cited_by_count": 42,
  "cited_by_api_url": "https://api.openalex.org/works?filter=cites:W1234567890"
}
```

### Mesh термины (опционально)

```json
{
  "mesh": [
    {
      "descriptor_ui": "D000001",
      "descriptor_name": "Anesthesia",
      "qualifier_ui": null,
      "qualifier_name": null,
      "major_topic_yn": false
    }
  ]
}
```

## 5. Парсинг и нормализация

### OpenAlex ID нормализация

```python
def normalize_openalex_id(openalex_url):
    """Извлечь короткий ID из URL"""
    if not openalex_url:
        return None
    
    # Формат: https://openalex.org/W1234567890
    pattern = r'https://openalex\.org/([A-Z]\d+)'
    match = re.match(pattern, openalex_url)
    
    if match:
        return match.group(1)  # W1234567890
    
    return None
```

### DOI нормализация

```python
def normalize_openalex_doi(doi_value):
    """Извлечь DOI из OpenAlex format"""
    if not doi_value:
        return None
    
    # OpenAlex хранит DOI как URL
    if isinstance(doi_value, dict):
        doi_value = doi_value.get('DOI', '')
    
    # Удалить URL префикс
    doi = str(doi_value).replace('https://doi.org/', '').strip().lower()
    
    return doi if re.match(r'^10\.\d{4,}/[^\s]+$', doi) else None
```

### Open Access статус

```python
def extract_oa_status(record):
    """Извлечь OA статус с деталями"""
    oa = record.get('open_access', {})
    
    return {
        'is_oa': oa.get('is_oa', False),
        'oa_status': oa.get('oa_status'),  # gold | green | hybrid | bronze | closed
        'oa_url': oa.get('oa_url'),
        'version': extract_version_from_locations(record),
        'license': extract_license_from_locations(record)
    }
```

### Типы публикаций (приоритеты)

```python
def extract_publication_type(record):
    """Извлечь тип с приоритетами"""
    # Приоритет 1: type_crossref (считается более точным)
    if 'type_crossref' in record and record['type_crossref']:
        return record['type_crossref'], 'type_crossref'
    
    # Приоритет 2: type (OpenAlex native)
    if 'type' in record and record['type']:
        return record['type'], 'type'
    
    # Приоритет 3: primary_location source type
    if 'primary_location' in record:
        source = record['primary_location'].get('source', {})
        source_type = source.get('type')
        if source_type:
            return source_type, 'primary_location'
    
    return None, None
```

### Concepts нормализация

```python
def normalize_concepts(concepts_list):
    """Извлечь и нормализовать концепты"""
    normalized = []
    
    for concept in concepts_list:
        norm_concept = {
            'concept_id': normalize_openalex_id(concept.get('id')),
            'concept_name': concept.get('display_name', ''),
            'score': float(concept.get('score', 0.0)),
            'level': int(concept.get('level', 0))
        }
        normalized.append(norm_concept)
    
    # Сортировка по score (descending)
    normalized.sort(key=lambda x: x['score'], reverse=True)
    
    return normalized
```

### Authors нормализация

```python
def normalize_openalex_authors(authorships):
    """Нормализация авторов из OpenAlex"""
    authors = []
    
    for idx, authorship in enumerate(authorships):
        author_obj = authorship.get('author', {})
        institutions = authorship.get('institutions', [])
        
        normalized = {
            'position': idx + 1,
            'author_id': normalize_openalex_id(author_obj.get('id')),
            'display_name': author_obj.get('display_name', ''),
            'orcid': normalize_orcid(author_obj.get('orcid')),
            'is_corresponding': authorship.get('is_corresponding', False),
            'raw_affiliation': authorship.get('raw_affiliation_string', ''),
            'author_position': authorship.get('author_position'),  # first | middle | last
            'institutions': [
                inst.get('display_name', '') 
                for inst in institutions
            ],
            'institution_ror': [
                inst.get('ror') 
                for inst in institutions 
                if inst.get('ror')
            ]
        }
        authors.append(normalized)
    
    return authors
```

## 6. Title-based поиск (fallback для отсутствующих DOI/PMID)

### Поиск по заголовку

```python
def search_by_title(title, fuzzy=True):
    """Поиск работы по заголовку"""
    
    # Normalize title
    normalized_title = normalize_title_for_search(title)
    
    # Exact match
    exact_results = fetch_openalex_works(
        filter=f"title.search:{normalized_title}",
        per_page=10
    )
    
    # Fuzzy match (опционально)
    if fuzzy and len(exact_results) == 0:
        # Упростить заголовок для fuzzy search
        simplified = simplify_title(normalized_title)
        fuzzy_results = fetch_openalex_works(
            filter=f"title.search:{simplified}",
            per_page=10
        )
        return fuzzy_results
    
    return exact_results
```

### Scoring и выбор

```python
def score_title_match(work_title, search_title):
    """Вычислить similarity score"""
    # Normalize оба заголовка
    work_norm = normalize_title_for_search(work_title)
    search_norm = normalize_title_for_search(search_title)
    
    # Jaccard similarity на словах
    work_words = set(work_norm.split())
    search_words = set(search_norm.split())
    
    intersection = len(work_words & search_words)
    union = len(work_words | search_words)
    
    return intersection / union if union > 0 else 0.0
```

**Применение:** выбирать результат с score > 0.8

## 7. Детерминизм и сортировка

### Сортировка выгрузки

1. По `publication_date` ASC
2. По `updated_date` DESC
3. По `id` ASC (лексикографическая для документов без даты)

### Hash вычисление

```python
def compute_hashes(record):
    """Hash для детерминизма"""
    
    # Business key = первичный идентификатор
    biz_key = record.get('doi') or record.get('pmid') or record.get('id')
    hash_business_key = hashlib.sha1(
        str(biz_key).encode('utf-8')
    ).hexdigest()
    
    # Row hash = все поля
    normalized_values = json.dumps(
        record, 
        sort_keys=True, 
        ensure_ascii=False
    )
    hash_row = hashlib.sha1(
        normalized_values.encode('utf-8')
    ).hexdigest()
    
    return {
        'hash_business_key': hash_business_key,
        'hash_row': hash_row
    }
```

## 8. Конфликты и приоритеты источников

### Политика разрешения коллизий

**Для типов публикаций:**
- OpenAlex `type_crossref` — самый точный
- Fallback на Crossref для детализации

**Для дат:**
- PubMed completed > Crossref published-print > OpenAlex publication_date

**Для Open Access статуса:**
- OpenAlex — авторитет (полная детализация OA)
- Не брать из других источников

**Для venue metadata:**
- OpenAlex (DOAJ, ISSN-L, publisher)
- Crossref как fallback

## 9. Конфигурация (YAML)

```yaml
sources:
  openalex:
    enabled: true
    http:
      base_url: "https://api.openalex.org"
      timeout_sec: 30.0
      retries:
        total: 3
        backoff_multiplier: 2.0
        backoff_max: 60.0
      identify:
        user_agent: "bioactivity_etl/1.0 (contact: owner@example.org)"
        api_key_env: "OPENALEX_API_KEY"  # опционально
    batching:
      per_page: 200
      use_cursor: true
    rate_limit:
      max_calls_per_sec: 10
      burst: 10
      workers: 4
    caching:
      driver: "redis"
      ttl_sec: 86400
    fallback:
      title_search_enabled: true
      fuzzy_matching: true
      similarity_threshold: 0.8
```

## 10. Клиент и контракты

### Интерфейс OpenAlexClient

```python
class OpenAlexClient:
    def fetch_by_doi(self, doi: str) -> dict:
        """Извлечь по DOI"""
        
    def fetch_by_pmid(self, pmid: str) -> dict:
        """Извлечь по PMID"""
        
    def fetch_by_openalex_id(self, oa_id: str) -> dict:
        """Извлечь по OpenAlex ID"""
        
    def search_by_title(
        self, 
        title: str, 
        fuzzy: bool = False
    ) -> list[dict]:
        """Поиск по заголовку с optional fuzzy matching"""
        
    def fetch_with_cursor(
        self, 
        filter_query: str, 
        per_page: int = 200
    ) -> Iterator[dict]:
        """Cursor pagination для больших списков"""
```

### Гарантии клиента

1. Автоматическое кэширование
2. Rate limiting
3. Retry при transient errors
4. Сохранение сырых JSON
5. Structured logging

## 11. Схемы данных

### OpenAlexRecordSchema (Bronze)

```python
@dataclass
class OpenAlexRecord:
    openalex_id: str  # PK
    doi: Optional[str]
    pmid: Optional[str]
    title: str
    publication_date: str  # ISO format
    publication_year: int
    type: str
    type_crossref: Optional[str]
    language: Optional[str]
    is_oa: bool
    oa_status: Optional[str]  # gold | green | hybrid | bronze | closed
    oa_url: Optional[str]
    venue_display_name: Optional[str]
    venue_id: Optional[str]
    venue_issn_l: Optional[str]
    venue_type: Optional[str]
    publisher: Optional[str]
    cited_by_count: int
    created_date: str
    updated_date: str
    hash_business_key: str
    hash_row: str
```

### OpenAlexConceptSchema (Silver)

```python
@dataclass
class OpenAlexConcept:
    openalex_id: str
    concept_id: str
    concept_name: str
    score: float
    level: int
```

### OpenAlexAuthorSchema (Silver)

```python
@dataclass
class OpenAlexAuthor:
    openalex_id: str
    position: int
    author_id: str
    display_name: str
    orcid: Optional[str]
    is_corresponding: bool
    author_position: str
    institutions: list[str]
    institution_ror: list[str]
```

## 12. Тест-план

### Unit тесты

1. **OpenAlex ID parsing:**
   - URL → short ID conversion
   - Invalid formats

2. **OA status extraction:**
   - gold, green, hybrid, bronze, closed
   - Missing open_access object

3. **Concepts normalization:**
   - Score sorting
   - Level extraction

4. **Title search:**
   - Exact matching
   - Fuzzy matching threshold

### Integration тесты

1. **Cursor pagination:**
   - Большой результат set
   - Детерминизм порядка

2. **Rate limiting:**
   - 429 handling
   - Backoff

3. **Golden files:**
   - Stable Parquet outputs
   - Checksums

4. **Title search fallback:**
   - Когда DOI/PMID не найдены
   - Similarity scoring

## 13. Документация

**`docs/sources/openalex-extraction.md`:**
- API спецификация
- Rate limits
- Filter syntax
- Examples

**`docs/api/openalex-client.md`:**
- Client contracts
- Error handling

**`docs/examples/openalex-usage.md`:**
- DOI/PMID extraction
- Title search fallback
- Cursor pagination

## 14. CLI интерфейс

```bash
python -m scripts/get_openalex.py \
  --input data/input/identifiers.csv \
  --config configs/config_openalex.yaml \
  --save-raw landing/openalex_$(date +%Y%m%d).json \
  --out parquet:data/bronze/openalex_records.parquet
```

### Опции

- `--mode {doi,pmid,oa_id,title}` — режим поиска
- `--rps 10` — requests per second
- `--api-key-env OPENALEX_API_KEY` — API ключ
- `--fallback-title` — включить title search fallback
- `--similarity-threshold 0.8` — порог для fuzzy matching

## 15. QC и лицензирование

### Quality checks

- Missing titles
- Конфликты типов с Crossref
- Invalid OA статусы
- Дубликаты по DOI/PMID

### Лицензирование

✅ **OpenAlex полностью открыт:**

- Metadata под CC0 (public domain)
- Можно использовать свободно
- Атрибуция: "Data from OpenAlex"
- Terms: https://docs.openalex.org/api-use-policy

## 16. Что именно улучшено

1. ✅ Title-based поиск для fallback (когда нет DOI/PMID)
2. ✅ Cursor pagination для детерминизма
3. ✅ OA статус детализация (gold/green/hybrid/bronze/closed)
4. ✅ Concepts extraction с scoring
5. ✅ Venue metadata (DOAJ, ISSN-L, publisher)
6. ✅ Fuzzy title matching
7. ✅ Graceful degradation
8. ✅ Comprehensive structured logging

