# План: Извлечение метаданных из Semantic Scholar API для обогащения документов ChEMBL

## 0. Цель и артефакты

### Цель

Детерминированно извлекать метаданные научных публикаций из Semantic Scholar API по PMID, DOI, title и обогащать document-измерение ChEMBL с фокусом на citation metrics, venue classification и author information.

### Артефакты на выходе

**Landing (сырые данные):**
- `data/landing/semanticscholar_raw_{yyyymmdd}.json` — сырые JSON записи для audit trail

**Bronze (нормализованные):**
- `data/bronze/semanticscholar_records.parquet` — нормализованные записи, одна строка на paper

**Silver (денормализованные справочники):**
- `data/silver/semanticscholar_authors.parquet` — авторы с аффилиациями
- `data/silver/semanticscholar_citations.parquet` — цитаты и references

**Метаданные:**
- `meta/semanticscholar/meta.yaml` — run_id, pipeline_version, row_count, hash_business_key, входы/выходы

**QC отчёты:**
- `qc/semanticscholar/quality_report.csv` — пропуски полей, конфликты с другими источниками
- `qc/semanticscholar/errors.csv` — все аварии и ошибки извлечения

**Логи:**
- `logs/semanticscholar/*.jsonl` — структурированные логи в JSON Lines формате

## 1. Соответствие политике Semantic Scholar и режимы доступа

### Базовый доступ vs Premium

**Базовый (без API ключа):**
- 100 запросов/5 минут (ограничение по времени)
- 100K запросов/месяц на email
- Rate limit: ~0.33 requests/секунду в среднем

**Premium (с API ключом):**
- 5000 запросов/5 минут
- Без месячного лимита
- Rate limit: ~16 requests/секунду

**Enterprise:**
- Ещё больше лимитов
- Приоритет поддержка

### API Key

Получить через: https://www.semanticscholar.org/product/api

```http
GET /graph/v1/paper/{paper_id} HTTP/1.1
Host: api.semanticscholar.org
x-api-key: <your_api_key>
```

**Важно:** API key обязателен для production использования!

### User-Agent рекомендуется

```http
GET /graph/v1/paper/10.1371/journal.pone.0000000 HTTP/1.1
Host: api.semanticscholar.org
x-api-key: <key>
User-Agent: bioactivity_etl/1.0 (mailto:owner@example.org)
```

### Этикет использования

1. **Избегать request storms**: равномерно распределить запросы
2. **Кэширование**: не запрашивать повторно в течение часа
3. **Respectful**: не злоупотреблять даже с premium key
4. **Error handling**: 429 означает превышение лимита, ждать

## 2. Semantic Scholar REST API: endpoints

### GET /graph/v1/paper/{paper_id} - извлечение по ID

**Поддерживаемые IDs:**
- DOI: `10.1371/journal.pone.0000000`
- ArXiv: `arXiv:1234.5678`
- MAG: `MAG:1234567890`
- AC: `AC123456789`
- PMID: `MED:12345678`

```bash
curl "https://api.semanticscholar.org/graph/v1/paper/10.1371/journal.pone.0000000" \
  -H "x-api-key: <your_key>"
```

### GET /graph/v1/paper/batch - batch request

**⚠️ Критично:** batch endpoint ускоренно расходует лимиты!

```bash
curl -X POST "https://api.semanticscholar.org/graph/v1/paper/batch" \
  -H "x-api-key: <your_key>" \
  -H "Content-Type: application/json" \
  -d '{
    "ids": [
      "10.1371/journal.pone.0000000",
      "PMID:12345678"
    ]
  }'
```

**Рекомендация:** использовать batch только если:
- Small number of papers
- Already cached DOI → paperId mappings
- Need to minimize API calls

### Search API (опционально)

```bash
curl "https://api.semanticscholar.org/graph/v1/paper/search?query=prostaglandin&limit=10" \
  -H "x-api-key: <your_key>"
```

Используется только для title-based fallback.

## 3. Рейт-лимитинг и стратегия запросов

### Rate Limits

**Без API ключа:**
- 100 requests / 5 minutes = ~0.33 req/sec
- **Реальность**: перенапряжение → бан

**С API ключом:**
- 5000 requests / 5 minutes = ~16 req/sec
- **Реальность**: 10 req/sec безопасно

### Наша стратегия (консервативная)

- **По умолчанию**: 0.8 requests/секунду (1.25s между запросами)
- **С API ключом**: 10 requests/секунду с burst до 15
- **Workers**: 1-2 параллельных потока (очень консервативно)
- **Timeout**: 30s

### Exponential Backoff

```python
def handle_rate_limit(attempt, error_response):
    """Smart backoff для Semantic Scholar"""
    if 'x-ratelimit-remaining' in error_response.headers:
        remaining = int(error_response.headers['x-ratelimit-remaining'])
        if remaining == 0:
            # Полностью исчерпан лимит, ждём reset
            wait_time = 300  # 5 минут
        else:
            # Есть остаток, exponential backoff
            wait_time = min(60 * (2 ** attempt), 300)
    else:
        # Fallback
        wait_time = 60 * (2 ** attempt)
    
    jitter = random.uniform(0, wait_time * 0.1)
    time.sleep(wait_time + jitter)
```

### Access Denied Handling

Semantic Scholar может вернуть 403 Access Denied для:
- Неавторизованных запросов (без API ключа и лимит исчерпан)
- Exceeded quota
- IP-based blocking

```python
def is_access_denied_error(error_msg):
    """Определить access denied"""
    denied_keywords = [
        "access denied",
        "quota exceeded",
        "rate limit exceeded",
        "unauthorized"
    ]
    error_lower = error_msg.lower()
    return any(keyword in error_lower for keyword in denied_keywords)
```

## 4. Извлекаемые поля и структура данных

### Основные метаданные

```json
{
  "paperId": "1234567890abcdef",
  "externalIds": {
    "DOI": "10.1371/journal.pone.0000000",
    "ArXiv": "1234.5678",
    "MAG": "1234567890",
    "ACL": "AC123456789",
    "PMID": "12345678",
    "PubMed": "12345678"
  },
  "corpusId": 123456789,
  "url": "https://www.semanticscholar.org/paper/...",
  "title": "Article Title Here",
  "abstract": "Abstract text...",
  "venue": "PLoS ONE",
  "publicationVenue": {
    "id": "venue_id",
    "name": "PLoS ONE",
    "type": "journal"
  },
  "year": 2023,
  "publicationDate": "2023-03-15",
  "referenceCount": 45,
  "citationCount": 120,
  "influentialCitationCount": 5,
  "isOpenAccess": true,
  "openAccessPdf": {
    "url": "https://journals.plos.org/...",
    "status": "gold"
  }
}
```

### Авторы

```json
{
  "authors": [
    {
      "authorId": "A1234567890",
      "name": "John Doe",
      "externalIds": {
        "ORCID": "0000-0001-2345-6789"
      }
    }
  ]
}
```

### Publication Types

```json
{
  "publicationTypes": [
    "JournalArticle",
    "Review"
  ]
}
```

Поддерживаемые типы:
- JournalArticle, Review, Conference, WorkshopPaper, BookChapter, Book, etc.

### Fields of Study

```json
{
  "fieldsOfStudy": [
    "Medicine",
    "Biology"
  ]
}
```

### Citation Context (опционально)

```json
{
  "citationContexts": [
    {
      "context": "Previous research shows...",
      "citingPaperId": "...",
      "intents": ["background", "method"]
    }
  ]
}
```

Доступно только с `citationContext=true` в запросе.

## 5. Парсинг и нормализация

### External IDs нормализация

```python
def extract_external_ids(record):
    """Извлечь все external IDs с нормализацией"""
    external_ids = record.get('externalIds', {})
    
    return {
        'doi': normalize_doi(external_ids.get('DOI')),
        'pmid': normalize_pmid(external_ids.get('PMID')),
        'pmid_alt': normalize_pmid(external_ids.get('PubMed')),
        'arxiv_id': external_ids.get('ArXiv'),
        'mag_id': external_ids.get('MAG'),
        'acl_id': external_ids.get('ACL')
    }
```

### Citation Metrics

```python
def extract_citation_metrics(record):
    """Извлечь citation metrics"""
    return {
        'citation_count': int(record.get('citationCount', 0)),
        'reference_count': int(record.get('referenceCount', 0)),
        'influential_citations': int(record.get('influentialCitationCount', 0))
    }
```

### Publication Types нормализация

```python
def normalize_publication_types(types_list):
    """Нормализация типов публикаций"""
    if not types_list:
        return []
    
    # Привести к lowercase
    normalized = [t.lower() for t in types_list]
    
    # Маппинг на стандартные типы
    type_mapping = {
        'journalarticle': 'Journal Article',
        'review': 'Review',
        'conference': 'Conference Paper',
        'workshoppaper': 'Workshop Paper',
        'bookchapter': 'Book Chapter',
        'book': 'Book'
    }
    
    return [type_mapping.get(t, t.title()) for t in normalized]
```

## 6. Title-based поиск (critical fallback)

### Поиск по заголовку через Search API

Semantic Scholar позволяет искать по заголовку:

```python
def search_by_title(title, max_results=10):
    """Поиск по заголовку"""
    
    # URL encode title
    encoded_title = urllib.parse.quote(f'"{title}"', safe='')
    
    url = f"https://api.semanticscholar.org/graph/v1/paper/search"
    params = {
        'query': title,
        'limit': max_results,
        'fields': 'paperId,title,externalIds,year,venue'
    }
    
    response = session.get(url, params=params, timeout=30)
    results = response.json().get('data', [])
    
    return results
```

### Scoring и выбор лучшего совпадения

```python
def score_title_match(result_title, search_title):
    """Jaccard similarity на словах"""
    result_words = set(normalize_title(result_title).lower().split())
    search_words = set(normalize_title(search_title).lower().split())
    
    intersection = len(result_words & search_words)
    union = len(result_words | search_words)
    
    return intersection / union if union > 0 else 0.0
```

**Применение:**
1. Search by title
2. Score each result
3. Select best match with score > 0.85
4. Validate year match if available
5. Use as fallback

### DOI-based resolution strategy

Когда есть DOI, но нет paperId:

```python
def resolve_doi_to_paper(doi):
    """Multi-step DOI resolution"""
    
    # Step 1: Прямой запрос по DOI
    try:
        paper = fetch_by_id(doi)
        if paper:
            return paper
    except NotFoundError:
        pass
    
    # Step 2: Если нет, попробовать через title (если title известен)
    if title:
        search_results = search_by_title(title)
        for result in search_results:
            result_doi = extract_doi_from_ids(result.get('externalIds', {}))
            if result_doi == doi:
                return fetch_by_id(result['paperId'])
    
    return None
```

## 7. Детерминизм и сортировка

### Сортировка выгрузки

1. По `year` DESC (сначала новые)
2. По `citationCount` DESC (популярные)
3. По `paperId` ASC (для стабильности)

### Hash вычисление

```python
def compute_hashes(record):
    """Hash для детерминизма"""
    
    # Business key = primary identifier
    biz_key = (
        record.get('externalIds', {}).get('DOI') or
        record.get('externalIds', {}).get('PMID') or
        record.get('paperId')
    )
    
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

## 8. Обработка ошибок и graceful degradation

### 403 Access Denied

```python
def handle_access_denied(error_response, attempt):
    """Обработка access denied"""
    
    if is_access_denied_error(str(error_response)):
        logger.warning(
            "semantic_scholar_access_denied",
            attempt=attempt,
            error=str(error_response)
        )
        
        # Waiting period before retry
        wait_time = 300  # 5 minutes minimum
        time.sleep(wait_time)
        
        # Retry only once
        if attempt < 2:
            return True
    
    return False
```

### 429 Rate Limiting

```python
def handle_semantic_scholar_rate_limit(error_response):
    """Специфичная обработка для Semantic Scholar"""
    
    # Check rate limit headers
    remaining = error_response.headers.get('x-ratelimit-remaining', 0)
    reset_time = error_response.headers.get('x-ratelimit-reset')
    
    if remaining == 0 and reset_time:
        wait_until = datetime.fromtimestamp(int(reset_time))
        now = datetime.now()
        wait_seconds = (wait_until - now).total_seconds()
        
        if wait_seconds > 0:
            time.sleep(min(wait_seconds, 300))
```

### 404 Not Found

Специфично для Semantic Scholar:
- Paper не существует в базе
- DOI/PMID не распознан
- Create tombstone record
- Continue processing

### Retry Strategy

```python
def retry_with_backoff(func, max_attempts=3):
    """Specialized retry для Semantic Scholar"""
    
    for attempt in range(max_attempts):
        try:
            return func()
        except AccessDeniedError:
            if handle_access_denied(attempt):
                continue
            raise
        except RateLimitError:
            handle_semantic_scholar_rate_limit(attempt)
            if attempt < max_attempts - 1:
                continue
            raise
        except TransientError as e:
            wait = 2 ** attempt
            time.sleep(wait)
            if attempt < max_attempts - 1:
                continue
            raise
```

## 9. Конфликты и приоритеты источников

### Политика разрешения коллизий

**Для цитаций:**
- Semantic Scholar — авторитет (citation metrics)
- Не перезаписывать из других источников

**Для типов публикаций:**
- PubMed > OpenAlex > Semantic Scholar (для биомедицины)
- Semantic Scholar лучше для AI/ML конференций

**Для venues:**
- OpenAlex > Semantic Scholar > Crossref
- Semantic Scholar часто лучше для conferences

**Для авторов:**
- Crossref (ORCID) > Semantic Scholar > PubMed

## 10. Конфигурация (YAML)

```yaml
sources:
  semantic_scholar:
    enabled: true
    http:
      base_url: "https://api.semanticscholar.org"
      timeout_sec: 30.0
      retries:
        total: 3
        backoff_multiplier: 2.0
        backoff_max: 300  # 5 minutes
      identify:
        user_agent: "bioactivity_etl/1.0 (mailto:owner@example.org)"
        api_key_env: "SEMANTIC_SCHOLAR_API_KEY"  # обязательно!
    rate_limit:
      max_calls_per_sec: 0.8
      with_api_key: 10.0
      burst: 15
      workers: 1
    fallback:
      title_search_enabled: true
      similarity_threshold: 0.85
      use_search_api: true  # Search API для fallback
    caching:
      driver: "redis"
      ttl_sec: 3600  # 1 hour (Semantic Scholar обновляется часто)
```

## 11. Клиент и контракты

### Интерфейс SemanticScholarClient

```python
class SemanticScholarClient:
    def fetch_by_pmid(self, pmid: str) -> dict:
        """Извлечь по PMID"""
        
    def fetch_by_doi(self, doi: str) -> dict:
        """Извлечь по DOI"""
        
    def fetch_by_paper_id(self, paper_id: str) -> dict:
        """Извлечь по paperId"""
        
    def search_by_title(
        self, 
        title: str, 
        max_results: int = 10
    ) -> list[dict]:
        """Поиск по заголовку через Search API"""
        
    def resolve_with_fallback(
        self, 
        identifier: str, 
        title: str = None
    ) -> dict:
        """Multi-step resolution с fallback"""
```

### Гарантии клиента

1. Обязательный API key validation
2. Access denied detection
3. Rate limiting с tracking
4. Title search fallback
5. Caching (1 hour TTL)
6. Structured logging

## 12. Схемы данных

### SemanticScholarRecordSchema (Bronze)

```python
@dataclass
class SemanticScholarRecord:
    paper_id: str  # PK
    corpus_id: Optional[int]
    doi: Optional[str]
    pmid: Optional[str]
    title: str
    abstract: Optional[str]
    venue: Optional[str]
    venue_id: Optional[str]
    publication_date: Optional[str]
    year: Optional[int]
    citation_count: int
    reference_count: int
    influential_citations: int
    is_open_access: bool
    publication_types: list[str]
    fields_of_study: list[str]
    is_open_access_pdf: bool
    oa_pdf_url: Optional[str]
    url: str
    created_at: datetime
    updated_at: datetime
    hash_business_key: str
    hash_row: str
```

### SemanticScholarAuthorSchema (Silver)

```python
@dataclass
class SemanticScholarAuthor:
    paper_id: str
    author_id: str
    name: str
    orcid: Optional[str]
```

### SemanticScholarCitationSchema (Silver)

```python
@dataclass
class SemanticScholarCitation:
    paper_id: str  # citing paper
    cited_paper_id: str
    citing_paper_id: str
    intent: list[str]
    context: Optional[str]
```

## 13. Тест-план

### Unit тесты

1. **External IDs parsing:**
   - DOI, PMID, ArXiv extraction
   - Multiple PMID formats

2. **Citation metrics:**
   - Counts extraction
   - Missing values handling

3. **Title search:**
   - Search API integration
   - Scoring algorithm
   - Threshold validation

4. **Access denied detection:**
   - Error message parsing
   - Retry logic

### Integration тесты

1. **PMID resolution:**
   - Direct lookup
   - Title fallback

2. **Rate limiting:**
   - 429 handling
   - Access denied recovery

3. **Golden files:**
   - Stable outputs
   - Checksums

4. **Title fallback:**
   - No DOI/PMID scenario
   - Similarity matching

## 14. Документация

**`docs/sources/semantic-scholar-extraction.md`:**
- API спецификация
- Rate limits и access denied
- Title search fallback
- Examples

**`docs/api/semantic-scholar-client.md`:**
- Client contracts
- Error handling
- Access denied recovery

**`docs/examples/semantic-scholar-usage.md`:**
- Basic lookup
- Title fallback strategy
- Batch operations (осторожно!)

## 15. CLI интерфейс

```bash
python -m scripts/get_semantic_scholar.py \
  --input data/input/identifiers.csv \
  --config configs/config_semantic_scholar.yaml \
  --api-key-env SEMANTIC_SCHOLAR_API_KEY \
  --fallback-title \
  --save-raw landing/semanticscholar_$(date +%Y%m%d).json \
  --out parquet:data/bronze/semanticscholar_records.parquet
```

### Опции

- `--mode {pmid,doi,paper_id,title}` — режим поиска
- `--rps 0.8` — requests per second
- `--api-key-env SEMANTIC_SCHOLAR_API_KEY` — **обязательно!**
- `--fallback-title` — включить title search fallback
- `--similarity-threshold 0.85` — порог для matching

## 16. QC и лицензирование

### Quality checks

- Missing API key (should fail fast)
- Access denied incidents
- Low title similarity scores (< threshold)
- Citation count anomalies
- Invalid publication types

### Лицензирование

⚠️ **Важно:** Semantic Scholar Metadata License

- Non-commercial use OK
- Commercial use requires special agreement
- Attribution required: "Data from Semantic Scholar"
- Terms: https://www.semanticscholar.org/product/api/api-terms-of-use

**В production:** проконсультироваться с юристами!

## 17. Что именно улучшено

1. ✅ Title-based поиск через Search API (критический fallback)
2. ✅ Access Denied handling (специфично для Semantic Scholar)
3. ✅ Citation metrics (уникальные данные)
4. ✅ Influence metrics (influentialCitationCount)
5. ✅ Fields of Study извлечение
6. ✅ Open Access PDF links
7. ✅ Multi-step DOI resolution strategy
8. ✅ Conservative rate limiting (0.8 req/sec без key, 10 с key)

## 18. Известные ограничения

### API Quirks

1. **Batch endpoint расходует лимиты быстро** — использовать осторожно
2. **Access denied без предупреждения** — может банить внезапно
3. **Title search не всегда точный** — нужен validation
4. **citationContexts опциональный** — не всегда доступен
5. **Rate limits жёсткие** — лучше быть консервативным

### Рекомендации для production

- Обязательно использовать API key
- Не использовать batch endpoint
- Aggressive caching (1 hour minimum)
- Exponential backoff при любых ошибках
- Логировать access denied incidents
- Title fallback как last resort
- Юридическая консультация для commercial use

