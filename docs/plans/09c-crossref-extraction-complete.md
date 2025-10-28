# План: Извлечение метаданных из Crossref API для обогащения документов ChEMBL

## 0. Цель и артефакты

### Цель

Детерминированно извлекать метаданные научных публикаций по DOI из Crossref REST API и обогащать document-измерение ChEMBL с фокусом на журнальные метаданные, авторов и даты публикации.

### Артефакты на выходе

**Landing (сырые данные):**
- `data/landing/crossref_raw_{yyyymmdd}.jsonl` — сырые JSON записи для audit trail

**Bronze (нормализованные):**
- `data/bronze/crossref_records.parquet` — нормализованные записи, одна строка на DOI

**Silver (денормализованные справочники):**
- `data/silver/crossref_authors.parquet` — авторы с аффилиациями и ORCID
- `data/silver/crossref_references.parquet` — библиографические ссылки
- `data/silver/crossref_funders.parquet` — информация о финансировании

**Метаданные:**
- `meta/crossref/meta.yaml` — run_id, pipeline_version, row_count, hash_business_key, входы/выходы

**QC отчёты:**
- `qc/crossref/quality_report.csv` — пропуски по ключевым полям, несогласованные ISSN, конфликты с PubMed
- `qc/crossref/errors.csv` — все аварии и ошибки извлечения

**Логи:**
- `logs/crossref/*.jsonl` — структурированные логи в JSON Lines формате

## 1. Соответствие политике Crossref и режимы доступа

### Polite Pool vs Public Pool

**Public Pool (без идентификации):**
- Rate limit: ~50 запросов/секунду
- Нестабильная производительность
- Может тротлиться при высокой нагрузке

**Polite Pool (рекомендуется):**
- Добавить `mailto=` в query string или User-Agent header
- Rate limit: до 50+ запросов/секунду с приоритетом
- Стабильная производительность

**Crossref Plus (с API токеном):**
- Требует регистрацию и токен
- Дополнительные метаданные
- Увеличенные лимиты

### Обязательные параметры

```http
GET /works/{doi} HTTP/1.1
Host: api.crossref.org
User-Agent: bioactivity_etl/1.0 (mailto:owner@example.org)
```

Или через query parameter:
```
https://api.crossref.org/works/{doi}?mailto=owner@example.org
```

### Этикет и Best Practices

1. **User-Agent обязателен**: указывать название приложения + контакт
2. **Кэширование**: сохранять ответы локально, не запрашивать повторно
3. **Батчинг**: использовать `/works` endpoint для множественных DOI
4. **Backoff при 429**: exponential backoff с jitter
5. **Respect для сервиса**: не молотить API без необходимости

## 2. Crossref REST API: endpoints и возможности

### GET /works/{doi} - извлечение по одному DOI

```bash
curl "https://api.crossref.org/works/10.1371/journal.pone.0000000" \
  -H "User-Agent: bioactivity_etl/1.0 (mailto:owner@example.org)"
```

Возвращает полную запись с метаданными.

### GET /works - пакетное извлечение и фильтрация

**По списку DOI (через filter):**
```bash
curl "https://api.crossref.org/works?filter=doi:10.1371/journal.pone.0000000,10.1038/nature12345&rows=100"
```

**Пагинация:**
- `rows` — количество записей (max 1000)
- `offset` — смещение
- `cursor` — для больших выборок (рекомендуется)

### Cursor-based pagination для больших списков

```bash
# Первый запрос
curl "https://api.crossref.org/works?filter=doi:10.1371/*&rows=1000&cursor=*"

# Следующая страница (cursor из previous response)
curl "https://api.crossref.org/works?filter=doi:10.1371/*&rows=1000&cursor=AoJ/..."
```

### Content Negotiation

Crossref поддерживает различные форматы через Accept headers:

- `application/json` — стандартный JSON (по умолчанию)
- `application/vnd.crossref.unixsd+xml` — Unixref XML
- `text/x-bibliography` — форматированная библиография
- `application/vnd.citationstyles.csl+json` — CSL JSON

**Для ETL используем JSON** как наиболее структурированный.

## 3. Рейт-лимитинг и стратегия запросов

### Rate Limits

**Public Pool:**
- ~50 запросов/секунду
- Может тротлиться

**Polite Pool:**
- 50+ запросов/секунду с приоритетом
- Стабильная производительность

**Crossref Plus:**
- Увеличенные лимиты (индивидуально)

### Наша стратегия (консервативная)

- **По умолчанию**: 2 запроса/секунду (polite pool)
- **С burst capacity**: до 5 запросов в burst
- **Workers**: 2-4 параллельных потока
- **Timeout**: connect 10s, read 30s

### Обработка 429 Too Many Requests

```python
def handle_rate_limit(response, attempt):
    """Exponential backoff с jitter"""
    if response.status_code == 429:
        retry_after = response.headers.get('Retry-After', 60)
        wait_time = min(int(retry_after) * (2 ** attempt), 300)
        jitter = random.uniform(0, wait_time * 0.1)
        time.sleep(wait_time + jitter)
        return True
    return False
```

### Батчинг стратегия

1. **Малые списки (< 100 DOI)**: индивидуальные запросы `/works/{doi}`
2. **Средние списки (100-1000)**: `/works?filter=doi:...` с одним запросом
3. **Большие списки (> 1000)**: cursor pagination с батчами по 1000

## 4. Извлекаемые поля и структура данных

### Основные метаданные (всегда доступны)

```json
{
  "DOI": "10.1371/journal.pone.0000000",
  "type": "journal-article",
  "title": ["Article Title Here"],
  "container-title": ["PLoS ONE"],
  "published-print": {"date-parts": [[2023, 3, 15]]},
  "published-online": {"date-parts": [[2023, 3, 1]]},
  "volume": "18",
  "issue": "3",
  "page": "e0000000",
  "ISSN": ["1932-6203"],
  "publisher": "Public Library of Science",
  "member": "340"
}
```

### Авторы и аффилиации

```json
{
  "author": [
    {
      "given": "John",
      "family": "Doe",
      "sequence": "first",
      "affiliation": [
        {
          "name": "Department of Biology, University Example"
        }
      ],
      "ORCID": "https://orcid.org/0000-0001-2345-6789"
    }
  ]
}
```

### Даты публикации (приоритеты)

**Порядок предпочтения:**
1. `published-print.date-parts` — дата печатной версии (приоритет)
2. `published-online.date-parts` — дата онлайн версии
3. `issued.date-parts` — общая дата публикации
4. `created.date-parts` — дата создания записи в Crossref

### Типы публикаций

```python
CROSSREF_TYPES = [
    "journal-article",      # Статья в журнале
    "book-chapter",         # Глава книги
    "posted-content",       # Preprint
    "proceedings-article",  # Статья конференции
    "monograph",           # Монография
    "reference-entry",     # Справочная запись
    "book",                # Книга
    "report",              # Отчёт
    "dataset",             # Датасет
    "component",           # Компонент
    "peer-review"          # Рецензия
]
```

### Subtype для детализации

```json
{
  "type": "posted-content",
  "subtype": "preprint"
}
```

### Ссылки и идентификаторы

```json
{
  "link": [
    {
      "URL": "https://journals.plos.org/plosone/article?id=10.1371/...",
      "content-type": "unspecified",
      "content-version": "vor",
      "intended-application": "text-mining"
    }
  ],
  "relation": {
    "is-preprint-of": [...],
    "has-review": [...]
  }
}
```

### Лицензии

```json
{
  "license": [
    {
      "URL": "https://creativecommons.org/licenses/by/4.0/",
      "start": {"date-parts": [[2023, 3, 15]]},
      "delay-in-days": 0,
      "content-version": "vor"
    }
  ]
}
```

### Финансирование (опционально)

```json
{
  "funder": [
    {
      "DOI": "10.13039/100000001",
      "name": "National Science Foundation",
      "award": ["1234567"]
    }
  ]
}
```

### Subject/тематика

```json
{
  "subject": [
    "Medicine",
    "Biology"
  ]
}
```

## 5. Парсинг и нормализация

### DOI нормализация

```python
def normalize_crossref_doi(doi_value):
    """Нормализация DOI из Crossref"""
    if not doi_value:
        return None
    
    doi = str(doi_value).strip().lower()
    
    # Удалить префикс URL если есть
    if doi.startswith('https://doi.org/'):
        doi = doi[16:]
    elif doi.startswith('http://dx.doi.org/'):
        doi = doi[18:]
    
    # Валидация формата
    if not re.match(r'^10\.\d{4,}/[^\s]+$', doi):
        return None
    
    return doi
```

### Даты нормализация (приоритетная логика)

```python
def extract_publication_date(record):
    """Извлечь дату с приоритетами"""
    date_fields = [
        'published-print',
        'published-online',
        'issued',
        'created'
    ]
    
    for field in date_fields:
        if field in record and 'date-parts' in record[field]:
            parts = record[field]['date-parts'][0]
            year = parts[0] if len(parts) > 0 else None
            month = parts[1] if len(parts) > 1 else None
            day = parts[2] if len(parts) > 2 else None
            
            if year:
                return {
                    'year': year,
                    'month': month or 0,
                    'day': day or 0,
                    'iso': format_iso_date(year, month, day),
                    'source': field
                }
    
    return None
```

### Авторы нормализация

```python
def normalize_authors(author_list):
    """Нормализация списка авторов"""
    authors = []
    
    for idx, author in enumerate(author_list):
        normalized = {
            'position': idx + 1,
            'family_name': author.get('family', ''),
            'given_name': author.get('given', ''),
            'sequence': author.get('sequence', 'additional'),
            'orcid': normalize_orcid(author.get('ORCID')),
            'affiliations': [
                aff.get('name', '') 
                for aff in author.get('affiliation', [])
            ]
        }
        authors.append(normalized)
    
    return authors
```

### ORCID нормализация

```python
def normalize_orcid(orcid_value):
    """Извлечь чистый ORCID"""
    if not orcid_value:
        return None
    
    # Убрать URL префикс
    orcid = orcid_value.replace('https://orcid.org/', '')
    orcid = orcid.replace('http://orcid.org/', '')
    
    # Валидация формата XXXX-XXXX-XXXX-XXXX
    if re.match(r'^\d{4}-\d{4}-\d{4}-\d{3}[0-9X]$', orcid):
        return orcid
    
    return None
```

### Title нормализация

```python
def normalize_title(title_array):
    """Извлечь и нормализовать заголовок"""
    if not title_array or len(title_array) == 0:
        return None
    
    # Crossref возвращает title как массив
    title = title_array[0]
    
    # Trim whitespace
    title = ' '.join(title.split())
    
    # Unicode normalization (NFC)
    title = unicodedata.normalize('NFC', title)
    
    return title
```

### ISSN нормализация (Print vs Electronic)

```python
def extract_issn(record):
    """Извлечь ISSN с типом"""
    issn_data = {}
    
    if 'ISSN' in record:
        issn_list = record['ISSN']
        
        # Обычно первый - print, второй - electronic
        if len(issn_list) > 0:
            issn_data['issn_print'] = issn_list[0]
        if len(issn_list) > 1:
            issn_data['issn_electronic'] = issn_list[1]
    
    # Альтернативно из issn-type массива
    if 'issn-type' in record:
        for issn_obj in record['issn-type']:
            issn_type = issn_obj.get('type')
            issn_value = issn_obj.get('value')
            
            if issn_type == 'print':
                issn_data['issn_print'] = issn_value
            elif issn_type == 'electronic':
                issn_data['issn_electronic'] = issn_value
    
    return issn_data
```

## 6. Детерминизм и сортировка

### Сортировка выгрузки

1. По `DOI` ASC (лексикографическая)
2. По `indexed.date-time` DESC (когда обновлено в Crossref)

### Hash вычисление

```python
def compute_hashes(record):
    """Вычисление hash для детерминизма"""
    # Business key = DOI
    hash_business_key = hashlib.sha1(
        record['DOI'].encode('utf-8')
    ).hexdigest()
    
    # Row hash = все нормализованные поля
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

## 7. Обработка ошибок и graceful degradation

### 404 Not Found

DOI не существует в Crossref:
- Создать tombstone запись: `{doi, error="NotFound", checked_at}`
- Продолжить обработку
- Попробовать альтернативные источники (OpenAlex, DataCite)

### 400 Bad Request

Некорректный формат DOI:
- Валидировать DOI перед запросом
- Логировать и пропустить
- Сохранить в `qc/crossref/invalid_dois.csv`

### 503 Service Unavailable

Сервис временно недоступен:
- Retry с exponential backoff (max 5 попыток)
- Если не удалось — отложить батч
- Записать в error log

### Rate Limiting (429)

```python
def handle_rate_limit_with_backoff(response, attempt, max_attempts=5):
    """Обработка 429 с экспоненциальным backoff"""
    if attempt >= max_attempts:
        raise MaxRetriesExceeded()
    
    retry_after = int(response.headers.get('Retry-After', 60))
    wait_time = min(retry_after * (2 ** attempt), 300)
    jitter = random.uniform(0, wait_time * 0.1)
    
    logger.warning(
        "rate_limit_hit",
        attempt=attempt,
        wait_time=wait_time,
        retry_after=retry_after
    )
    
    time.sleep(wait_time + jitter)
```

### Connection Errors

```python
def retry_with_timeout(func, max_attempts=3):
    """Retry логика для connection errors"""
    for attempt in range(max_attempts):
        try:
            return func()
        except (Timeout, ConnectionError) as e:
            if attempt == max_attempts - 1:
                raise
            
            wait = 2 ** attempt
            logger.warning(
                "connection_error",
                attempt=attempt,
                error=str(e),
                wait=wait
            )
            time.sleep(wait)
```

## 8. Конфликты и приоритеты источников

### Политика разрешения коллизий

При конфликтах между источниками:

**Для DOI метаданных:**
- Crossref — авторитетный источник для DOI (это регистратор)
- Приоритет для: journal name, ISSN, publisher, dates

**Для заголовков:**
- PubMed > Crossref (для биомедицины, лучше качество)
- Crossref fallback для небиомедицинских

**Для авторов:**
- Crossref (полнее, с ORCID)
- PubMed fallback

**Для дат:**
- Приоритет: PubMed completed date > Crossref published-print > Crossref published-online

## 9. Конфигурация (YAML)

```yaml
sources:
  crossref:
    enabled: true
    http:
      base_url: "https://api.crossref.org"
      timeout_connect: 10.0
      timeout_read: 30.0
      retries:
        total: 3
        backoff_multiplier: 2.0
        backoff_max: 60.0
      identify:
        mailto: "owner@example.org"
        user_agent: "bioactivity_etl/1.0"
        plus_token_env: "CROSSREF_PLUS_TOKEN"  # опционально
    batching:
      dois_per_request: 100
      use_cursor: true  # для больших списков
      rows_per_page: 1000
    rate_limit:
      max_calls_per_sec: 2
      burst: 5
      workers: 2
    caching:
      driver: "redis"
      ttl_sec: 86400
    polite_pool:
      enabled: true  # использовать polite pool
```

## 10. Клиент и контракты

### Интерфейс CrossrefClient

```python
class CrossrefClient:
    def fetch_by_doi(self, doi: str) -> dict:
        """Извлечь одну запись по DOI"""
        
    def fetch_by_dois_batch(
        self, 
        dois: list[str], 
        batch_size: int = 100
    ) -> dict[str, dict]:
        """Извлечь множество записей"""
        
    def fetch_with_cursor(
        self, 
        filter_query: str, 
        rows: int = 1000
    ) -> Iterator[dict]:
        """Cursor-based pagination для больших списков"""
        
    def check_doi_exists(self, doi: str) -> bool:
        """Проверить существование DOI"""
```

### Гарантии клиента

1. Автоматическое добавление `mailto` в requests
2. Rate limiting с token bucket
3. Retry logic для transient errors
4. Сохранение сырых ответов в `landing/`
5. Structured logging всех операций

## 10. Схемы данных

### CrossrefRecordSchema (Bronze)

```python
@dataclass
class CrossrefRecord:
    doi: str  # PK
    type: str
    subtype: Optional[str]
    title: str
    container_title: Optional[str]
    publisher: str
    issn_print: Optional[str]
    issn_electronic: Optional[str]
    volume: Optional[str]
    issue: Optional[str]
    page: Optional[str]
    published_print_date: Optional[str]  # ISO format
    published_online_date: Optional[str]
    date_source: str  # какое поле использовано
    member: str  # Crossref member ID
    license_url: Optional[str]
    subject: list[str]
    language: Optional[str]
    references_count: int
    is_referenced_by_count: int
    indexed_date: datetime
    created_at: datetime
    updated_at: datetime
    hash_business_key: str
    hash_row: str
```

### CrossrefAuthorSchema (Silver)

```python
@dataclass
class CrossrefAuthor:
    doi: str
    position: int
    sequence: str  # first | additional
    family_name: str
    given_name: Optional[str]
    orcid: Optional[str]
    affiliations: list[str]
```

### CrossrefReferenceSchema (Silver)

```python
@dataclass
class CrossrefReference:
    doi: str  # citing DOI
    reference_key: str
    reference_doi: Optional[str]
    reference_title: Optional[str]
    reference_author: Optional[str]
    reference_year: Optional[int]
```

### CrossrefFunderSchema (Silver)

```python
@dataclass
class CrossrefFunder:
    doi: str
    funder_doi: Optional[str]
    funder_name: str
    award_numbers: list[str]
```

## 11. Тест-план

### Unit тесты

1. **DOI нормализация:**
   - URL prefixes удаление
   - Lowercase приведение
   - Валидация формата

2. **Dates парсинг:**
   - published-print, published-online, issued, created
   - Приоритеты
   - Неполные даты (только год)

3. **Authors парсинг:**
   - С ORCID и без
   - Multiple affiliations
   - Sequence ordering

4. **ISSN extraction:**
   - Из ISSN array
   - Из issn-type objects
   - Print vs Electronic

### Integration тесты

1. **Cursor pagination:**
   - Большой список DOI (>1000)
   - Проверка детерминизма порядка

2. **Rate limiting:**
   - Throttling корректность
   - Backoff при 429

3. **Golden files:**
   - Стабильные Parquet с checksums
   - Неизменность порядка колонок

4. **Error scenarios:**
   - 404, 400, 503, 429
   - Graceful degradation

## 12. Документация

### Структура документов

**`docs/sources/crossref-extraction.md`:**
- Спецификация API
- Polite pool guidelines
- Rate limiting
- Примеры запросов

**`docs/api/crossref-client.md`:**
- Контракты клиента
- Исключения
- Retry политики

**`docs/examples/crossref-usage.md`:**
- Извлечение по списку DOI
- Cursor pagination
- Обработка ошибок

## 13. CLI интерфейс

```bash
python -m scripts/get_crossref.py \
  --input data/input/dois.csv \
  --config configs/config_crossref.yaml \
  --mode batch \
  --save-raw landing/crossref_$(date +%Y%m%d).jsonl \
  --out parquet:data/bronze/crossref_records.parquet
```

### Опции

- `--mode {single, batch, cursor}` — режим извлечения
- `--rps 2` — requests per second
- `--mailto owner@example.org` — polite pool email
- `--plus-token-env CROSSREF_PLUS_TOKEN` — токен для Plus
- `--use-cursor` — использовать cursor pagination
- `--batch-size 100` — размер батча

## 14. QC и лицензирование

### Quality checks

- Пустые titles
- Несогласованные ISSN (print vs electronic)
- Конфликты дат с PubMed
- Дубликаты DOI
- Invalid ORCID форматы

### Лицензирование

⚠️ **Важно:** Crossref metadata под CC0 (public domain).

- Можно использовать свободно
- Указать источник: "Data from Crossref"
- Ссылка на Terms of Use обязательна

## 15. Что именно улучшено

1. ✅ Polite pool с mailto (приоритет обработки)
2. ✅ Cursor-based pagination для больших списков
3. ✅ Приоритеты дат (published-print > published-online > issued)
4. ✅ ISSN extraction с типами (print/electronic)
5. ✅ ORCID нормализация для авторов
6. ✅ Graceful degradation при 404/429
7. ✅ Детерминизм и hash вычисление
8. ✅ Structured logging и audit trail

