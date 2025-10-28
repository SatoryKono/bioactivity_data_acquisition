# 3. Извлечение данных (UnifiedAPIClient)

## Обзор

UnifiedAPIClient — универсальный клиент для работы с внешними API, объединяющий:

- **TTL-кэш** для тяжелых источников (ChEMBL_data_acquisition6)

- **Circuit breaker** для защиты от каскадных ошибок (bioactivity_data_acquisition5)

- **Fallback manager** со стратегиями отката (bioactivity_data_acquisition5)

- **Token bucket rate limiter** с jitter (ChEMBL_data_acquisition6)

- **Exponential backoff** с giveup условиями (оба проекта)

## Архитектура

```text
UnifiedAPIClient
├── Cache Layer (опционально)
│   └── TTLCache (thread-safe, cachetools)
├── Circuit Breaker Layer
│   └── CircuitBreaker (half-open state, timeout tracking)
├── Fallback Layer
│   └── FallbackManager (strategies: network error, timeout, 5xx)
├── Rate Limiting Layer
│   └── TokenBucketLimiter (with jitter, per-API)
├── Retry Layer
│   └── RetryPolicy (exponential backoff, giveup conditions)
└── Request Layer
    ├── Session management
    ├── Response parsing (JSON/XML)
    └── Pagination handling

```

## Компоненты

### 1. APIConfig (dataclass)

Конфигурация клиента:

```python
@dataclass
class APIConfig:
    """Конфигурация API клиента."""

    name: str  # Имя API (chembl, pubmed, etc.)

    base_url: str
    headers: dict[str, str] = field(default_factory=dict)

    # Cache

    cache_enabled: bool = False
    cache_ttl: int = 3600  # секунды

    cache_maxsize: int = 1024

    # Rate limiting

    rate_limit_max_calls: int = 1
    rate_limit_period: float = 1.0  # секунды

    rate_limit_jitter: bool = True

    # Retry

    retry_total: int = 3
    retry_backoff_factor: float = 2.0
    retry_giveup_on: list[type[Exception]] = field(default_factory=lambda: [])

    # Timeout

    timeout_connect: float = 10.0
    timeout_read: float = 30.0

    # Circuit breaker

    cb_failure_threshold: int = 5
    cb_timeout: float = 60.0

    # Fallback

    fallback_enabled: bool = True
    fallback_strategies: list[str] = field(default_factory=lambda: ["network", "timeout"])

```

### 2. CircuitBreaker

Защита от каскадных ошибок:

```python
class CircuitBreaker:
    """Circuit breaker для защиты API."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        timeout: float = 60.0
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time: float | None = None
        self.state = "closed"  # closed, open, half-open

    def call(self, func):
        """Выполняет func с circuit breaker."""
        if self.state == "open":
            if time.time() - (self.last_failure_time or 0) > self.timeout:
                self.state = "half-open"
            else:
                raise CircuitBreakerOpenError(f"Circuit breaker for {self.name} is open")

        try:
            result = func()
            if self.state == "half-open":
                self.state = "closed"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = "open"

            raise

```

### 3. TokenBucketLimiter

Rate limiting с jitter:

```python
class TokenBucketLimiter:
    """Token bucket rate limiter с jitter."""

    def __init__(
        self,
        max_calls: int,
        period: float,
        jitter: bool = True
    ):
        self.max_calls = max_calls
        self.period = period
        self.jitter = jitter

        self.tokens = max_calls
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        """Ожидает и получает token."""
        with self.lock:
            self._refill()

            if self.tokens >= 1:
                self.tokens -= 1

                if self.jitter:
                    # Добавляем случайную задержку до 10% от периода

                    jitter = random.uniform(0, self.period * 0.1)
                    time.sleep(jitter)
            else:
                # Вычисляем время ожидания

                wait_time = self.period - (time.monotonic() - self.last_refill)
                if wait_time > 0:
                    time.sleep(wait_time)
                    self._refill()
                    self.tokens -= 1

    def _refill(self):
        """Пополняет bucket."""
        now = time.monotonic()
        elapsed = now - self.last_refill

        if elapsed >= self.period:
            self.tokens = self.max_calls
            self.last_refill = now

```

### 4. RetryPolicy

Политика повторов с giveup:

```python
class RetryPolicy:
    """Политика повторов с giveup условиями."""

    def __init__(
        self,
        total: int = 3,
        backoff_factor: float = 2.0,
        giveup_on: list[type[Exception]] = None
    ):
        self.total = total
        self.backoff_factor = backoff_factor
        self.giveup_on = giveup_on or []

    def should_giveup(self, exc: Exception, attempt: int) -> bool:
        """Определяет, нужно ли прекратить попытки."""
        # Прекращаем если достигли лимита

        if attempt >= self.total:
            return True

        # Прекращаем если исключение в списке giveup_on

        if type(exc) in self.giveup_on:
            return True

        # Специальная обработка для HTTP ошибок

        if isinstance(exc, requests.exceptions.HTTPError):
            if hasattr(exc, 'response') and exc.response:
                status_code = exc.response.status_code
                # Не прекращаем для 429 (rate limit) и 5xx

                if status_code == 429 or (500 <= status_code < 600):
                    return False
                # **Критическое**: Fail-fast на 4xx (кроме 429) - невосстановимые ошибки клиента

                elif 400 <= status_code < 500:
                    logger.error(
                        "Client error, giving up",
                        code=status_code,
                        attempt=attempt,
                        message=str(exc)
                    )
                    return True

        # По умолчанию продолжаем

        return False

    def get_wait_time(self, attempt: int) -> float:
        """Вычисляет время ожидания для attempt."""
        return self.backoff_factor ** attempt

```

### 5. FallbackManager

Управление fallback стратегиями:

```python
class FallbackManager:
    """Управляет fallback стратегиями."""

    def __init__(self, strategies: list[str]):
        self.strategies = strategies
        self.fallback_data: dict[str, Any] = {}

    def execute_with_fallback(
        self,
        func: Callable,
        fallback_data: dict | None = None
    ) -> Any:
        """Выполняет func с fallback."""
        try:
            return func()
        except Exception as e:
            if not self.should_fallback(e):
                raise

            # Используем fallback данные

            data = fallback_data or self.get_fallback_data()

            logger.warning(
                "Using fallback data",
                error=str(e),
                strategy=self.get_strategy_for_error(e)
            )

            return data

    def should_fallback(self, exc: Exception) -> bool:
        """Определяет, нужно ли использовать fallback."""
        if isinstance(exc, requests.exceptions.ConnectionError):
            return "network" in self.strategies
        if isinstance(exc, requests.exceptions.Timeout):
            return "timeout" in self.strategies
        if isinstance(exc, requests.exceptions.HTTPError):
            if hasattr(exc, 'response') and exc.response:
                if 500 <= exc.response.status_code < 600:
                    return "5xx" in self.strategies
        return False

    def get_fallback_data(self) -> dict:
        """Возвращает пустые fallback данные."""
        return {}

```

### 6. ResponseParser

Универсальный парсер ответов:

```python
class ResponseParser:
    """Парсит ответы API."""

    def parse(self, response: requests.Response) -> dict[str, Any]:
        """Парсит response в dict."""
        content_type = response.headers.get('content-type', '').lower()

        # JSON

        if 'application/json' in content_type or 'json' in content_type:
            return response.json()

        # XML (например, ChEMBL)

        if 'application/xml' in content_type or 'xml' in content_type:
            return self._parse_xml(response.text)

        # Text (например, UniProt TSV)

        if 'text/plain' in content_type or 'text/tab-separated-values' in content_type:
            return self._parse_text(response.text)

        # По умолчанию пытаемся JSON

        try:
            return response.json()
        except ValueError:
            return {"raw": response.text}

    def _parse_xml(self, xml_text: str) -> dict:
        """Парсит XML в dict."""
        try:
            from defusedxml.ElementTree import fromstring as safe_fromstring
            root = safe_fromstring(xml_text)
            return self._xml_to_dict(root)
        except Exception as e:
            logger.error("Failed to parse XML", error=str(e))
            return {"error": str(e), "raw": xml_text}

    def _xml_to_dict(self, element) -> dict:
        """Рекурсивно конвертирует XML элемент в dict."""
        result = {}

        # Атрибуты

        if element.attrib:
            result.update(element.attrib)

        # Дочерние элементы

        for child in element:
            child_dict = self._xml_to_dict(child)
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_dict)
            else:
                result[child.tag] = child_dict

        # Текст

        if not result and element.text:
            return element.text.strip()

        return result

```

### 7. PaginationHandler

Обработка различных типов пагинации:

```python
class PaginationHandler:
    """Обрабатывает пагинацию."""

    def paginate(
        self,
        session: requests.Session,
        url: str,
        config: dict
    ) -> Iterator[dict]:
        """Возвращает генератор страниц."""

        pagination_type = config.get("type", "page")  # page, cursor, offset

        if pagination_type == "page":
            yield from self._paginate_by_page(session, url, config)
        elif pagination_type == "cursor":
            yield from self._paginate_by_cursor(session, url, config)
        elif pagination_type == "offset":
            yield from self._paginate_by_offset(session, url, config)

    def _paginate_by_page(
        self,
        session: requests.Session,
        url: str,
        config: dict
    ) -> Iterator[dict]:
        """Пагинация по номеру страницы."""
        page = 1
        max_pages = config.get("max_pages", 10)
        page_size = config.get("page_size", 100)

        while page <= max_pages:
            params = {config.get("page_param", "page"): page}
            if page_size:
                params[config.get("size_param", "page_size")] = page_size

            response = session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            yield data

            # Проверяем, есть ли еще страницы

            if not self._has_next_page(data, config):
                break

            page += 1

    def _paginate_by_cursor(
        self,
        session: requests.Session,
        url: str,
        config: dict
    ) -> Iterator[dict]:
        """Пагинация по cursor token."""
        cursor: str | None = config.get("initial_cursor")
        limit = config.get("limit", 100)

        while True:
            params = {config.get("cursor_param", "cursor"): cursor} if cursor else {}
            params[config.get("size_param", "limit")] = limit

            response = session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            yield data

            if not self._has_next_page(data, config):
                break

            cursor = data.get(config.get("next_cursor_key", "next_cursor"))

    def _paginate_by_offset(
        self,
        session: requests.Session,
        url: str,
        config: dict
    ) -> Iterator[dict]:
        """Пагинация по offset/limit."""
        offset = config.get("offset_start", 0)
        limit = config.get("limit", 100)
        max_total = config.get("max_total")

        while True:
            params = {
                config.get("offset_param", "offset"): offset,
                config.get("size_param", "limit"): limit,
            }

            response = session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            yield data

            if not self._has_next_page(data, config):
                break

            offset += limit
            if max_total is not None and offset >= max_total:
                break

    def _has_next_page(self, payload: dict, config: dict) -> bool:
        """Определяет, есть ли следующая страница."""
        strategy = config.get("type", "page")

        if strategy == "page":
            total_pages = payload.get(config.get("total_pages_key", "total_pages"))
            current_page = payload.get(config.get("page_key", "page"))
            items = payload.get(config.get("items_key", "items"), [])
            return bool(items) and (
                total_pages is None or (current_page or 0) < total_pages
            )

        if strategy == "cursor":
            has_more = payload.get(config.get("has_more_key", "has_more"))
            next_cursor = payload.get(config.get("next_cursor_key", "next_cursor"))
            items = payload.get(config.get("items_key", "items"), [])
            return bool(items) and (has_more is True or next_cursor)

        if strategy == "offset":
            items = payload.get(config.get("items_key", "items"), [])
            total = payload.get(config.get("total_key", "total"))
            next_offset = payload.get(config.get("next_offset_key", "offset"))
            if total is not None and next_offset is not None:
                return next_offset < total
            return len(items) == config.get("limit", 100)

        return False

```

## Основной класс: UnifiedAPIClient

```python
class UnifiedAPIClient:
    """Универсальный API клиент."""

    def __init__(self, config: APIConfig):
        self.config = config

        # Инициализация подсистем

        self.cache = TTLCache(
            maxsize=config.cache_maxsize,
            ttl=config.cache_ttl
        ) if config.cache_enabled else None

        self.circuit_breaker = CircuitBreaker(
            name=config.name,
            failure_threshold=config.cb_failure_threshold,
            timeout=config.cb_timeout
        )

        self.rate_limiter = TokenBucketLimiter(
            max_calls=config.rate_limit_max_calls,
            period=config.rate_limit_period,
            jitter=config.rate_limit_jitter
        )

        self.retry_policy = RetryPolicy(
            total=config.retry_total,
            backoff_factor=config.retry_backoff_factor,
            giveup_on=config.retry_giveup_on
        )

        self.fallback_manager = FallbackManager(
            strategies=config.fallback_strategies
        ) if config.fallback_enabled else None

        self.parser = ResponseParser()

        # Session

        self.session = requests.Session()
        self.session.headers.update(config.headers)

    def request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> dict:
        """Выполняет HTTP запрос."""

        # Circuit breaker + Retry

        def _make_request():
            url = urljoin(self.config.base_url, endpoint)

            # Rate limiting перед каждой попыткой

            self.rate_limiter.acquire()

            response = self.session.request(
                method,
                url,
                timeout=(self.config.timeout_connect, self.config.timeout_read),
                **kwargs
            )

            # Обработка специальных статусов

            if response.status_code == 429:  # Rate limit

                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    wait_time = min(int(retry_after), 60)
                    logger.warning(
                        "Rate limited by API",
                        code=429,
                        retry_after=wait_time,
                        attempt=attempt,
                        endpoint=endpoint
                    )
                    time.sleep(wait_time)
                raise RateLimitError("Rate limited")

            response.raise_for_status()
            return response

        # Circuit breaker + Retry с backoff

        attempt = 0
        while True:
            try:
                response = self.circuit_breaker.call(_make_request)
                return self.parser.parse(response)
            except Exception as e:
                attempt += 1

                if self.retry_policy.should_giveup(e, attempt):
                    # Fallback

                    if self.fallback_manager:
                        return self.fallback_manager.execute_with_fallback(
                            lambda: None  # Уже провалилось

                        )
                    raise

                # Backoff

                wait_time = self.retry_policy.get_wait_time(attempt)
                time.sleep(wait_time)

```

### Cache policy

UnifiedAPIClient разделяет ответственность кэширования на два уровня:

1. **In-memory TTLCache** — run-scoped. Очищается при завершении пайплайна.

2. **Persistent cache** — release-scoped. Ключи include `chembl_release`/`pipeline_version`.

```python
def _cache_key(self, endpoint: str, params: dict) -> str:
    """Формирует release-scoped ключ."""
    release = params.get("chembl_release") or self.config.headers.get("X-Source-Release")
    payload = {
        "endpoint": endpoint,
        "params": params,
        "release": release,
    }
    return canonical_hash(payload)

def get_with_cache(self, endpoint: str, *, params: dict | None = None) -> dict:
    """Выполняет запрос с warm-up и инвалидацией."""
    params = params or {}
    key = self._cache_key(endpoint, params)

    if self.cache and key in self.cache:
        return self.cache[key]

    response = self.get(endpoint, params=params)

    if self.cache:
        self.cache[key] = response

    return response

```

**Warm-up:** допускается прогрев популярных ключей при старте (например, `status` endpoints).

**Invalidation:**

- При смене `chembl_release`/`pipeline_version` сбрасываем persistent cache (новый namespace).

- Принудительное очищение через CLI флаг `--cache-clear` добавляет `run_id` в namespace.

**TTL ответственность:** значения TTL читаются из `config.cache_ttl`; истёкший ключ удаляется при обращении. Для критичных

источников (ChEMBL activities) дополнительно проверяем release-маркер и drop cache, если API вернул новый `release`.

```python
    def get(self, endpoint: str, **kwargs) -> dict:
        """GET запрос с автоматическим переключением на POST при длинных URL."""
        params = kwargs.get("params") or {}

        # Проверка длины URL

        url = urljoin(self.config.base_url, endpoint)
        full_url = requests.Request("GET", url, params=params).prepare().url

        max_url_length = getattr(self.config, 'max_url_length', 2000)
        if len(full_url) > max_url_length:
            # Переключаемся на POST с X-HTTP-Method-Override

            logger.info(
                "URL too long, switching to POST",
                method="GET->POST",
                endpoint=endpoint,
                url_length=len(full_url),
                max_length=max_url_length
            )
            headers = kwargs.pop("headers", {})
            headers["X-HTTP-Method-Override"] = "GET"
            return self.request("POST", endpoint, data=params, headers=headers, **kwargs)

        return self.request("GET", endpoint, **kwargs)

    def post(self, endpoint: str, data: dict, **kwargs) -> dict:
        """POST запрос."""
        return self.request("POST", endpoint, json=data, **kwargs)

```

## Конфигурации для разных API

### ChEMBL

```python
chembl_config = APIConfig(
    name="chembl",
    base_url="https://www.ebi.ac.uk/chembl/api/data",
    headers={"Accept": "application/json"},
    cache_enabled=True,
    cache_ttl=3600,
    cache_maxsize=2048,
    rate_limit_max_calls=20,
    rate_limit_period=1.0,
    timeout_connect=5.0,
    timeout_read=90.0
)

```

### PubMed

```python
pubmed_config = APIConfig(
    name="pubmed",
    base_url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
    rate_limit_max_calls=2,
    rate_limit_period=1.0,
    retry_total=10,
    retry_backoff_factor=3.0,
    timeout_read=60.0,
    fallback_strategies=["timeout", "5xx"]
)

```

### Semantic Scholar

```python
semantic_scholar_config = APIConfig(
    name="semantic_scholar",
    base_url="https://api.semanticscholar.org/graph/v1/paper",
    headers={"x-api-key": "{API_KEY}"},
    rate_limit_max_calls=1,
    rate_limit_period=10.0,  # Консервативно: 1 запрос в 10 сек

    retry_total=15,
    retry_backoff_factor=5.0,
    timeout_read=60.0
)

```

### PubChem

```python
pubchem_config = APIConfig(
    name="pubchem",
    base_url="https://pubchem.ncbi.nlm.nih.gov/rest/pug",
    cache_enabled=True,
    cache_ttl=7200,  # 2 часа

    rate_limit_max_calls=5,
    rate_limit_period=1.0,
    timeout_connect=10.0,
    timeout_read=30.0
)

```

### UniProt

```python
uniprot_config = APIConfig(
    name="uniprot",
    base_url="https://rest.uniprot.org",
    cache_enabled=True,
    cache_ttl=3600,
    timeout_connect=10.0,
    timeout_read=30.0
)

```

### IUPHAR

```python
iuphar_config = APIConfig(
    name="iuphar",
    base_url="https://www.guidetopharmacology.org/DATA",
    cache_enabled=True,
    cache_ttl=3600,
    rate_limit_max_calls=10,
    rate_limit_period=1.0
)

```

## Использование

```python
from unified_client import UnifiedAPIClient, APIConfig

# Создание клиента

config = APIConfig(
    name="chembl",
    base_url="https://www.ebi.ac.uk/chembl/api/data",
    cache_enabled=True
)
client = UnifiedAPIClient(config)

# Простой запрос

data = client.get("molecule/CHEMBL25.json")

# С параметрами

data = client.get("molecule", params={"molecule_chembl_id__in": "CHEMBL25,CHEMBL26"})

```

## Error Model

Классификация ошибок и реакция пайплайна:

### Классы ошибок

```python
class APIError(Exception):
    """Базовый класс для ошибок API."""
    pass

class ClientError(APIError):
    """Ошибка клиента (4xx): неправильный запрос."""
    def __init__(self, code: int, message: str, endpoint: str):
        self.code = code
        self.message = message
        self.endpoint = endpoint

class ServerError(APIError):
    """Ошибка сервера (5xx): временная проблема."""
    def __init__(self, code: int, message: str, endpoint: str):
        self.code = code
        self.message = message
        self.endpoint = endpoint

class RateLimited(APIError):
    """Превышен лимит запросов (429)."""
    def __init__(self, retry_after: int | None = None):
        self.retry_after = retry_after

class RetryExhausted(APIError):
    """Исчерпаны все попытки повтора."""
    def __init__(self, attempt: int, last_error: Exception):
        self.attempt = attempt
        self.last_error = last_error

class PartialFailure(APIError):
    """Частичный сбой: получены не все данные."""
    def __init__(self, received: int, expected: int, page_state: str | None):
        self.received = received
        self.expected = expected
        self.page_state = page_state

```

### Поля события

Все события ошибок содержат:

- `code`: HTTP код или внутренний код ошибки

- `message`: описание ошибки

- `retry_after`: время ожидания в секундах (для 429)

- `endpoint`: URL эндпоинта

- `page_state`: состояние пагинации при ошибке

- `attempt`: номер попытки

### Таблица реакций пайплайна

| Код | Класс | Действие | Retry | Fallback |
|-----|-------|----------|-------|----------|
| 400 | ClientError | Fail-fast | Нет | Нет |
| 401 | ClientError | Fail-fast | Нет | Нет |
| 403 | ClientError | Fail-fast | Нет | Нет |
| 404 | ClientError | Fail-fast | Нет | Нет |
| 429 | RateLimited | Wait + retry | Да | Да |

| 500 | ServerError | Retry с backoff | Да | Да |
| 502 | ServerError | Retry с backoff | Да | Да |
| 503 | ServerError | Retry с backoff | Да | Да |
| Partial | PartialFailure | Log + requeue | Да | Да |

### Протокол повторной постановки (requeue) для PartialFailure

**Цель:** гарантировать дочитывание недополученных страниц без нарушения детерминизма.

**Шаги протокола:**

1. При выбросе `PartialFailure` сохраняем `endpoint`, `page_state`, `expected` и `received` в run-scoped `retry_queue`.
2. После основной пагинации обрабатываем `retry_queue` FIFO, повторно вызывая `request()` с оригинальным `page_state`.
3. Ограничиваем `max_partial_retries` (конфиг `http.global.partial_retries.max`, по умолчанию 3) для защиты от бесконечных циклов.
4. Логи повторов включают `run_id`, `page_state`, `attempt` и `retry_origin="partial_requeue"`.

```python
from collections import deque
from dataclasses import dataclass

retry_queue: deque[RetryWorkItem] = deque()


@dataclass
class RetryWorkItem:
    endpoint: str
    params: dict
    attempt: int = 0


def requeue_partial(endpoint: str, params: dict) -> None:
    """Помещает PartialFailure в очередь повторной обработки."""
    retry_queue.append(RetryWorkItem(endpoint=endpoint, params=params.copy()))


def drain_partial_queue(client: UnifiedAPIClient) -> None:
    """Обрабатывает очередь частичных сбоев FIFO."""
    while retry_queue:
        item = retry_queue.popleft()
        if item.attempt >= config.http["global"].partial_retries["max"]:
            raise RetryExhausted(
                item.attempt,
                PartialFailure(0, 0, item.params.get("page_state"))
            )
        client.logger.info(
            "partial_requeue_retry",
            endpoint=item.endpoint,
            page_state=item.params.get("page_state"),
            attempt=item.attempt + 1,
            retry_origin="partial_requeue"
        )
        client.request("GET", item.endpoint, params=item.params)
        item.attempt += 1

```

**Примечание:** `params` обязан содержать исходный `page_state`, чтобы соблюсти контракт идемпотентности.

**Конфигурация:**

```yaml
http:
  global:
    partial_retries:
      max: 3  # Максимум 3 попытки для PartialFailure
      backoff_factor: 2.0
```

**Обоснование:** Предотвращает потерю данных при частичных сбоях пагинации, формализует недостающий контракт обработки ошибок, закрывает риск R3 из gap-анализа.

### Примеры логов

```json
{"level": "error", "code": 429, "message": "Rate limited", "retry_after": 60,
 "endpoint": "/api/molecule", "page_state": "page=42", "attempt": 3,
 "timestamp_utc": "2025-01-28T14:23:15.123Z"}

{"level": "error", "code": 500, "message": "Internal server error",
 "endpoint": "/api/activity", "page_state": null, "attempt": 1,
 "timestamp_utc": "2025-01-28T14:23:20.456Z"}

{"level": "warning", "code": "partial_failure", "message": "Received 950 of 1000 items",
 "received": 950, "expected": 1000, "page_state": "cursor=abc123",
 "timestamp_utc": "2025-01-28T14:23:25.789Z"}

```

## Pagination

Спецификация стратегий пагинации для внешних API.

### Стратегии пагинации по pipeline

**Унифицированная стратегия для ChEMBL pipelines (v3.0):**

| Pipeline | Стратегия | Параметр | Batch Size | URL Limit | Endpoint |
|----------|-----------|----------|------------|-----------|----------|
| Assay | Batch IDs | `assay_chembl_id__in` | 25 | 2000 | `/assay.json` |
| Testitem | Batch IDs | `molecule_chembl_id__in` | 25 | 2000 | `/molecule.json` |
| Activity | Batch IDs | `activity_id__in` | 25 | 2000 | `/activity.json` |
| Target | Batch IDs | `target_chembl_id__in` | 25 | 2000 | `/target.json` |

**Общие стратегии для других API:**

#### Page + Limit

```python
params = {"page": 1, "limit": 100}

# Ответ: {"items": [...], "page": 1, "total_pages": 10}

```

#### Cursor

```python
params = {"cursor": "abc123", "limit": 100}

# Ответ: {"items": [...], "next_cursor": "def456", "has_more": true}

```

#### Offset + Limit

```python
params = {"offset": 0, "limit": 100}

# Ответ: {"items": [...], "offset": 100, "total": 1000}

```

### Сигналы завершения

| Стратегия | Сигнал завершения |
|-----------|-------------------|
| Page + Limit | `items=[]` или `page > total_pages` |

| Cursor | `items=[]` или `next_cursor=null` или `has_more=false` |
| Offset + Limit | `received < limit` или `offset >= total` |

### Контракт идемпотентности

Одинаковый `page_state` → идентичный набор данных.

```python

# Запрос 1

params = {"page": 5}
response1 = api.get("/data", params=params)

# Запрос 2 (через 1 час)

params = {"page": 5}  # Тот же page_state

response2 = api.get("/data", params=params)

assert response1.items == response2.items  # Идемпотентность

```

Нарушения идемпотентности:

- Изменение порядка элементов между запросами

- Добавление/удаление элементов в том же наборе

- Изменение timestamp в данных (если не part of business key)

### Запрет смешивания стратегий

**Критическое правило:** Каждый запрос использует **только одну** стратегию пагинации.

**⚠️ Breaking Change (v3.0):** Все ChEMBL pipelines унифицированы на batch IDs стратегию.

**Недопустимо:**

```python

# Смешивание page и cursor

params = {"page": 1, "cursor": "abc123"}  # Ошибка! Непредсказуемое поведение

# Смешивание offset и cursor

params = {"offset": 100, "cursor": "abc123"}  # Ошибка!

# Смешивание batch IDs с другими стратегиями

params = {"assay_chembl_id__in": "CHEMBL1,CHEMBL2", "offset": 0}  # Ошибка!

```

**Допустимо (унифицированная стратегия для ChEMBL):**

```python

# Batch IDs для всех ChEMBL pipelines

params = {"activity_id__in": "123,456,789"}  # Activity

params = {"assay_chembl_id__in": "CHEMBL1,CHEMBL2"}  # Assay

params = {"molecule_chembl_id__in": "CHEMBL25,CHEMBL26"}  # Testitem

params = {"target_chembl_id__in": "CHEMBL231,CHEMBL232"}  # Target

# Однозначная стратегия - только cursor (для других API)

params = {"cursor": "abc123", "limit": 100}  # Только cursor

# Однозначная стратегия - только page

params = {"page": 1, "limit": 100}  # Только page

```

**Валидация стратегии:**

```python
def validate_pagination_params(params: dict) -> None:
    """Валидирует, что используется только одна стратегия."""
    strategies = sum([
        "offset" in params,
        "page" in params,
        "cursor" in params
    ])

    if strategies > 1:
        raise ValueError(f"Multiple pagination strategies detected: {params}")

```

**См. также**: [gaps.md](../gaps.md) (G2), [06-activity-data-extraction.md](06-activity-data-extraction.md).

### TTL курсора

TTL курсора — ответственность внешнего API. UnifiedAPIClient:

- Не устанавливает TTL для cursor

- Не валидирует срок действия cursor

- Логирует предупреждение при использовании истекшего cursor

## Rate Limiting и Retry-After

### Контракт Retry-After (инвариант)

**Обязательное требование**: Respect Retry-After обязателен; ожидание не меньше указанного; ретраи на 4xx запрещены (кроме 429); circuit-breaker обязателен.

**Протокол для 429**:

```python
if response.status_code == 429:
    retry_after = response.headers.get('Retry-After')
    if retry_after:
        wait = min(int(retry_after), 60)  # Cap at 60s

        logger.warning("Rate limited by API",
                      code=429,
                      retry_after=wait,
                      endpoint=endpoint,
                      attempt=attempt,
                      run_id=context.run_id)
        time.sleep(wait)
    raise RateLimitError("Rate limited")

```

**Политика ретраев**:

- 2xx, 3xx: успех, возвращаем response

- 429: respect Retry-After, ретраить

- 4xx (кроме 429): не ретраить, fail-fast

- 5xx: exponential backoff, retry

**См. также**: [gaps.md](../gaps.md) (G11), [acceptance-criteria.md](../acceptance-criteria.md) (AC5).

## Acceptance Criteria

Матрица проверяемых инвариантов для API клиента:

### AC-07: Respect Retry-After (429)

**Цель:** Гарантировать корректную обработку HTTP 429 с Retry-After заголовком.

**Тест:**

```python

# Mock 429 ответа с Retry-After: 7

response.status_code = 429
response.headers['Retry-After'] = '7'

# Запрос

result = client.get("/api/data", params={"limit": 100})

# Ожидаемое: логирование retry_after=7 и attempt

# Проверяем лог

assert "Rate limited by API" in log_output
assert "retry_after=7" in log_output
assert "attempt=1" in log_output

```

**Порог:** Время ожидания >= указанному Retry-After.

### AC-19: Fail-Fast на 4xx (кроме 429)

**Цель:** Гарантировать немедленное прекращение ретраев при клиентских ошибках.

**Тест:**

```python

# Mock 400 ответа

response.status_code = 400

# Запрос

try:
    result = client.get("/api/data", params={"invalid": "param"})
except Exception:
    pass

# Ожидаемое: только 1 попытка, не 3

assert attempt == 1
assert "Client error, giving up" in log_output

```

**Порог:** Нет ретраев на 4xx (кроме 429).

## Best Practices

1. **Включайте кэш для тяжелых API**: ChEMBL, PubChem, UniProt

2. **Настройте rate limits строго**: следование лимитам API

3. **Используйте circuit breaker**: для production окружений

4. **Настраивайте fallback**: для критичных пайплайнов

5. **Мониторьте timeout**: разные API имеют разные требования

6. **Логируйте все запросы**: для отладки и аудита

7. **Используйте POST override для длинных URL**: автоматически при превышении max_url_length

8. **Не смешивайте стратегии пагинации**: только offset, или только cursor, или только page

---

**Следующий раздел**: [04-normalization-validation.md](04-normalization-validation.md)
