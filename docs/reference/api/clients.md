# API Клиенты

## Обзор

HTTP клиенты для работы с внешними API источниками данных. Все клиенты наследуются от базового класса `BaseApiClient` и поддерживают rate limiting, retry logic и обработку ошибок.

## Базовый клиент

### BaseApiClient

Базовый класс для всех API клиентов, расположенный в `library.clients.base`.

**Основные возможности:**
- Rate limiting с token bucket алгоритмом
- Retry logic с экспоненциальной задержкой
- Circuit breaker для защиты от каскадных сбоев
- Структурированное логирование
- Graceful degradation

## ChEMBL клиенты

### ChEMBLClient

Клиент для работы с ChEMBL API, расположенный в `library.clients.chembl`.

**Основные методы:**
- `get_molecules()` — извлечение данных молекул
- `get_targets()` — извлечение данных мишеней
- `get_activities()` — извлечение данных активностей
- `get_assays()` — извлечение данных экспериментов

### TestitemChEMBLClient

Специализированный клиент для извлечения данных молекул.

## PubChem клиент

### PubChemClient

Клиент для работы с PubChem API, расположенный в `library.clients.pubchem`.

**Основные методы:**
- `search_by_smiles()` — поиск по SMILES структуре
- `get_compound_data()` — получение данных соединения
- `enrich_molecules()` — обогащение данных молекул

## Crossref клиент

### CrossrefClient

Клиент для работы с Crossref API, расположенный в `library.clients.crossref`.

**Основные методы:**
- `search_works()` — поиск публикаций
- `get_work()` — получение данных публикации
- `resolve_doi()` — разрешение DOI

## OpenAlex клиент

### OpenAlexClient

Клиент для работы с OpenAlex API, расположенный в `library.clients.openalex`.

**Основные методы:**
- `search_works()` — поиск работ
- `get_work()` — получение данных работы
- `get_author()` — получение данных автора

## PubMed клиент

### PubMedClient

Клиент для работы с PubMed API, расположенный в `library.clients.pubmed`.

**Основные методы:**
- `search_articles()` — поиск статей
- `get_article()` — получение данных статьи
- `get_abstract()` — получение аннотации

## Semantic Scholar клиент

### SemanticScholarClient

Клиент для работы с Semantic Scholar API, расположенный в `library.clients.semantic_scholar`.

**Основные методы:**
- `search_papers()` — поиск статей
- `get_paper()` — получение данных статьи
- `get_author()` — получение данных автора

## Вспомогательные компоненты

### RateLimiter

Контроль скорости запросов, расположенный в `library.clients.base`.

### CircuitBreaker

Защита от каскадных сбоев, расположенный в `library.clients.circuit_breaker`.

### FallbackManager

Управление резервными стратегиями, расположенный в `library.clients.fallback`.

## Примеры использования

### Базовое использование клиента

```python
from library.clients.chembl import ChEMBLClient
from library.config import APIClientConfig

# Создание конфигурации
config = APIClientConfig(
    base_url="https://www.ebi.ac.uk/chembl/api/data",
    timeout=60,
    retries=5,
    rate_limit=10
)

# Создание клиента
client = ChEMBLClient(config)

# Извлечение данных
data = client.get_data(endpoint="molecules", params={"limit": 100})
```

### Использование с обработкой ошибок

```python
from library.clients.exceptions import APIError, RateLimitError

try:
    data = client.get_data(endpoint="molecules")
except RateLimitError as e:
    logger.warning(f"Превышен лимит запросов: {e}")
    # Ожидание и повторная попытка
    time.sleep(e.retry_after)
    data = client.get_data(endpoint="molecules")
except APIError as e:
    logger.error(f"Ошибка API: {e}")
    raise
```

### Batch обработка

```python
# Извлечение данных батчами
batch_size = 100
all_data = []

for offset in range(0, total_count, batch_size):
    batch_data = client.get_data(
        endpoint="molecules",
        params={"limit": batch_size, "offset": offset}
    )
    all_data.extend(batch_data)
```

## Конфигурация клиентов

### Настройки HTTP

```yaml
sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout: 60
    retries: 5
    rate_limit: 10
    headers:
      User-Agent: "BioactivityDataAcquisition/1.0"
```

### Настройки retry

```yaml
retry_settings:
  max_attempts: 5
  base_delay: 1.0
  max_delay: 60.0
  exponential_base: 2.0
```

### Настройки rate limiting

```yaml
rate_limit_settings:
  requests_per_second: 10
  burst_size: 50
  window_size: 60
```

## Мониторинг и логирование

### Структурированное логирование

```python
logger.info("API request started",
           client="chembl",
           endpoint="molecules",
           params={"limit": 100})

logger.info("API request completed",
           client="chembl",
           endpoint="molecules",
           response_time=1.23,
           status_code=200,
           data_count=100)
```

### Метрики производительности

```python
metrics = {
    "requests_total": request_count,
    "requests_successful": success_count,
    "requests_failed": failure_count,
    "average_response_time": avg_response_time,
    "rate_limit_hits": rate_limit_hits
}

logger.info("Pipeline metrics", **metrics)
```
