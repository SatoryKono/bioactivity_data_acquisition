# HTTP клиенты

Клиенты для работы с внешними API источниками данных.

## Базовый класс

::: library.clients.base.BaseApiClient

## Специализированные клиенты

### ChEMBL
::: library.clients.chembl.ChEMBLClient

### Crossref
::: library.clients.crossref.CrossrefClient

### OpenAlex
::: library.clients.openalex.OpenAlexClient

### PubMed
::: library.clients.pubmed.PubMedClient

### Semantic Scholar
::: library.clients.semantic_scholar.SemanticScholarClient

## Утилиты

### Rate Limiting
::: library.clients.base.RateLimiter

### Circuit Breaker
::: library.clients.circuit_breaker.APICircuitBreaker

### Fallback Strategy
::: library.clients.fallback.FallbackManager

## Примеры использования

### Создание клиента

```python
from library.clients.chembl import ChEMBLClient
from library.config import Config

config = Config.from_yaml("configs/config.yaml")
client = ChEMBLClient(config.clients[0])
```

### Получение данных

```python
# Получение данных с пагинацией
data = client.fetch_data()
```

### Обработка ошибок

```python
from library.clients.exceptions import ApiClientError, RateLimitError

try:
    data = client.fetch_data()
except RateLimitError:
    # Обработка превышения лимитов
    pass
except ApiClientError:
    # Обработка других ошибок API
    pass
```
