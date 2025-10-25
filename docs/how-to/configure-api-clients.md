# Настройка API клиентов

Руководство по настройке и конфигурации API клиентов для различных источников данных.

## Обзор

Проект поддерживает множество API источников данных. Каждый клиент требует специфической настройки для корректной работы.

## ChEMBL API

### Базовые настройки

```yaml
# configs/config.yaml
sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    timeout_sec: 60.0
    max_retries: 5
    rate_limit:
      requests_per_second: 0.2  # 3 запроса в 15 секунд
```

### Аутентификация

ChEMBL API не требует аутентификации для базовых запросов, но рекомендуется указать User-Agent:

```yaml
http:
  headers:
    User-Agent: "BioactivityDataAcquisition/1.0 (contact@example.com)"
```

## Crossref API

### Настройка

```yaml
sources:
  crossref:
    base_url: "https://api.crossref.org/works"
    timeout_sec: 30.0
    max_retries: 3
    rate_limit:
      requests_per_second: 50
```

### Рекомендации

- Crossref рекомендует указывать User-Agent с контактной информацией
- API поддерживает до 50 запросов в секунду
- Для больших объёмов данных рекомендуется использовать Polite Pool

## OpenAlex API

### Настройка

```yaml
sources:
  openalex:
    base_url: "https://api.openalex.org/works"
    timeout_sec: 30.0
    max_retries: 3
    rate_limit:
      requests_per_second: 10
```

### Особенности

- OpenAlex предоставляет бесплатный доступ без регистрации
- Рекомендуется указывать email в User-Agent для получения уведомлений о проблемах

## PubMed API

### Настройка

```yaml
sources:
  pubmed:
    base_url: "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    timeout_sec: 60.0
    max_retries: 10
    rate_limit:
      requests_per_second: 3
```

### Требования

- PubMed требует указания email в параметрах запроса
- API имеет строгие ограничения: не более 3 запросов в секунду
- Рекомендуется использовать batch-запросы для эффективности

## Semantic Scholar API

### Настройка

```yaml
sources:
  semantic_scholar:
    base_url: "https://api.semanticscholar.org/graph/v1/paper"
    timeout_sec: 60.0
    max_retries: 15
    rate_limit:
      requests_per_5min: 100
```

### Аутентификация

Для увеличения лимитов можно получить API ключ:

```yaml
sources:
  semantic_scholar:
    api_key: "${SEMANTIC_SCHOLAR_API_KEY}"  # Из переменных окружения
```

## PubChem API

### Настройка

```yaml
sources:
  pubchem:
    base_url: "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    timeout_sec: 45.0
    max_retries: 8
    rate_limit:
      requests_per_second: 5
```

## UniProt API

### Настройка

```yaml
sources:
  uniprot:
    base_url: "https://rest.uniprot.org"
    timeout_sec: 45.0
    max_retries: 5
    rate_limit:
      requests_per_second: 10
```

## Переменные окружения

Для чувствительных данных используйте переменные окружения:

```bash
# .env файл
SEMANTIC_SCHOLAR_API_KEY=your_api_key_here
CROSSREF_EMAIL=your_email@example.com
PUBMED_EMAIL=your_email@example.com
```

## Проверка подключения

### Тест всех клиентов

```bash
# Проверка доступности всех API
make test-api-connections
```

### Индивидуальная проверка

```python
from library.clients.chembl import ChEMBLClient
from library.clients.crossref import CrossrefClient

# Тест ChEMBL
chembl = ChEMBLClient()
result = chembl.health_check()
print(f"ChEMBL: {result}")

# Тест Crossref
crossref = CrossrefClient()
result = crossref.health_check()
print(f"Crossref: {result}")
```

## Мониторинг и логирование

### Настройка логирования

```yaml
# configs/logging.yaml
loggers:
  library.clients:
    level: INFO
    handlers: [console, file]
```

### Метрики производительности

Все клиенты автоматически собирают метрики:

- Время ответа API
- Количество запросов
- Количество ошибок
- Использование rate limits

## Troubleshooting

### Частые проблемы

1. **Timeout ошибки**
   - Увеличьте `timeout_sec` в конфигурации
   - Проверьте стабильность интернет-соединения

2. **Rate limit превышен**
   - Уменьшите `requests_per_second`
   - Добавьте задержки между запросами

3. **403 Forbidden**
   - Проверьте User-Agent заголовки
   - Убедитесь в корректности API ключей

4. **Connection refused**
   - Проверьте доступность API endpoints
   - Убедитесь в отсутствии блокировок firewall

### Отладка

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Включить детальное логирование для клиентов
logger = logging.getLogger('library.clients')
logger.setLevel(logging.DEBUG)
```

## Лучшие практики

1. **Всегда указывайте User-Agent** с контактной информацией
2. **Используйте переменные окружения** для API ключей
3. **Настройте мониторинг** для отслеживания производительности
4. **Реализуйте graceful degradation** при недоступности API
5. **Кэшируйте результаты** для снижения нагрузки на API
6. **Соблюдайте rate limits** для стабильной работы
