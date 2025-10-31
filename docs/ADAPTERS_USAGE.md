# Использование внешних адаптеров

## Обзор

Проект реализует 4 адаптера для обогащения метаданных документов из внешних источников:

1. **PubMedAdapter** - PubMed E-utilities API
2. **CrossrefAdapter** - Crossref REST API
3. **OpenAlexAdapter** - OpenAlex Works API
4. **SemanticScholarAdapter** - Semantic Scholar Graph API

## Конфигурация

### Переменные окружения

Перед запуском необходимо установить следующие переменные окружения (опционально):

```bash

export PUBMED_EMAIL="your-email@example.com"
export PUBMED_API_KEY="your-pubmed-api-key"  # optional, для увеличения лимитов

export CROSSREF_MAILTO="your-email@example.com"  # для polite pool

export SEMANTIC_SCHOLAR_API_KEY="your-s2-api-key"  # обязателен для production

```

> **Примечание.** Синтаксис `${VAR:}` указывает конфигуратор использовать пустую строку,
> если переменная окружения `VAR` не определена. Это удобно для опциональных ключей API,
> вроде `PUBMED_API_KEY` или `SEMANTIC_SCHOLAR_API_KEY`, которые повышают лимиты, но не
> требуются для локальных прогонов.

### Конфигурация YAML

Файл `configs/pipelines/document.yaml`:

```yaml

sources:
  pubmed:
    enabled: true
    base_url: "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    tool: "bioactivity_etl"
    email: "${PUBMED_EMAIL}"
    api_key: "${PUBMED_API_KEY:}"
    batch_size: 200
    rate_limit_max_calls: 3  # 10 with API key
    rate_limit_period: 1.0
    workers: 1

  crossref:
    enabled: true
    base_url: "https://api.crossref.org"
    mailto: "${CROSSREF_MAILTO}"
    batch_size: 100
    rate_limit_max_calls: 2
    rate_limit_period: 1.0
    workers: 2

  openalex:
    enabled: true
    base_url: "https://api.openalex.org"
    batch_size: 100
    rate_limit_max_calls: 10
    rate_limit_period: 1.0
    workers: 4

  semantic_scholar:
    enabled: true
    base_url: "https://api.semanticscholar.org/graph/v1"
    api_key: "${SEMANTIC_SCHOLAR_API_KEY:}"
    batch_size: 50
    rate_limit_max_calls: 1  # 10 with API key
    rate_limit_period: 1.25
    workers: 1

```

## Использование

### Автоматическая интеграция

Адаптеры автоматически интегрированы в `DocumentPipeline`:

```python

from bioetl.config import PipelineConfig
from bioetl.sources.chembl.document.pipeline import DocumentPipeline

config = PipelineConfig.from_yaml("configs/pipelines/document.yaml")
pipeline = DocumentPipeline(config, run_id="test")

# Извлечение с автоматическим обогащением

df = pipeline.extract("data/input/documents.csv")

```

> **Совместимость:** исторический импорт `from bioetl.pipelines.document import DocumentPipeline` остаётся валидным благодаря прокси `src/bioetl/pipelines/document.py`.

### Прямое использование

```python

from bioetl.sources.document.adapters.config import AdapterConfig
from bioetl.sources.document.adapters.crossref import CrossrefAdapter
from bioetl.sources.document.adapters.pubmed import PubMedAdapter
from bioetl.core.api_client import APIConfig

# Настройка PubMed

pubmed_config = APIConfig(
    name="pubmed",
    base_url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
    rate_limit_max_calls=3,
    rate_limit_period=1.0,
)
adapter_config = AdapterConfig(enabled=True, batch_size=200, workers=1)
adapter_config.tool = "bioactivity_etl"
adapter_config.email = "test@example.com"

pubmed = PubMedAdapter(pubmed_config, adapter_config)

# Получение данных по PMIDs

pmids = ["12345678", "87654321"]
df = pubmed.process(pmids)

pubmed.close()

```

## Приоритеты источников

При объединении данных из нескольких источников используется следующая приоритетность:

- **title**: PubMed > ChEMBL > OpenAlex > Crossref > Semantic Scholar
- **abstract**: PubMed > ChEMBL > OpenAlex > Crossref > Semantic Scholar
- **journal**: PubMed > Crossref > OpenAlex > ChEMBL > Semantic Scholar
- **authors**: PubMed > Crossref > OpenAlex > ChEMBL > Semantic Scholar
- **doi_clean**: Crossref > PubMed > OpenAlex > Semantic Scholar > ChEMBL
- **year**: PubMed > Crossref > OpenAlex > ChEMBL > Semantic Scholar
- **volume**: PubMed > Crossref > ChEMBL
- **issue**: PubMed > Crossref > ChEMBL
- **issn_print**: Crossref > PubMed
- **issn_electronic**: Crossref > PubMed

## Rate Limiting

Все адаптеры используют `UnifiedAPIClient` с встроенными rate limiters:

- **PubMed**: 3 rps (10 rps с API key)
- **Crossref**: 2 rps
- **OpenAlex**: 10 rps
- **Semantic Scholar**: 0.8 rps (10 rps с API key)

## Получение API ключей

### PubMed API Key

1. Зарегистрироваться на https://www.ncbi.nlm.nih.gov/account/settings/
2. Найти API Key в настройках аккаунта

### Crossref (не требуется)

Crossref не требует API ключ, но рекомендуется указать `mailto` для polite pool

### Semantic Scholar API Key

1. Зарегистрироваться на https://www.semanticscholar.org/
2. Перейти в Developer API settings
3. Создать API ключ

## Troubleshooting

### PubMed: "Required parameter 'email' missing"

- Установите переменную окружения `PUBMED_EMAIL`
- Или укажите в конфиге `email`

### Semantic Scholar: Rate limit exceeded

- Получите API ключ для увеличения лимитов
- Установите `SEMANTIC_SCHOLAR_API_KEY`

### Connection timeout

- Проверьте настройки `timeout_connect` и `timeout_read` в конфиге
- Увеличьте значения для медленных источников

