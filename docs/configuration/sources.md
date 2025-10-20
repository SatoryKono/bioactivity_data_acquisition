## Configuration → Sources

Опирается на `configs/config.yaml` и `configs/config_target_full.yaml`.

### Общие ключи

- `http.global.timeout_sec`, `http.global.retries.total|backoff_multiplier`, `http.global.headers`
- Пер-источник: `sources.<name>.http.base_url|timeout_sec|headers|retries`, `sources.<name>.rate_limit`

### Используемые источники

#### chembl

- base_url: `https://www.ebi.ac.uk/chembl/api/data`
- заголовки: `Accept: application/json`, `Authorization: "Bearer {CHEMBL_API_TOKEN}"` (при наличии)
- ретраи: `total`/`backoff_multiplier` (пер-источник либо глобальные)
- особое: статус/релиз через клиент (`get_chembl_status`)

#### crossref

- base_url: `https://api.crossref.org/works`
- заголовки: `Accept: application/json`, `Crossref-Plus-API-Token: "{CROSSREF_API_KEY}"`
- пагинация: cursor (`page_param: cursor`)

#### openalex

- base_url: `https://api.openalex.org/works`
- заголовки: `Accept: application/json`

#### pubmed

- base_url: `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/`
- заголовки: `Accept: application/json`, `User-Agent: bioactivity-data-acquisition/0.1.0`, `api_key: "{PUBMED_API_KEY}"`
- ретраи: повышенные (`total: 10`, `backoff_multiplier: 3.0`)
- rate limit: пример `max_calls: 2, period: 1.0`

#### semantic_scholar

- base_url: `https://api.semanticscholar.org/graph/v1/paper`
- заголовки: `Accept: application/json`, `x-api-key: "{SEMANTIC_SCHOLAR_API_KEY}"`
- ретраи/лимиты: консервативные (`total: 15`, `backoff_multiplier: 5.0`, `max_calls: 1/10s`)

#### uniprot (target)

- base_url: `https://rest.uniprot.org/uniprotkb`
- заголовки: `Accept: application/json`, `User-Agent: bioactivity-data-acquisition/0.1.0`
- ретраи: `total: 15`, `backoff_multiplier: 4.0`
- rate limit: `max_calls: 3, period: 1.0`

#### iuphar (target)

- base_url: `https://www.guidetopharmacology.org`
- заголовки: `Accept: application/json`, `User-Agent: bioactivity-data-acquisition/0.1.0`
- CSV словари: `configs/dictionary/_target/` (файлы: `_IUPHAR_family.csv`, `_IUPHAR_target.csv`)

### Секреты через окружение

- Плейсхолдеры в заголовках `{VARNAME}` → берутся из `ENV`.
- Рекомендуемый префикс для переопределений: `BIOACTIVITY__...` (см. `library.documents.config.DEFAULT_ENV_PREFIX`).

### Типовые ограничения и рекомендации

- Для внешних API возможны ответы 429/5xx; используйте консервативные ретраи и лимиты.
- Ключи/токены повышают лимиты (например, `api_key` для PubMed, `x-api-key` для Semantic Scholar, `Crossref-Plus-API-Token` для Crossref).
- Уточняйте лимиты на стороне провайдера и задавайте `rate_limit.max_calls/period` для каждого источника.

### Быстрые проверки и мониторинг

Для быстрой проверки доступности и лимитов используйте вспомогательные команды:

```bash
python -m library.tools.quick_api_check
python -m library.tools.api_health_check --save
python -m library.tools.check_api_limits
python -m library.tools.check_specific_limits --source chembl
```

Для периодического мониторинга доступны:

```bash
python -m library.tools.monitor_api
python -m library.tools.monitor_pubmed
python -m library.tools.monitor_semantic_scholar
```
