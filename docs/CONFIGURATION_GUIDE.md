# Configuration Patterns for ChEMBL Pipelines

To keep ChEMBL-specific configuration consistent across pipelines, reuse the shared include defined in `configs/includes/chembl_source.yaml`.

## Shared include

The include provides baseline values for the primary ChEMBL source:

```yaml

# configs/includes/chembl_source.yaml

sources:
  chembl:
    enabled: true
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    batch_size: 20
    max_url_length: 2000
    headers:
      Accept: "application/json"
      User-Agent: "bioetl-chembl-default/1.0"
    rate_limit_jitter: true

```text

These defaults cover the API endpoint, deterministic request headers, and common throttling behaviour. Individual pipelines only override the pieces that differ.

## Referencing the include

Pipeline YAML files should extend both the global base configuration and the shared ChEMBL include:

```yaml

extends:

  - ../base.yaml
  - ../includes/chembl_source.yaml

```text

Inside the `sources.chembl` block override only the parameters that vary per pipeline, typically `batch_size` and the pipeline-specific `headers.User-Agent`:

```yaml

sources:
  chembl:
    batch_size: 10
    headers:
      User-Agent: "bioetl-document-pipeline/1.0"

```text

Additional ChEMBL options (e.g. cache settings, circuit breakers) may be added in the pipeline file as needed, but the shared defaults should remain untouched.

## External adapter overrides

Document enrichment adapters (PubMed, Crossref, OpenAlex, Semantic Scholar) inherit global cache and HTTP defaults. When a specific source needs different behaviour, override the fields directly inside the corresponding `sources.<adapter>` block:

```yaml

sources:
  pubmed:
    cache_enabled: false
    cache_ttl: 3600        # seconds
    cache_maxsize: 2048    # entries in the TTL cache
    timeout_sec: 15.0      # applies to both connect/read timeouts

```text

All overrides are optional; unset values fall back to the global `cache` and `http.global` configuration. Use `timeout_sec` to adjust both connect and read timeouts together, or provide the more granular `connect_timeout_sec` / `read_timeout_sec` keys when an API needs asymmetric limits.

## Validating merges

Configuration loading resolves all `extends` entries recursively. Unit tests under `tests/unit/test_config_loader.py` ensure that multiple `extends` blocks merge correctly and that per-pipeline overrides are applied without losing the shared defaults. If you introduce new includes, add similar tests to guard against regression.

## Environment-bound secrets

Некоторые адаптеры не работают без заранее экспортированных переменных окружения.
Полный список и формат значений задокументирован в [`.env.example`](../.env.example).

* `PUBMED_TOOL` и `PUBMED_EMAIL` подставляются в конфиг документного пайплайна и
  передаются в PubMed E-utilities (`sources.pubmed.tool/email`).
* `CROSSREF_MAILTO` требуется Crossref для polite-пула (`sources.crossref.mailto`).
* `SEMANTIC_SCHOLAR_API_KEY` и `PUBMED_API_KEY` опциональны, но расширяют лимиты
  соответствующих API (`sources.semantic_scholar.api_key`, `sources.pubmed.api_key`).
* `IUPHAR_API_KEY` обязателен для обогащения целевых сущностей (`sources.iuphar`).

Рекомендуемый порядок загрузки секретов:

```bash
cp .env.example .env           # однократно, затем заполните значения
${SHELL:-bash} -lc 'set -a; source .env; set +a'
```

Команда выше экспортирует все ключи из `.env` в текущую оболочку, после чего CLI
(`python -m bioetl.cli.main …`) и тесты используют корректные токены.

