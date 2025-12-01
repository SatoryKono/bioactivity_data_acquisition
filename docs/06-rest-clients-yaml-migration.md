# REST Client YAML Configuration Plan

## Summary — Objectives and Outcomes
- Centralize every REST client parameter (base URLs, endpoints, methods, auth, query/pagination, response schemas) in declarative YAML files per source.
- Replace hardcoded HTTP details inside client implementations with strongly typed configuration objects under `clients.config`.
- Instantiate clients through a factory that consumes YAML-driven configs and a shared HTTP backend, enforcing the unified REST client contract.
- Remove legacy configuration paths and constants, ensuring no fallback adapters are kept.

## YAML Configuration Structure
A single extensible schema applies to all REST sources (ChEMBL, PubChem, PubMed, OpenAlex, Crossref, Semantic Scholar, UniProt, etc.). Defaults assume HTTPS + JSON but can be overridden per resource.

```yaml
source: chembl
protocol: https                # default: https
base_url: "https://www.ebi.ac.uk/chembl/api/data"
default_timeout: 30.0
rate_limit:
  requests_per_minute: 60      # optional
  burst_size: 10               # optional token-bucket parameter

auth:
  type: none                   # none | api_key | bearer | basic | custom
  header_name: null            # e.g., "Authorization" for bearer; required for api_key/custom
  query_param: null            # optional location for api_key
  token_env: CHEMBL_API_TOKEN  # env var name when applicable
  credentials:
    username: null             # for basic
    password_env: null

resources:
  activity:
    path: "/activity"
    method: GET                # GET | POST | PUT | DELETE | PATCH
    headers:
      fixed: {Accept: application/json}
      allowed: ["X-Trace-Id"]
    query:
      fixed: {format: json}
      allowed:
        - target_chembl_id
        - assay_chembl_id
        - document_chembl_id
    body:
      type: json               # json | form | multipart | none
      template: null           # optional Jinja/format template for POST/PUT
    paging:
      type: link               # link | offset | page | token
      page_param: page
      page_size_param: page_size
      default_page_size: 1000
      max_page_size: 1000
      next_link_path: page_info.next
      token_path: null         # for token-based pagination
    response:
      format: json             # json | xml | text
      record_path: activities  # JSONPath/dot notation
      fields:
        - name: activity_id
          path: activity_id
          type: int
        - name: standard_value
          path: standard_value
          type: float
        - name: standard_units
          path: standard_units
          type: str
      extra_metadata:
        - name: page
          path: page_info.page
          type: int
```

**Key rules**
- `resources` may contain multiple entries per source; each resource controls HTTP method, path templating, headers, auth overrides, and pagination.
- `auth` supports per-resource overrides to model heterogeneous endpoints.
- `response.format` governs deserialization; `record_path` and `fields[*].path` use dotted paths for JSON and XPath-like strings for XML.
- `paging` supports link traversal, offset/page integers, or opaque continuation tokens.

## Python Configuration Models (`clients.config`)
Use Pydantic v2 models (or frozen dataclasses with validation helpers) mirroring the YAML schema. Recommended defaults: `protocol="https"`, `response.format="json"`, `paging.type="offset"` with `page_param="page"`, `page_size_param="page_size"`, `default_timeout=30.0`, `auth.type="none"`.

```python
# clients/config/models.py
from pydantic import BaseModel, Field

class AuthConfig(BaseModel):
    type: Literal["none", "api_key", "bearer", "basic", "custom"] = "none"
    header_name: str | None = None
    query_param: str | None = None
    token_env: str | None = None
    credentials: dict[str, str | None] = Field(default_factory=dict)

class RateLimitConfig(BaseModel):
    requests_per_minute: int | None = None
    burst_size: int | None = None

class PagingConfig(BaseModel):
    type: Literal["link", "offset", "page", "token"] = "offset"
    page_param: str | None = "page"
    page_size_param: str | None = "page_size"
    default_page_size: int | None = None
    max_page_size: int | None = None
    next_link_path: str | None = None
    token_path: str | None = None

class FieldConfig(BaseModel):
    name: str
    path: str
    type: Literal["int", "float", "str", "bool", "object", "array"]

class ResponseConfig(BaseModel):
    format: Literal["json", "xml", "text"] = "json"
    record_path: str | None = None
    fields: list[FieldConfig] = Field(default_factory=list)
    extra_metadata: list[FieldConfig] = Field(default_factory=list)

class ResourceConfig(BaseModel):
    path: str
    method: Literal["GET", "POST", "PUT", "DELETE", "PATCH"] = "GET"
    headers: dict[str, dict[str, str] | None] = Field(default_factory=dict)
    query: dict[str, dict[str, str] | list[str] | None] = Field(default_factory=dict)
    body: dict[str, Any] | None = None
    paging: PagingConfig | None = None
    response: ResponseConfig
    auth: AuthConfig | None = None  # optional override

class SourceConfig(BaseModel):
    source: str
    protocol: Literal["http", "https"] = "https"
    base_url: str
    default_timeout: float = 30.0
    rate_limit: RateLimitConfig | None = None
    auth: AuthConfig = Field(default_factory=AuthConfig)
    resources: dict[str, ResourceConfig]
```

**Loading helpers (`clients/config/loader.py`)**
- `load_source_config(name: str, root: Path | None = None) -> SourceConfig` — loads `configs/clients/<name>.yml` (default root=`configs/clients`), validates, injects defaults.
- `load_all_sources(root: Path) -> dict[str, SourceConfig>` — bulk-load with clear error aggregation per file.
- Validation rules: required `source`, `base_url`, `resources`; method must be in allowed set; pagination tokens mutually exclusive with link/offset; `fields.type` limited to known primitives.

## REST Client Factory
The factory produces instances that implement the unified `RestDataClient` contract using only `SourceConfig` and a shared HTTP backend (`clients.base.HttpBackend`).

```python
# clients/factory.py
REGISTRY: dict[str, type[DataClient]] = {
    "chembl": ChemblClient,
    "pubchem": PubChemClient,
    "pubmed": PubMedClient,
    "crossref": CrossrefClient,
    "openalex": OpenAlexClient,
    "semantic_scholar": SemanticScholarClient,
    "uniprot": UniProtClient,
}

def create_client(
    source: str,
    *,
    config: SourceConfig | None = None,
    http_backend: HttpBackend | None = None,
    context: ClientFactoryContext | None = None,
) -> DataClient:
    cfg = config or load_source_config(source)
    backend = http_backend or HttpBackend.from_config(cfg)
    client_cls = REGISTRY[source]
    return client_cls(cfg, backend, context=context)
```

- `HttpBackend.from_config` builds resilient sessions (timeouts, retries, rate limiting, auth injection) once per source.
- Clients become thin: they select a resource from `SourceConfig.resources`, compose requests via `HttpBackend`, and map `ResponseConfig` to `Record` objects without embedding URLs or field names.

## Migration Plan for Existing REST Parameters
1. Inventory current hardcoded values per source (base URLs, endpoints, query mappings, pagination, response keys).
2. Create `src/bioetl/clients/config/yaml/<source>.yml` following the new schema, treating current values as the source of truth.
3. Add `clients.config` models + loaders and wire them into the factory/registry.
4. Refactor each client to consume `SourceConfig` (no direct constants), delegating HTTP execution to `HttpBackend`.
5. Remove legacy config loaders (`bioetl.config.load_client_config`, inline mappings) and deprecated constants.

| Source | Current parameter locations | New YAML file | New config module |
| --- | --- | --- | --- |
| ChEMBL | Default API settings in `src/bioetl/core/config/models.py` (`ChemblAPIConfigModel`), entity names and pagination defaults across `src/bioetl/clients/chembl/*` | `src/bioetl/clients/config/yaml/chembl.yml` | `clients/config/models.py` (SourceConfig) + `clients/factory.py` |
| PubChem | Resource endpoints/id fields via `configs/pubchem.yaml` + `bioetl.config.load_client_config` used in `src/bioetl/clients/pubchem/client.py` | `src/bioetl/clients/config/yaml/pubchem.yml` | same as above |
| PubMed | Resource endpoints/id fields via `configs/pubmed.yaml` + `load_client_config` in `src/bioetl/clients/pubmed/client.py` | `src/bioetl/clients/config/yaml/pubmed.yml` | same |
| Crossref | Resource mapping from `configs/crossref.yaml` consumed by `src/bioetl/clients/crossref/client.py` | `src/bioetl/clients/config/yaml/crossref.yml` | same |
| OpenAlex | Resource mapping from `configs/openalex.yaml` consumed by `src/bioetl/clients/openalex/client.py` | `src/bioetl/clients/config/yaml/openalex.yml` | same |
| Semantic Scholar | Resource mapping from `configs/semantic_scholar.yaml` consumed by `src/bioetl/clients/semantic_scholar/client.py` | `src/bioetl/clients/config/yaml/semantic_scholar.yml` | same |
| UniProt | Resource mapping from `configs/uniprot.yaml` consumed by `src/bioetl/clients/uniprot/client.py` | `src/bioetl/clients/config/yaml/uniprot.yml` | same |

## Validation and Health-Checks
- **Static validation:**
  - Ensure required fields: `source`, `base_url`, `resources[*].path`, `resources[*].response`.
  - Enforce enums for methods/auth/paging types; forbid conflicting pagination settings (e.g., `token_path` with `page_param`).
  - Verify `fields.path`/`record_path` syntax (non-empty strings) and type values.
- **Runtime checks:**
  - Factory bootstraps `HttpBackend` with resolved auth (env var presence for API keys/bearer tokens) and validated rate limits.
  - Per-source smoke test: single request against a lightweight endpoint using YAML config; assert HTTP 2xx and parse per `response.format`.
  - Schema probe: validate that `record_path` exists and `fields.path` resolve in the sample payload; warn on missing/extra fields.
  - Pagination probe: for paginated resources, assert that `next_link_path` or `token_path` is discoverable in the first response.

These steps guarantee that after migration, all REST clients are configuration-driven, free from hardcoded HTTP parameters, and compatible with the unified client contract.
