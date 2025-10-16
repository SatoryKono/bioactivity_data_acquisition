# Configuration Reference

This document describes the canonical configuration file (`configs/config.yaml`)
used by the
bioactivity ETL pipeline. The configuration system is based on Pydantic models
and supports
layered overrides with the following precedence (lowest to highest):

1. **Built-in defaults**– defined in `library.config`.
2.**YAML file**– values defined in `configs/config.yaml`or a custom file
passed to the CLI.
3.**Environment variables**– keys prefixed with `BIOACTIVITY__` by default.
4.**CLI overrides**– provided with repeated`--set section.key=value`options.

Secrets (API tokens, credentials) are**never** read from YAML. They are
resolved exclusively from
environment variables during the final loading step. Missing required secrets
raise an error before
any network call is attempted.

## Sections

| Section | Description | Key highlights |

|---------|-------------|----------------|

|`http`| Global HTTP behaviour for clients. |`global.timeout*sec`,
`global.retries.total`, `global.headers.*`. |

| `sources`| Per-source HTTP configuration. Each key is a source slug. |`http.base*url`, `pagination.*`, `http.timeout*sec`. |

| `io`| Input/output paths. Directories are created automatically. |`input.documents*csv`, `output.data*path`, `output.qc*report*path`. |

| `runtime`| Execution toggles exposed via CLI and env overrides. |`workers`.
|

| `logging`| Structured logging configuration. |`level`. |

| `validation`| Pandera validation options and QC thresholds. |`strict`,
`qc.max*missing*fraction`, `qc.max*duplicate*fraction`. |

| `determinism`| Controls reproducibility of generated artefacts. |`sort.by`,
`sort.ascending`, `column*order`. |

| `transforms`| Unit harmonisation and other transformations. |`unit*conversion`. |

| `postprocess`| Optional post-processing outputs. |`qc.enabled`,
`correlation.enabled`. |

### Environment variable overrides

Environment variables use a double-underscore (`__`) to separate nesting levels and the prefix `BIOACTIVITY__`.

Examples (Bash):

```bash
export BIOACTIVITY__LOGGING__LEVEL=DEBUG
export BIOACTIVITY__HTTP__GLOBAL__TIMEOUT_SEC=45
export BIOACTIVITY__SOURCES__CHEMBL__HTTP__HEADERS__AUTHORIZATION="Bearer token"
export BIOACTIVITY__RUNTIME__WORKERS=8
```

Examples (PowerShell):

```powershell
$env:BIOACTIVITY__LOGGING__LEVEL = "DEBUG"
$env:BIOACTIVITY__HTTP__GLOBAL__TIMEOUT_SEC = "45"
$env:BIOACTIVITY__SOURCES__CHEMBL__HTTP__HEADERS__AUTHORIZATION = "Bearer token"
$env:BIOACTIVITY__RUNTIME__WORKERS = "8"
```

### CLI overrides

The CLI exposes a `--set` (`-s`) option that accepts dotted paths:

```bash

bioactivity-data-acquisition pipeline --config configs/config.yaml \
  --set runtime.workers=8 \
  --set sources.chembl.pagination.max_pages=1
```

Each`--set`argument must use`KEY=VALUE`syntax (the equals sign is
mandatory). Passing a
malformed value, such as`--set runtime.workers`, results in an error before the
pipeline starts.

Values are parsed using YAML semantics, so `true`, `false`, `null`, integers,
and floats are
converted automatically. Later overrides win over earlier ones.

### Secrets

HTTP headers and parameters can reference environment variables by wrapping the
variable name in
curly braces. For example, the canonical configuration sets
`Authorization: "Bearer {CHEMBL_API_TOKEN}"`. At load time, `Config` replaces
the placeholder with
the value of the`CHEMBL*API*TOKEN`environment variable. Placeholders that do
not resolve are left
untouched, which keeps validation deterministic for local development.

### Canonical file

The canonical configuration lives at`configs/config.yaml`. It provides working
defaults for the
ChEMBL + Crossref pipeline and can be copied as a starting point for new
environments. Keep
`reports/config_audit.csv` in sync with changes to this file.

### Keys and defaults (excerpt)

| Key | Type | Default | Notes |
|---|---|---|---|
| http.global.timeout_sec | float | 30.0 | Global HTTP timeout |
| http.global.retries.total | int | 5 | Total retry attempts |
| http.global.retries.backoff_multiplier | float | 1.0 | Exponential backoff |
| http.global.headers.User-Agent | str | bioactivity-data-acquisition/0.1.0 | Sent to all sources |
| sources.\<name\>.http.base_url | url | — | Per-source base URL |
| sources.\<name\>.pagination.max_pages | int/null | null | Pagination cap |
| io.output.data_path | path | — | Output dataset path |
| io.output.qc_report_path | path | — | QC report path |
| io.output.correlation_path | path | — | Correlation path |
| runtime.workers | int | 4 | Worker threads |
| validation.qc.max_missing_fraction | float | 1.0 | QC threshold |
| validation.qc.max_duplicate_fraction | float | 1.0 | QC threshold |
| determinism.sort.by | list[str] | [document_chembl_id, doi] | Deterministic order |
| determinism.column_order | list[str] | see code | Output columns order |
| postprocess.qc.enabled | bool | true | Generate QC |
| postprocess.correlation.enabled | bool | true | Generate correlation |

Full list is defined by Pydantic models in `src/library/config.py` and `src/library/documents/config.py`.

### Secrets table

Secrets are substituted into configuration values using `{PLACEHOLDER}` syntax and must come from environment variables.

| Placeholder | Environment variable | Example usage |
|---|---|---|
| `{CHEMBL_API_TOKEN}` | `CHEMBL_API_TOKEN` | `sources.chembl.http.headers.Authorization: "Bearer {CHEMBL_API_TOKEN}"` |
| `{PUBMED_API_KEY}` | `PUBMED_API_KEY` | `sources.pubmed.http.headers.api_key: "{PUBMED_API_KEY}"` |
| `{SEMANTIC_SCHOLAR_API_KEY}` | `SEMANTIC_SCHOLAR_API_KEY` | `sources.semantic_scholar.http.headers.x-api-key: "{SEMANTIC_SCHOLAR_API_KEY}"` |
| `{crossref_api_key}` | `CROSSREF_API_KEY` | `sources.crossref.http.headers.Crossref-Plus-API-Token: "{crossref_api_key}"` |
