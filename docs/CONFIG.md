# Configuration Reference

This document describes the canonical configuration file (`configs/config.yaml`) used by the
bioactivity ETL pipeline. The configuration system is based on Pydantic models and supports
layered overrides with the following precedence (lowest to highest):

1. **Built-in defaults** – defined in `library.config`.
2. **YAML file** – values defined in `configs/config.yaml` or a custom file passed to the CLI.
3. **Environment variables** – keys prefixed with `BIOACTIVITY__` by default.
4. **CLI overrides** – provided with repeated `--set section.key=value` options.

Secrets (API tokens, credentials) are **never** read from YAML. They are resolved exclusively from
environment variables during the final loading step. Missing required secrets raise an error before
any network call is attempted.

## Sections

| Section | Description | Key highlights |
|---------|-------------|----------------|
| `http` | Global HTTP behaviour for clients. | `global.timeout_sec`, `global.retries.total`, `global.headers.*`. |
| `sources` | Per-source HTTP configuration. Each key is a source slug. | `http.base_url`, `pagination.*`, `http.timeout_sec`. |
| `io` | Input/output paths. Directories are created automatically. | `input.documents_csv`, `output.data_path`, `output.qc_report_path`. |
| `runtime` | Execution toggles exposed via CLI and env overrides. | `workers`. |
| `logging` | Structured logging configuration. | `level`. |
| `validation` | Pandera validation options and QC thresholds. | `strict`, `qc.max_missing_fraction`, `qc.max_duplicate_fraction`. |
| `determinism` | Controls reproducibility of generated artefacts. | `sort.by`, `sort.ascending`, `column_order`. |
| `transforms` | Unit harmonisation and other transformations. | `unit_conversion`. |
| `postprocess` | Optional post-processing outputs. | `qc.enabled`, `correlation.enabled`. |

### Environment variable overrides

Environment variables use a double-underscore (`__`) to separate nesting levels and the prefix
`BIOACTIVITY__`. Examples:

- `BIOACTIVITY__HTTP__GLOBAL__TIMEOUT_SEC=45`
- `BIOACTIVITY__SOURCES__CHEMBL__HTTP__HEADERS__AUTHORIZATION="Bearer token"`
- `BIOACTIVITY__RUNTIME__WORKERS=8`
- `BIOACTIVITY__LOGGING__LEVEL=DEBUG`

### CLI overrides

The CLI exposes a `--set` (`-s`) option that accepts dotted paths:

```bash
bioactivity-data-acquisition pipeline --config configs/config.yaml \
  --set runtime.workers=8 \
  --set sources.chembl.pagination.max_pages=1
```

Each `--set` argument must use `KEY=VALUE` syntax (the equals sign is mandatory). Passing a
malformed value, such as `--set runtime.workers`, results in an error before the pipeline starts.

Values are parsed using YAML semantics, so `true`, `false`, `null`, integers, and floats are
converted automatically. Later overrides win over earlier ones.

### Secrets

HTTP headers and parameters can reference environment variables by wrapping the variable name in
curly braces. For example, the canonical configuration sets
`Authorization: "Bearer {CHEMBL_API_TOKEN}"`. At load time, `Config` replaces the placeholder with
the value of the `CHEMBL_API_TOKEN` environment variable. Placeholders that do not resolve are left
untouched, which keeps validation deterministic for local development.

### Canonical file

The canonical configuration lives at `configs/config.yaml`. It provides working defaults for the
ChEMBL + Crossref pipeline and can be copied as a starting point for new environments. Keep
`reports/config_audit.csv` in sync with changes to this file.
