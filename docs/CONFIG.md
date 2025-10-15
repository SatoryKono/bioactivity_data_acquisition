# Configuration Reference

This document describes the canonical configuration file (`configs/config.yaml`) used by the
bioactivity ETL pipeline. The configuration system is based on Pydantic models and supports
layered overrides with the following precedence (lowest to highest):

1. **Built-in defaults** – defined in `bioactivity.config`.
2. **YAML file** – values defined in `configs/config.yaml` or a custom file passed to the CLI.
3. **Environment variables** – keys prefixed with `BIOACTIVITY__` by default.
4. **CLI overrides** – provided with repeated `--set section.key=value` options.

Secrets (API tokens, credentials) are **never** read from YAML. They are resolved exclusively from
environment variables during the final loading step. Missing required secrets raise an error before
any network call is attempted.

## Sections

| Section | Description | Key highlights |
|---------|-------------|----------------|
| `project` | Metadata about the deployment. | `name`, `version`, `description`. |
| `io` | Input/output paths. Directories are created automatically. | `output.data_path`, `output.qc_report_path`, `output.correlation_path`. |
| `runtime` | Execution toggles exposed via CLI and env overrides. | `log_level`, `max_workers`, `progress`. |
| `determinism` | Controls reproducibility of generated artefacts. | `random_seed`, `sort_rows`, `row_sort_keys`. |
| `validation` | Pandera validation options and QC thresholds. | `strict`, `enforce_output_schema`, `thresholds.*`. |
| `http` | Global HTTP behaviour for clients. | `timeout`, `retry.*`, `rate_limit.*`, `user_agent`. |
| `sources` | Per-source HTTP configuration. Each key is a source slug. | `url`, `params`, `pagination.*`, `auth.secret_name`. |
| `postprocess` | Optional post-processing outputs. | `qc.enabled`, `qc.correlation`, `reporting.include_timestamp`. |
| `cli` | CLI-specific behaviour. | `env_prefix`, `defaults_file`, `allow_env_override`. |
| `secrets` | Registry of secret definitions (resolved from env). | `required`, `optional`. |

### Environment variable overrides

Environment variables use a double-underscore (`__`) to separate nesting levels and a prefix defined
in `cli.env_prefix` (default `BIOACTIVITY`). Examples:

- `BIOACTIVITY__RUNTIME__LOG_LEVEL=DEBUG`
- `BIOACTIVITY__SOURCES__CHEMBL__PAGINATION__MAX_PAGES=3`
- `BIOACTIVITY__CLI__ENV_PREFIX=PIPELINE` (can be used to change the prefix itself)

If `cli.env_prefix` is overridden (via YAML or environment), a second environment scan is performed
with the new prefix so that subsequent overrides can use it.

### CLI overrides

The CLI exposes a `--set` (`-s`) option that accepts dotted paths:

```bash
bioactivity-data-acquisition pipeline --config configs/config.yaml \
  --set runtime.log_level=DEBUG \
  --set sources.chembl.pagination.max_pages=1
```

Values are parsed using YAML semantics, so `true`, `false`, `null`, integers, and floats are
converted automatically. Later overrides win over earlier ones.

### Secrets

Declare secrets under `secrets.required` or `secrets.optional` with logical names and the name of the
environment variable providing the value. To reference a secret from a source, set
`sources.<slug>.auth.secret_name` to the logical name. During load, the configuration resolves the
secret and injects the appropriate header.

| Logical name | Environment variable | Used by | Required |
|--------------|---------------------|---------|----------|
| `chembl_api_token` | `CHEMBL_API_TOKEN` | `sources.chembl` | Yes |
| `crossref_api_key` | `CROSSREF_API_KEY` | `sources.crossref` | Optional |

### Canonical file

The canonical configuration lives at `configs/config.yaml`. It provides working defaults for the
ChEMBL + Crossref pipeline and can be copied as a starting point for new environments. Keep
`reports/config_audit.csv` in sync with changes to this file.
