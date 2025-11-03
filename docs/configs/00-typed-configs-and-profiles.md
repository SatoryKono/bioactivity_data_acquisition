# Specification: Typed Configurations and Profiles

This document provides a comprehensive specification for the `bioetl` configuration system, based on the implementation in `[ref: repo:src/bioetl/config/loader.py@refactoring_001]`.

## 1. Core Concepts

### 1.1. `PipelineConfig` Pydantic Model

Every configuration is parsed into a strict Pydantic model, `PipelineConfig`, defined in `[ref: repo:src/bioetl/configs/models.py@refactoring_001]`. This ensures that any loaded configuration is validated against a single source of truth for its structure, data types, and constraints. An invalid configuration will cause the CLI to fail at startup with a clear validation error.

### 1.2. Configuration Profiles (`extends`)

Profiles are reusable, partial YAML files, typically stored in `configs/profiles/`. They allow common settings (like HTTP client details or determinism policies) to be shared across multiple pipelines. A pipeline configuration can inherit from one or more profiles using the `extends` key.

## 2. Layer Merging Algorithm

The final configuration is built by merging multiple sources. Each subsequent source overrides the values from the previous one. The precise order of precedence is implemented in the `load_config` function in `[ref: repo:src/bioetl/config/loader.py@refactoring_001]`.

**Order of Precedence (Lowest to Highest):**

1.  **Base Profiles**: Files listed in the `extends` key are loaded first.
2.  **Main Config File**: The main pipeline-specific YAML file is merged on top.
3.  **CLI `--set` Overrides**: Key-value pairs from the `--set` flag are merged next.
4.  **Environment Variable Overrides**: Environment variables have the highest precedence.

### Pseudocode for Merging Logic (based on `loader.py`)

```python
# Simplified from src/bioetl/config/loader.py

def load_config(config_path, overrides, env_prefix):
    # 1. Load main config file and recursively resolve `extends`
    config_data = _load_with_extends(config_path)

    # 2. Apply CLI --set overrides
    if overrides:
        config_data = deep_merge(config_data, overrides)

    # 3. Apply environment variables
    env_overrides = _load_env_overrides(env_prefix)
    if env_overrides:
        config_data = deep_merge(config_data, env_overrides)

    # 4. Validate the final merged dictionary
    return PipelineConfig.model_validate(config_data)
```

## 3. Environment Variable Overrides

-   **Prefixes**: The loader checks for variables starting with `BIOETL_` or `BIOACTIVITY_`.
-   **Path Construction**: The variable name is stripped of its prefix, lowercased, and split by `__` to construct the nested dictionary path.
-   **Example**: `BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=120.0` overrides `http.default.timeout_sec`.

## 4. `!include` Tag

The YAML loader supports a custom `!include` tag for embedding one YAML file within another. This is distinct from the `extends` mechanism.

## 5. Reference: `PipelineConfig` Structure

The `PipelineConfig` model is the root of the configuration. For the full, authoritative structure, see `[ref: repo:src/bioetl/configs/models.py@refactoring_001]`.

| Key | Description |
|---|---|
| `version` | **Required.** The config schema version (must be `1`). |
| `pipeline`| **Required.** Metadata about the pipeline. |
| `http` | **Required.** Named profiles for HTTP clients. |
| `sources` | Configuration for each data source. |
| `cache` | Configuration for the HTTP request cache. |
| `paths` | Root directories for I/O operations. |
| `determinism`| Settings for ensuring reproducible outputs. |
| `materialization`| Configuration for writing data artifacts. |
| `fallbacks`| Global configuration for source fallback behavior. |
| `cli`| Captures runtime flags from the CLI. |

## 6. Usage Example

**`configs/pipelines/chembl/activity.yaml`:**
```yaml
extends:
  - ../../profiles/base.yaml
  - ../../profiles/determinism.yaml

sources:
  chembl:
    batch_size: 25
```

**Execution with Overrides:**
```bash
export BIOETL__SOURCES__CHEMBL__BATCH_SIZE=50

python -m bioetl.cli.main activity \
  --config configs/pipelines/chembl/activity.yaml \
  --set sources.chembl.batch_size=10
```
In this case, the `batch_size` would be **50**, as the environment variable has the highest precedence.
