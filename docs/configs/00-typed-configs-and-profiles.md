# Specification: Typed Configurations and Profiles

This document provides a comprehensive specification for the `bioetl` configuration system, based on the implementation in `[ref: repo:src/bioetl/config/loader.py@test_refactoring_32]`.

## 1. Core Concepts

### 1.1. `PipelineConfig` Pydantic Model

Every configuration is parsed into a strict Pydantic model, `PipelineConfig`, defined in `[ref: repo:src/bioetl/configs/models.py@test_refactoring_32]`. This ensures that any loaded configuration is validated against a single source of truth for its structure, data types, and constraints. An invalid configuration will cause the CLI to fail at startup with a clear validation error.

### 1.2. Configuration Profiles (`extends`)

Profiles are reusable, partial YAML files, typically stored in `configs/profiles/`. They allow common settings (like HTTP client details or determinism policies) to be shared across multiple pipelines. A pipeline configuration can inherit from one or more profiles using the `extends` key.

## 2. Layer Merging Algorithm

The final configuration is built by merging multiple sources. Each subsequent source overrides the values from the previous one. The precise order of precedence is implemented in the `load_config` function in `[ref: repo:src/bioetl/config/loader.py@test_refactoring_32]`.

**Order of Precedence (Lowest to Highest):**

1.  **Base Profiles**: Files listed in the `extends` key are loaded first, providing the base layer. If multiple files are extended, they are merged in the order they are listed.
2.  **Main Config File**: The main pipeline-specific YAML file (e.g., `chembl_activity.yaml`) is merged on top of the base profiles.
3.  **CLI `--set` Overrides**: Key-value pairs provided via the `--set` command-line flag are merged next. The `parse_cli_overrides` function handles the parsing of these dot-notation keys.
4.  **Environment Variable Overrides**: Environment variables are the final layer and have the highest precedence.

### Pseudocode for Merging Logic (based on `loader.py`)

```python
# Simplified from src/bioetl/config/loader.py

def load_config(config_path, overrides, env_prefix):
    # 1. Load main config file and recursively resolve `extends`
    # This creates the base configuration dictionary.
    config_data = _load_with_extends(config_path)

    # 2. Apply CLI --set overrides
    # The `overrides` dict is created by `parse_cli_overrides`.
    if overrides:
        config_data = deep_merge(config_data, overrides)

    # 3. Apply environment variables
    # The `_load_env_overrides` function finds all env vars
    # with the specified prefix (e.g., 'BIOETL_').
    env_overrides = _load_env_overrides(env_prefix)
    if env_overrides:
        config_data = deep_merge(config_data, env_overrides)

    # 4. Validate the final merged dictionary against Pydantic model
    return PipelineConfig.model_validate(config_data)
```

## 3. Environment Variable Overrides

The `_load_env_overrides` function in `loader.py` implements the logic for environment variable overrides.

-   **Prefixes**: The function checks for variables starting with `BIOETL_` or `BIOACTIVITY_`.
-   **Path Construction**: The path to the configuration key is constructed from the variable name by stripping the prefix and splitting the remainder by a double underscore (`__`). The parts are lowercased.
-   **Example**:
    -   An environment variable `BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=120.0`
    -   will be parsed into the dictionary `{'http': {'default': {'timeout_sec': 120.0}}}`
    -   This dictionary is then deep-merged into the main configuration, overriding any existing value for that key.

## 4. `!include` Tag

The YAML loader is extended with a custom `!include` tag. This allows a YAML file to include content from another file directly. This is a powerful feature for composing complex configurations, but it is distinct from the `extends` mechanism, which operates on the dictionary level after the files are loaded.

## 5. Reference: `PipelineConfig` Structure

The following table details the key top-level blocks available in the `PipelineConfig` model. For the full, authoritative structure, see `[ref: repo:src/bioetl/configs/models.py@test_refactoring_32]`.

| Key | Type | Description |
| --- | --- | --- |
| **`version`** | `int` | **Required.** The version of the config schema. Must be `1`. |
| **`pipeline`** | `object` | **Required.** Metadata about the pipeline itself. |
| **`http`** | `dict[str, object]` | **Required.** A dictionary of named HTTP client profiles. |
| **`sources`** | `dict[str, object]` | Configuration for each data source. |
| **`cache`** | `object` | Configuration for the HTTP request cache. |
| **`paths`** | `object` | Defines root directories for I/O operations. |
| **`determinism`** | `object` | Settings for ensuring reproducible outputs. |
| **`materialization`** | `object` | Configuration for writing data artifacts. |
| **`fallbacks`** | `object` | Global configuration for source fallback behavior. |
| **`cli`**| `object` | Captures runtime flags and toggles from the CLI. |

## 6. Usage Example

**`configs/pipelines/chembl/activity.yaml`:**
```yaml
extends:
  - ../../profiles/base.yaml
  - ../../profiles/determinism.yaml

version: 1

pipeline:
  name: "chembl-activity"
  entity: "activity"

sources:
  chembl:
    batch_size: 25
```

**Execution with Overrides:**
```bash
# Set the BIOETL__SOURCES__CHEMBL__BATCH_SIZE env var
export BIOETL__SOURCES__CHEMBL__BATCH_SIZE=50

# Run the pipeline with a --set override
python -m bioetl.cli.main activity \
  --config configs/pipelines/chembl/activity.yaml \
  --set sources.chembl.batch_size=10
```

**Resulting Precedence:**

Based on the implementation in `loader.py`, the final value for `sources.chembl.batch_size` is determined as follows:

1.  The `base.yaml` and `determinism.yaml` profiles are loaded.
2.  `activity.yaml` is merged, setting the `batch_size` to `25`.
3.  The `--set` override from the CLI is merged, changing the `batch_size` to `10`.
4.  Finally, the environment variable override is merged, changing the `batch_size` to `50`.

Therefore, for this specific execution, the pipeline would run with a `batch_size` of **50**.
