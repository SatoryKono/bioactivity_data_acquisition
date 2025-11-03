# Specification: Typed Configurations and Profiles

This document provides a comprehensive specification for the `bioetl` configuration system. The system is built on Pydantic for strict type validation and supports a layered approach, including reusable profiles, to promote consistency and reduce boilerplate.

- **[Reference](#reference-pipelineconfig-structure):** Jump to the full `PipelineConfig` reference table.

## 1. Core Concepts

### 1.1. `PipelineConfig` Pydantic Model

The entire configuration is parsed into a strict Pydantic model, `PipelineConfig`. This ensures that every configuration file is validated against a single source of truth for structure, data types, and constraints (e.g., a value must be a positive integer). If a configuration file violates the defined schema, the pipeline will fail at startup with a clear error message, preventing runtime errors caused by typos or incorrect value types.

The full, versioned source for this model can be found at `[ref: repo:src/bioetl/configs/models.py]`.

### 1.2. Configuration Profiles

Profiles are reusable, partial YAML configuration files located in `configs/profiles/`. They are designed to capture common settings for a specific concern. The two primary profiles are:

- **`base.yaml`**: Provides baseline settings for HTTP clients, caching, and path conventions. All pipelines should extend this.
- **`determinism.yaml`**: Provides settings to ensure bit-for-bit reproducible outputs, such as stable sorting and hashing policies. Pipelines requiring determinism should extend this.

### 1.3. Configuration Layers and Merging

The final configuration used by a pipeline is built by merging multiple layers. Each subsequent layer overrides the values from the previous one. The order of precedence is as follows (where 1 is the lowest precedence):

1.  **Profiles**: The `extends` key in the main config file loads one or more profiles.
2.  **Main Config File**: The pipeline-specific YAML file (e.g., `chembl_activity.yaml`).
3.  **CLI `--set` Overrides**: Key-value pairs provided at the command line.
4.  **Environment Variable Overrides**: For overriding specific, sensitive, or runtime-dependent values.

## 2. Layer Merging Algorithm

The configuration loader recursively merges dictionaries from each layer. When a key exists in a higher-precedence layer, its value replaces the value from the lower-precedence layer. Lists are replaced, not merged.

### Pseudocode for Merging

```python
def merge(base: dict, override: dict) -> dict:
    """Recursively merge `override` into `base`."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge(result[key], value)
        else:
            # For simple values and lists, the override completely replaces the base.
            result[key] = value
    return result

# 1. Load profiles specified in `extends`
final_config = {}
for profile_path in main_config.get("extends", []):
    profile_content = load_yaml(profile_path)
    final_config = merge(final_config, profile_content)

# 2. Merge the main config file over the profiles
final_config = merge(final_config, main_config)

# 3. Merge CLI --set overrides
cli_overrides = parse_cli_set_flags() # e.g., {"sources.chembl.batch_size": 20}
final_config = merge(final_config, cli_overrides)

# 4. Merge environment variable overrides
env_overrides = parse_env_vars() # e.g., {"BIOETL__SOURCES__CHEMBL__API_KEY": "..."}
final_config = merge(final_config, env_overrides)

# 5. Validate the final merged dictionary against the PipelineConfig Pydantic model
validated_config = PipelineConfig(**final_config)
```

## 3. Reference: `PipelineConfig` Structure

The following table details every key available in the `PipelineConfig` model, organized by its top-level block.

| Key | Type | Description & Constraints | Default |
| --- | --- | --- | --- |
| **`version`** | `int` | **Required.** The version of the config schema. Must be `1`. | `N/A` |
| **`extends`** | `list[str]` | A list of profile paths to extend, relative to `configs/profiles/`. | `[]` |
| **`pipeline`** | `object` | **Required.** Metadata about the pipeline itself. | `N/A` |
| `pipeline.name` | `str` | **Required.** The unique name of the pipeline. | `N/A` |
| `pipeline.entity` | `str` | **Required.** The business entity the pipeline processes (e.g., `activity`). | `N/A` |
| `pipeline.version` | `str` | Semantic version of the pipeline's definition. | `"1.0.0"` |
| `pipeline.release_scope` | `bool` | If true, artifacts are scoped to a release-specific directory. | `true` |
| **`http`** | `dict[str, object]` | **Required.** A dictionary of named HTTP client profiles. | `N/A` |
| `http.<profile>.timeout_sec` | `float` | Total request timeout in seconds. Must be `> 0`. | `60.0` |
| `http.<profile>.retries.total`| `int` | Total number of retries on failure. Must be `>= 0`. | `5` |
| `http.<profile>.retries.backoff_multiplier` | `float` | Multiplier for exponential backoff. Must be `> 0`. | `2.0` |
| `http.<profile>.rate_limit.max_calls` | `int` | Max calls per period. Must be `>= 1`. | `10` |
| `http.<profile>.rate_limit.period` | `float` | Rate limit period in seconds. Must be `> 0`. | `1.0` |
| `http.<profile>.headers` | `dict[str, str]` | Default headers for requests. Supports `env:` substitution. | `{}` |
| **`sources`** | `dict[str, object]` | Configuration for each data source. | `{}` |
| `sources.<name>.enabled` | `bool` | Enable or disable the source. | `true` |
| `sources.<name>.base_url` | `str` | **Required.** The base URL for the source's API. | `N/A` |
| `sources.<name>.api_key` | `str` | API key. Can be an `env:VAR` reference. | `null` |
| `sources.<name>.http_profile`| `str` | The name of a profile from the top-level `http` block to use. | `null` |
| `sources.<name>.batch_size` | `int` | The number of items to request per batch. Must be `>= 1`. | `null` |
| **`cache`** | `object` | Configuration for the HTTP request cache. | `N/A` |
| `cache.enabled` | `bool` | Enable or disable caching. | `true` |
| `cache.directory` | `str` | Cache directory, relative to `paths.cache_root`. | `"http_cache"` |
| `cache.ttl` | `int` | Cache Time-To-Live in seconds. Must be `>= 0`. | `86400` |
| `cache.maxsize` | `int` | Maximum number of items in the cache. Must be `>= 1`. | `1024` |
| **`paths`** | `object` | Defines root directories for I/O operations. | `N/A` |
| `paths.input_root` | `str` | Root directory for input files. | `"data/input"` |
| `paths.output_root` | `str` | Root directory for output files. | `"data/output"` |
| `paths.cache_root` | `str` | Root directory for cache files. | `".cache"` |
| **`determinism`** | `object` | Settings for ensuring reproducible outputs. | `N/A` |
| `determinism.hash_algorithm`| `str` | Algorithm for row hashing (e.g., `sha256`). | `"sha256"` |
| `determinism.float_precision`| `int` | Decimal precision for floats during serialization for hashing. | `6` |
| `determinism.sort.by` | `list[str]` | A list of column names to sort by before writing. | `[]` |
| `determinism.sort.ascending`| `list[bool]`| Sort directions corresponding to `sort.by`. | `[]` |
| `determinism.column_order` | `list[str]` | A canonical list of column names to enforce order. | `[]` |
| **`materialization`** | `object` | Configuration for writing data artifacts. | `N/A` |
| `materialization.root` | `str` | The root directory for all materialized outputs. | `"data/output"` |
| `materialization.default_format` | `str` | The default file format (e.g., `parquet`). | `"parquet"` |
| `materialization.stages.<name>.directory` | `str` | Subdirectory for a given stage (e.g., `primary`). | `null` |
| **`fallbacks`** | `object` | Global configuration for source fallback behavior. | `N/A` |
| `fallbacks.enabled` | `bool` | Enable or disable fallback mechanisms. | `true` |
| `fallbacks.prefer_cache`| `bool` | If true, use cached data before attempting a network call. | `true` |

## 4. Environment Variable Overrides

Any configuration key can be overridden using an environment variable. The variable name is constructed by joining the nested keys with a double underscore (`__`).

- **Prefix**: `BIOETL`
- **Separator**: `__`

**Example:**

To override the ChEMBL source batch size, you would set the following environment variable:

```bash
export BIOETL__SOURCES__CHEMBL__BATCH_SIZE=20
```

To override the default HTTP timeout:

```bash
export BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=120.0
```

## 5. Usage Example

This example shows a simplified pipeline configuration that inherits from both `base.yaml` and `determinism.yaml`, defines a source, and sets a pipeline-specific sort key.

### 5.1. Files

**`configs/profiles/base.yaml`:** (As created in the previous step)
**`configs/profiles/determinism.yaml`:** (As created in the previous step)

**`configs/pipelines/chembl/activity.yaml`:**

```yaml
# Inherit from both base and determinism profiles.
# `determinism` overrides `base` if there are conflicting keys.
extends:
  - base.yaml
  - determinism.yaml

version: 1

pipeline:
  name: "chembl-activity"
  entity: "activity"
  version: "2.1.0"

sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    # Use the 'default' client defined in `base.yaml`.
    http_profile: "default"
    batch_size: 25
    api_key: "env:CHEMBL_API_KEY" # Securely load from environment

# Override determinism settings with a pipeline-specific sort order.
determinism:
  sort:
    by: ["activity_id", "doc_id"]
    ascending: [true, true]
```

### 5.2. Running the Pipeline with Overrides

This command executes the `chembl-activity` pipeline and uses `--set` to override the configured batch size for a smaller test run.

```bash
python -m bioetl.cli.main run chembl-activity \
  --config configs/pipelines/chembl/activity.yaml \
  --set sources.chembl.batch_size=10
```

In this scenario, the final `batch_size` of `10` is used because the `--set` flag has higher precedence than the value of `25` set in `activity.yaml`.

## 6. Validation Error Examples

Because the configuration is parsed by a strict Pydantic model, any deviation from the defined schema will result in a validation error. This prevents common mistakes like typos or incorrect data types.

### Example 1: Typo in a Key

**Invalid `activity.yaml`:**

```yaml
# ...
sources:
  chembl:
    base_url: "..."
    # Typo: `bacth_size` instead of `batch_size`
    bacth_size: 25
```

**Pydantic Error Message (at startup):**

```
pydantic_core._pydantic_core.ValidationError: 1 validation error for PipelineConfig
sources.chembl
  Extra inputs are not permitted [type=extra_forbidden, input_value=25, input_type=int]
```
This error clearly indicates that `bacth_size` is not a valid field within the `Source` model.

### Example 2: Incorrect Data Type

**Invalid `activity.yaml`:**

```yaml
# ...
http:
  default:
    # Invalid type: `timeout_sec` must be a float, not a string.
    timeout_sec: "sixty"
```

**Pydantic Error Message (at startup):**

```
pydantic_core._pydantic_core.ValidationError: 1 validation error for PipelineConfig
http.default.timeout_sec
  Input should be a valid number, unable to parse string as a number [type=float_parsing, input_value='sixty', input_type=str]
```
This error shows that the string `"sixty"` could not be parsed into the expected `float` type.

## 7. Test-Plan Configs

For testing and continuous integration, it is often necessary to run pipelines with reduced datasets or against mock APIs. The layered configuration system is well-suited for this. A common pattern is to create a test-specific configuration profile.

**`configs/profiles/test.yaml`:**

```yaml
# This profile overrides production settings for CI/CD and testing.

# Use a mock API server instead of the real ChEMBL API.
sources:
  chembl:
    base_url: "http://mock-chembl-api:8000/data"
    # Disable API key for the mock server.
    api_key: null

# Reduce timeouts and retries for faster test execution.
http:
  default:
    timeout_sec: 10.0
    retries:
      total: 1

# Disable caching to ensure tests are not affected by stale data.
cache:
  enabled: false
```

**Running a pipeline with the test profile:**

To run the `chembl-activity` pipeline in test mode, you would simply include `test.yaml` in the `extends` list.

**`configs/pipelines/chembl/activity.test.yaml`:**

```yaml
# Inherit from all three profiles. Order matters.
# `test.yaml` will override `determinism.yaml` and `base.yaml`.
extends:
  - base.yaml
  - determinism.yaml
  - test.yaml

version: 1

pipeline:
  name: "chembl-activity-test"
  entity: "activity"
  version: "2.1.0"

# Keep the rest of the config the same...
```

This approach keeps the core pipeline logic unchanged while allowing for a complete swap of external dependencies and runtime parameters, making the pipeline highly testable.
