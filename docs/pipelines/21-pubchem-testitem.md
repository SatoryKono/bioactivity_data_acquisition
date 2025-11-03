# Pipeline: `testitem_pubchem`

This document describes the `testitem_pubchem` pipeline, which is responsible for extracting testitem data from PubChem.

**Note:** This pipeline is not yet implemented in the `test_refactoring_32` branch. This document serves as a specification for its future implementation.

## 1. Identification

| Item              | Value                                                                                              | Status                |
| ----------------- | -------------------------------------------------------------------------------------------------- | --------------------- |
| **Pipeline Name** | `testitem_pubchem`                                                                                 | Not Implemented       |
| **CLI Command**   | `python -m bioetl.cli.main testitem_pubchem`                                                       | Not Implemented       |
| **Config File**   | [ref: repo:src/bioetl/configs/pipelines/pubchem/testitem_pubchem.yaml@test_refactoring_32]     | Not Implemented       |
| **CLI Registration** | [ref: repo:src/bioetl/cli/registry.py@test_refactoring_32]                                          | Not Implemented       |

## 2. Purpose and Scope

This pipeline is designed to extract `testitem` data from PubChem. It is a standalone pipeline that does not perform any joins or enrichment with other data sources.

The pipeline utilizes the existing PubChem source components, which can be found at [ref: repo:src/bioetl/sources/pubchem/@test_refactoring_32].

## 3. Inputs (CLI/Configs/Profiles)

### CLI Flags

The pipeline would support the following standard CLI flags:

| Flag              | Description                                                                 |
| ----------------- | --------------------------------------------------------------------------- |
| `--config`        | Path to a pipeline-specific configuration file.                               |
| `--output-dir`    | Directory to write the output artifacts to.                                 |
| `--dry-run`       | Run the pipeline without writing any output.                                |
| `--limit`         | Limit the number of records to process.                                     |
| `--profile`       | Apply a configuration profile (e.g., `determinism`).                         |

### Configuration Merge Order

The configuration is loaded in the following order, with later sources overriding earlier ones:

1.  **Base Profile:** `src/bioetl/configs/base.yaml`
2.  **Profile:** e.g., `src/bioetl/configs/includes/determinism.yaml` (activated by `--profile determinism`)
3.  **Explicit Config:** The file specified by the `--config` flag.
4.  **CLI Flags:** Any flags that override configuration values (e.g., `--limit`).

### Configuration Keys

The following table describes the expected keys in the `testitem_pubchem.yaml` configuration file. See [ref: repo:src/bioetl/configs/models.py@test_refactoring_32] for the underlying configuration models.

| Key                             | Type    | Required | Default | Description                                                                 |
| ------------------------------- | ------- | -------- | ------- | --------------------------------------------------------------------------- |
| `pipeline.name`                 | string  | Yes      |         | The name of the pipeline (e.g., `testitem_pubchem`).                           |
| `pipeline.version`              | string  | Yes      |         | The version of the pipeline.                                                |
| `sources.pubchem.base_url`      | string  | No       | `https://pubchem.ncbi.nlm.nih.gov/rest/pug` | The base URL for the PubChem API.                                           |
| `sources.pubchem.rate_limit_max_calls` | integer | No       | 5       | The maximum number of API calls per period.                                 |
| `sources.pubchem.rate_limit_period`    | float   | No       | 1.0     | The period in seconds for the rate limit.                                   |
| `materialization.pipeline_subdir` | string  | Yes      |         | The subdirectory within the output directory to write artifacts to.         |

## 4. Extraction (Client → Paginator → Parser)

The extraction process would use the existing components from the PubChem source module.

- **Client:** The `PubChemClient` ([ref: repo:src/bioetl/sources/pubchem/client/client.py@test_refactoring_32]) would be responsible for making HTTP requests to the PubChem API. It would handle timeouts, retries with backoff, and rate limiting as configured in the pipeline's YAML file. Log records would include fields such as `endpoint`, `attempt`, and `duration_ms`.
- **Paginator:** A paginator, likely based on an offset/limit strategy, would be used to iterate through the PubChem search results. The paginator would handle the details of fetching pages of data until the end of the result set is reached. It would also respect rate limits and introduce pauses if necessary.
- **Parser:** A parser ([ref: repo:src/bioetl/sources/pubchem/parser/parser.py@test_refactoring_32]) would be responsible for parsing the JSON response from the PubChem API. It would extract the relevant fields for `testitem` data and raise errors if required fields are missing or invalid.

The specific PubChem endpoint and query parameters for `testitem` data would need to be determined and implemented in the pipeline's extraction logic.

## 5. Normalization and Validation

- **Normalizer:** The `PubChemNormalizer` ([ref: repo:src/bioetl/sources/pubchem/normalizer/normalizer.py@test_refactoring_32]) would be used to canonicalize identifiers and types, and to fill in any required fields that are not present in the raw extracted data.
- **Pandera Schema:** A Pandera schema ([ref: repo:src/bioetl/sources/pubchem/schema/schema.py@test_refactoring_32]) would be used to validate the structure and types of the normalized data. The schema would be configured with `strict=True`, `ordered=True`, and `coerce=True` to ensure data quality. It would also define a business key and perform a uniqueness check on that key.

## 6. Outputs and Determinism

- **Artifact Format:** The pipeline would produce a single output file in either CSV or Parquet format, as configured.
- **Sort Keys:** The output data would be sorted by a stable key (e.g., a unique `testitem` identifier from PubChem) to ensure deterministic output.
- **Hashing:** Each row would have a `hash_row` and `hash_business_key` column. The `hash_row` would be a hash of the entire row's data, while the `hash_business_key` would be a hash of the columns that uniquely identify a `testitem`.
- **`meta.yaml`:** A `meta.yaml` file would be generated alongside the data artifact, containing the following information:
    - `dataset`: The name of the dataset (e.g., `testitem_pubchem`).
    - `pipeline`: The name and version of the pipeline.
    - `schema_version`: The version of the Pandera schema used.
    - `column_order`: The exact order of the columns in the output file.
    - `row_count`: The number of rows in the output file.
    - `business_key`: The list of columns that make up the business key.
    - `hash_algo`: The hashing algorithm used (e.g., `sha256`).
    - `inputs/outputs`: A list of input and output files.
    - `config_fingerprint`: A hash of the configuration used for the pipeline run.
    - `generated_at_utc`: The timestamp of the pipeline run.

## 7. QC Metrics (Extraction Level)

The following QC metrics would be collected and reported in the pipeline's logs and `meta.yaml` file:

| Metric                  | Description                                                                 |
| ----------------------- | --------------------------------------------------------------------------- |
| `response_count`        | The total number of responses received from the PubChem API.                |
| `pages_total`           | The total number of pages retrieved from the PubChem API.                   |
| `duplicate_count`       | The number of duplicate records found based on the business key.            |
| `missing_required_fields` | The number of records with missing required fields.                         |
| `retry_events`          | The number of times the HTTP client had to retry a request.                 |

## 8. Errors and Exit Codes

The pipeline would use the following exit codes to indicate success or failure:

| Exit Code | Category                | Description                                                                 |
| --------- | ----------------------- | --------------------------------------------------------------------------- |
| 0         | Success                 | The pipeline completed successfully.                                        |
| 1         | Application Error       | A fatal error occurred, such as a network error or a bug in the code.       |
| 2         | Usage Error             | An error occurred due to invalid configuration or command-line arguments.   |

Diagnostic messages would be logged to the console and/or a log file, providing details about the cause of the error.

## 9. Usage Examples

### Minimal Run

```bash
python -m bioetl.cli.main testitem_pubchem \
  --config src/bioetl/configs/pipelines/pubchem/testitem_pubchem.yaml \
  --output-dir data/output/testitem_pubchem
```

### Dry Run

```bash
python -m bioetl.cli.main testitem_pubchem \
  --config src/bioetl/configs/pipelines/pubchem/testitem_pubchem.yaml \
  --output-dir data/output/testitem_pubchem \
  --dry-run
```

### With Determinism Profile

```bash
python -m bioetl.cli.main testitem_pubchem \
  --config src/bioetl/configs/pipelines/pubchem/testitem_pubchem.yaml \
  --output-dir data/output/testitem_pubchem \
  --profile determinism
```
