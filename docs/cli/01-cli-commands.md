# CLI Command Reference

## 1. Global Commands

### `list`

The `list` command displays all pipelines that are currently discovered and registered with the CLI.

**Synopsis:**
```bash
python -m bioetl.cli.main list
```

**Flags:**
This command takes no flags.

**Example Output:**
```
Available Pipelines:
- activity
- assay
- document
- target
- testitem
```

**Exit Codes:**
-   `0`: Success.

---

## 2. Pipeline Commands

This section details the commands used to execute specific ETL pipelines. All pipeline commands share a common set of flags.

### Standard Flags for All Pipelines

| Flag                          | Alias | Type          | Required? | Description                                                                                    |
| ----------------------------- | ----- | ------------- | --------- | ---------------------------------------------------------------------------------------------- |
| `--output-dir`                | `-o`  | `DIRECTORY`   | **Yes**   | The root directory where all output artifacts will be written.                                 |
| `--config`                    |       | `PATH`        | No        | Path to the pipeline's YAML configuration. Defaults to the pipeline's standard config file.      |
| `--input-file`                | `-i`  | `PATH`        | No        | Path to a local input file, if the pipeline uses one (e.g., a seed list).                       |
| `--dry-run`                   | `-d`  | `BOOLEAN`     | No        | Run all stages up to `validate` but do not write any output files.                               |
| `--sample`                    |       | `INTEGER`     | No        | Process only the first N records. Useful for testing and development.                          |
| `--limit`                     |       | `INTEGER`     | No        | (Deprecated) An alias for `--sample`.                                                          |
| `--extended` / `--no-extended`|       | `BOOLEAN`     | No        | Generate extended QC artifacts (e.g., correlation reports). Default is `False`.                 |
| `--set`                       | `-S`  | `KEY=VALUE`   | No        | Override a specific configuration value from the command line. Can be used multiple times.     |
| `--verbose`                   | `-v`  | `BOOLEAN`     | No        | Enable verbose (DEBUG level) logging.                                                          |

### `activity`

Executes the ChEMBL activity data pipeline.

**Synopsis:**
```bash
python -m bioetl.cli.main activity \
  --output-dir <path/to/output/dir> \
  [--config <path/to/activity.yaml>] \
  [--dry-run]
```

**Description:**
This pipeline extracts activity data points from the ChEMBL database. A `--dry-run` will test the connection to the ChEMBL API and validate the configuration and transformation logic for a small sample of data.

**Examples:**

*   **Minimal Run:**
    ```bash
    python -m bioetl.cli.main activity --output-dir data/output/activity
    ```

*   **Dry Run with a Small Sample:**
    ```bash
    python -m bioetl.cli.main activity --output-dir /tmp/activity-test --sample 10 --dry-run
    ```

**Artifacts:**
-   `activity_*.parquet`: The main dataset.
-   `meta.yaml`: The run metadata file.
-   `qc/`: Directory containing quality control reports.

### `assay`

Executes the ChEMBL assay data pipeline.

**Synopsis:**
```bash
python -m bioetl.cli.main assay \
  --output-dir <path/to/output/dir> \
  [--config <path/to/assay.yaml>] \
  [--dry-run]
```
*(Examples and Artifacts are analogous to the `activity` command.)*

### `target`

Executes the ChEMBL target data pipeline.

**Synopsis:**
```bash
python -m bioetl.cli.main target \
  --output-dir <path/to/output/dir> \
  [--config <path/to/target.yaml>] \
  [--dry-run]
```
*(Examples and Artifacts are analogous to the `activity` command.)*

### `document`

Executes the ChEMBL document data pipeline.

**Synopsis:**
```bash
python -m bioetl.cli.main document \
  --output-dir <path/to/output/dir> \
  [--config <path/to/document.yaml>] \
  [--dry-run]
```
*(Examples and Artifacts are analogous to the `activity` command.)*

### `testitem`

Executes the ChEMBL test item (molecule) data pipeline.

**Synopsis:**
```bash
python -m bioetl.cli.main testitem \
  --output-dir <path/to/output/dir> \
  [--config <path/to/testitem.yaml>] \
  [--dry-run]
```
*(Examples and Artifacts are analogous to the `activity` command.)*

## 3. CLI Test Plan

-   **Golden Help Tests**: A test suite should capture the output of `python -m bioetl.cli.main <command> --help` for each command and compare it against a "golden" file to detect any changes in flags or help text.
-   **`list` Command Integration Test**: An integration test should run `python -m bioetl.cli.main list` and assert that the output contains the exact list of documented pipeline commands (`activity`, `assay`, etc.). This verifies that the command registry is working as expected.
-   **`--dry-run` Invariant Test**: An integration test for each pipeline command should run with the `--dry-run` flag and assert that the command completes with an exit code of `0` and that **no files** are created in the specified `--output-dir`. This is a critical test of the CLI's side-effect contract.
