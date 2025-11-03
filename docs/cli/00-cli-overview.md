# CLI Overview and Principles

## 1. Purpose and Scope

The `bioetl` Command-Line Interface (CLI) is the primary entry point for executing and managing ETL pipelines. It serves as a user-facing frontend to the `PipelineBase` orchestration framework, providing a standardized way to run pipelines, manage configurations, and inspect results.

The CLI is designed around the same core principles as the `PipelineBase` orchestrator: determinism, reproducibility, and clarity. It handles the "scaffolding" of a pipeline run, including configuration loading, logging setup, and parameter parsing, allowing the pipeline itself to focus solely on the `extract` → `transform` → `validate` → `write` lifecycle.

[ref: repo:README.md@test_refactoring_32]

## 2. Architecture

The CLI is a single application built with [Typer](https://typer.tiangolo.com/), located at `src/bioetl/cli/app.py`. Its architecture is based on a dynamic command registry.

-   **Application Entry Point**: The main entry point is `python -m bioetl.cli.main`.
-   **Command Registry**: The CLI does not hardcode pipeline commands. Instead, it discovers and registers them at runtime. Any pipeline class that inherits from `PipelineBase` and is correctly placed in the `src/bioetl/pipelines/` directory is automatically made available as a subcommand (e.g., `activity`, `assay`). This is handled by the registration logic in `src/bioetl/cli/app.py`.
-   **Pipeline Factory**: When a command like `activity` is invoked, the CLI uses a factory pattern to find and instantiate the corresponding `ActivityPipeline` class, injecting the required `PipelineConfig` object.

[ref: repo:src/bioetl/cli/app.py@test_refactoring_32]

## 3. Configuration Profiles

The CLI is responsible for loading and merging all configuration files into a single `PipelineConfig` object. The merging follows a strict order of precedence, where later sources override earlier ones.

1.  **Base Profiles (`extends`)**: The `extends` key in a YAML file loads one or more base profiles. By convention, all pipelines extend `base.yaml` and `determinism.yaml`. The profiles are merged in the order they are listed.
2.  **Main Config File**: The values in the main pipeline config file (e.g., `activity.yaml`) are merged next, overriding any values from the base profiles.
3.  **CLI Overrides (`--set`)**: Any values passed via the `--set KEY=VALUE` flag are merged next, providing a way to override specific settings for a single run.
4.  **Environment Variables**: Finally, any environment variables with a `BIOETL_` prefix are applied, having the highest precedence. For example, `BIOETL_SOURCES__CHEMBL__BATCH_SIZE=50` would override the batch size.

This layered approach provides a powerful and flexible system for managing configurations across different environments.

[ref: repo:src/bioetl/config/loader.py@test_refactoring_32]

## 4. Logging and Output

-   **Log Format**: By default, the CLI emits human-readable `key=value` logs to the console. In production or containerized environments, it is recommended to configure the logger for `json` output. The verbosity can be increased with the `--verbose` flag.
-   **`--dry-run` Behavior**: When the `--dry-run` flag is used, the CLI executes all pipeline stages up to and including `validate`, but it **will not write any files**. The process will exit after confirming that the configuration is valid and the initial data can be processed. This is an essential tool for development and debugging.
-   **Artifact Location**: All output artifacts, including the final dataset, quality reports, and the `meta.yaml` file, are saved in the directory specified by the required `--output-dir` flag.

## 5. Exit Codes and Error Handling

The CLI uses standard exit codes to signal the outcome of a pipeline run, making it suitable for use in automated scripting and orchestration tools (e.g., Airflow, cron).

| Exit Code | Meaning                   | Description                                                                                             |
| --------- | ------------------------- | ------------------------------------------------------------------------------------------------------- |
| `0`       | **Success**               | The pipeline completed all stages successfully and all output artifacts were written.                   |
| `1`       | **Pipeline Failure**      | A general, non-specific error occurred during one of the pipeline stages (e.g., a network error, a transformation logic error). The logs will contain the full traceback. |
| `2`       | **Invalid Parameters**    | The command was called with invalid or conflicting flags (e.g., `--sample -1`). This is a Typer-managed error. |
| `(Other)` | **Configuration Failure** | A non-zero exit code will also be returned for issues like a missing or malformed configuration file. |
