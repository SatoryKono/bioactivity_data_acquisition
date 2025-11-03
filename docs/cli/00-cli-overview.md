# CLI Overview and Principles

## 1. Purpose and Scope

The `bioetl` Command-Line Interface (CLI) is the primary entry point for executing and managing ETL pipelines. It provides a standardized way to run pipelines, manage configurations, and pass runtime parameters.

The CLI's main responsibility is to handle the "scaffolding" of a pipeline run. This includes:
-   Parsing command-line arguments.
-   Loading and merging all configuration sources.
-   Setting up the logging system.
-   Instantiating the correct pipeline class and injecting its configuration.
-   Reporting the final status of the run.

## 2. Architecture: A Static Registry

The CLI is a Typer application whose main entry point is `python -m bioetl.cli.main`. Its architecture is based on a **static command registry** that is built when the application starts.

-   **Command Registration**: The file `[ref: repo:src/bioetl/cli/registry.py@test_refactoring_32]` defines the list of all available pipeline commands (e.g., `activity`, `assay`, `target`). It explicitly imports a `build_command_config` function for each pipeline and uses these to construct a dictionary that maps command names to their configurations.
-   **Application Startup**: The main application file, `[ref: repo:src/bioetl/cli/app.py@test_refactoring_32]`, reads this static registry and uses a factory pattern (`create_pipeline_command`) to generate and register a Typer command for each entry.

This approach is **not dynamic**. Adding a new pipeline requires explicitly adding its configuration to `registry.py`.

## 3. Configuration Loading and Precedence

The single most important function of the CLI is to build the `PipelineConfig` object that will be passed to the pipeline. It does this by loading and merging settings from multiple sources in a strict order of precedence. This entire process is managed by the `load_config` function found in `[ref: repo:src/bioetl/config/loader.py@test_refactoring_32]`.

The order of precedence is as follows (where 5 has the highest precedence and overrides all others):
1.  `base.yaml`
2.  `network.yaml` / `determinism.yaml` (if extended)
3.  Pipeline-specific `--config` file
4.  CLI `--set` flags
5.  Environment variables (e.g., `BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=120`)


This layered approach provides a powerful and flexible system for managing configurations.

## 4. Key Flags and Behavior

-   **`--config`**: The **required** path to the main YAML configuration file for the pipeline.
-   **`--output-dir`**: The **required** directory where all output artifacts will be saved.
-   **`--set`**: Overrides a specific configuration value using dot notation (e.g., `--set sources.chembl.batch_size=10`). Can be repeated.
-   **`--dry-run`**: Executes all pipeline setup, including configuration loading and validation, but **stops before running the pipeline**. It is an essential tool for verifying that a configuration is valid.
-   **`--verbose`**: Increases the logging level to provide more detailed output for debugging.

A full list of commands and their specific flags can be found in the next document: `[ref: repo:docs/cli/01-cli-commands.md@test_refactoring_32]`.
