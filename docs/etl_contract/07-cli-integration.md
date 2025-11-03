# 7. CLI Integration

## Overview

The `bioetl` framework includes a powerful and user-friendly Command-Line Interface (CLI) for managing and executing ETL pipelines. The CLI is the primary entry point for running pipelines in production, development, and testing environments. It is built using the [Typer](https://typer.tiangolo.com/) library, which provides a clean, modern interface with automatic help generation and argument parsing.

## Pipeline Discovery and Registration

The CLI uses a **static command registry**, which is defined in `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]`. This file explicitly imports and configures each available pipeline command.

This approach is **not dynamic**. Adding a new pipeline requires explicitly adding its configuration to the `registry.py` file. This ensures that the list of available commands is always explicit and predictable.

## Core Commands

The CLI is invoked via `python -m bioetl.cli.main`.

### `list`

The `list` command displays all currently registered pipeline commands.

**Usage:**
```bash
python -m bioetl.cli.main list
```

### `<pipeline-name>`

Each registered pipeline is available as a subcommand (e.g., `activity`, `assay`). This is the command used to execute a pipeline run.

**Usage:**
```bash
python -m bioetl.cli.main <pipeline-name> [OPTIONS]
```

**Example:**
```bash
python -m bioetl.cli.main activity --config configs/pipelines/chembl/activity.yaml --output-dir /data/output/activity
```

## Standard Command-Line Arguments

Every pipeline command supports a standard set of command-line arguments.

-   `--config PATH`: **(Required)** Specifies the path to the pipeline's YAML configuration file.

-   `--output-dir DIRECTORY`: **(Required)** Specifies the root directory where the output artifacts will be written.

-   `--input-file PATH`: (Optional) Specifies the path to a local input file for pipelines that require it.

-   `--set TEXT`: (Optional) Overrides a specific configuration value using dot notation (e.g., `--set sources.chembl.batch_size=10`). Can be used multiple times.

-   `--dry-run`: When this flag is present, the pipeline will execute all stages up to and including validation, but it **will not write any files**. It is an essential tool for verifying that a configuration is valid.

**Example of a `--dry-run`:**
```bash
# This command will connect to the ChEMBL API, fetch a few records,
# transform and validate them, and then exit without writing any output.
# It is a perfect way to check that the pipeline is configured correctly.
python -m bioetl.cli.main chembl_activity --output-dir /tmp/test --limit 10 --dry-run
```

By providing a consistent and powerful CLI, the framework makes the process of running, testing, and debugging pipelines straightforward and repeatable for all users.
