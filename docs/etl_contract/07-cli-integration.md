# 7. CLI Integration

## Overview

The `bioetl` framework includes a powerful and user-friendly Command-Line Interface (CLI) for managing and executing ETL pipelines. The CLI is the primary entry point for running pipelines in production, development, and testing environments. It is built using the [Typer](https://typer.tiangolo.com/) library, which provides a clean, modern interface with automatic help generation and argument parsing.

## Pipeline Discovery and Registration

Pipelines are discovered and registered with the CLI automatically. As long as a new pipeline class inherits from `PipelineBase` and is correctly placed within the `src/bioetl/pipelines/` directory, the framework's registry will find it and make it available as a CLI command.

This convention-based approach means that developers do not need to write any boilerplate code to integrate their new pipelines with the CLI.

## Core Commands

The CLI is invoked via `python -m bioetl.cli.main`. It provides a set of standard commands for interacting with the framework.

### `list`

The `list` command displays all currently registered and available pipelines. This is the first command a user should run to see which pipelines they can execute.

**Usage:**
```bash
python -m bioetl.cli.main list
```

**Example Output:**
```
Available Pipelines:
- chembl_activity
- chembl_assay
- chembl_target
- uniprot_protein
```

### `<pipeline_name>`

Each registered pipeline is available as a subcommand. This is the command used to execute a pipeline run.

**Usage:**
```bash
python -m bioetl.cli.main <pipeline_name> [OPTIONS]
```

**Example:**
```bash
python -m bioetl.cli.main chembl_activity --output-dir /data/output/chembl/activity-20231027
```

## Standard Command-Line Arguments

Every pipeline command supports a standard set of command-line arguments that allow users to override settings in the YAML configuration at runtime.

-   `--config TEXT`: Specifies the path to the pipeline's YAML configuration file. If not provided, the framework will look for a default configuration file matching the pipeline's name.

-   `--output-dir DIRECTORY`: **(Required)** Specifies the root directory where the output artifacts (the dataset and its `meta.yaml` file) will be written.

-   `--input-file PATH`: (Optional) For pipelines that read from a local file instead of an API, this argument allows the user to specify the path to the input file.

-   `--limit INTEGER`: (Optional) A convenient option for development and testing. It limits the pipeline to processing only the first `N` records.

-   `--dry-run`: This is an invaluable tool for validating a pipeline's configuration. When this flag is present, the pipeline will execute all stages up to and including the `validate` stage, but it **will not write any files**. A dry run is used to:
    -   Verify that the YAML configuration is valid.
    -   Confirm that the pipeline can successfully connect to the data source.
    -   Run a small amount of data through the `extract` and `transform` stages to ensure the logic is working.
    -   Perform a validation check on the transformed data.

**Example of a `--dry-run`:**
```bash
# This command will connect to the ChEMBL API, fetch a few records,
# transform and validate them, and then exit without writing any output.
# It is a perfect way to check that the pipeline is configured correctly.
python -m bioetl.cli.main chembl_activity --output-dir /tmp/test --limit 10 --dry-run
```

By providing a consistent and powerful CLI, the framework makes the process of running, testing, and debugging pipelines straightforward and repeatable for all users.
