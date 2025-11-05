# CLI Overview and Principles

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

## 1. Purpose and Scope

The `bioetl` Command-Line Interface (CLI) is the primary entry point for executing and managing ETL pipelines. It provides a standardized way to run pipelines, manage configurations, and pass runtime parameters.

The CLI's main responsibility is to handle the "scaffolding" of a pipeline run. This includes:
- Parsing command-line arguments.
- Loading and merging all configuration sources.
- Setting up the logging system.
- Instantiating the correct pipeline class and injecting its configuration.
- Reporting the final status of the run.

## 2. Architecture: A Static Registry

The CLI is a Typer application whose main entry point is `python -m bioetl.cli.main`. Its architecture is based on a **static command registry** that is built when the application starts.

- **Command Registration**: The file `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` defines the list of all available pipeline commands (e.g., `activity_chembl`, `assay_chembl`, `target`). It explicitly imports a `build_command_config` function for each pipeline and uses these to construct a dictionary that maps command names to their configurations.
- **Application Startup**: The main application file, `[ref: repo:src/bioetl/cli/app.py@refactoring_001]`, reads this static registry and uses a factory pattern (`create_pipeline_command`) to generate and register a Typer command for each entry.

This approach is **not dynamic**. Adding a new pipeline requires explicitly adding its configuration to `registry.py`.

## 3. Configuration Loading and Precedence

The single most important function of the CLI is to build the `PipelineConfig` object that will be passed to the pipeline. It does this by loading and merging settings from multiple sources in a strict order of precedence. This entire process is managed by the `load_config` function found in `[ref: repo:src/bioetl/config/loader.py@refactoring_001]`.

The order of precedence is as follows (where 4 has the highest precedence and overrides all others):

**Order of Precedence (Lowest to Highest):**

1. **Base Profiles**: Files listed in the `extends` key are loaded first. This typically includes `base.yaml` and can also include `network.yaml` (for network settings) and `determinism.yaml` (for reproducibility settings).
2. **Pipeline Config**: The main pipeline-specific YAML file provided via `--config`.
3. **CLI `--set` Flags**: Key-value pairs from the `--set` flag are merged next.
4. **Environment Variables**: Environment variables have the highest precedence (e.g., `BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=120`).

This layered approach provides a powerful and flexible system for managing configurations.

## 4. Key Flags and Behavior

- **`--config`**: The **required** path to the main YAML configuration file for the pipeline.
- **`--output-dir`**: The **required** directory where all output artifacts will be saved.
- **`--set`**: Overrides a specific configuration value using dot notation (e.g., `--set sources.chembl.batch_size=10`). Can be repeated.
- **`--dry-run`**: Executes all pipeline setup, including configuration loading and validation, but **stops before running the pipeline**. It is an essential tool for verifying that a configuration is valid.
- **`--verbose`**: Increases the logging level to provide more detailed output for debugging.

A full list of commands and their specific flags can be found in the `[ref: repo:docs/cli/01-cli-commands.md@refactoring_001]`. For a detailed reference on exit codes, see `[ref: repo:docs/cli/02-cli-exit_codes.md@refactoring_001]`.

## 5. Command Catalog

The Typer application defined in `[ref: repo:src/bioetl/cli/app.py@refactoring_001]` loads the static registry from `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` and registers each pipeline command at startup. The resulting command surface is summarized below.

| Command | Invocation | Description | Source |
| --- | --- | --- | --- |
| Root | `python -m bioetl.cli.main` | Entry point that exposes all subcommands and global options. | `[ref: repo:src/bioetl/cli/app.py@refactoring_001]` |
| `list` | `python -m bioetl.cli.main list` | Prints the names of every registered pipeline so operators can discover available commands. | `[ref: repo:src/bioetl/cli/app.py@refactoring_001]` |
| `activity` | `python -m bioetl.cli.main activity` | Runs the ChEMBL activity ETL pipeline. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |
| `assay` | `python -m bioetl.cli.main assay` | Runs the ChEMBL assay ETL pipeline. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |
| `document` | `python -m bioetl.cli.main document` | Runs the ChEMBL document ETL pipeline with external enrichers. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |
| `target` | `python -m bioetl.cli.main target` | Runs the ChEMBL target ETL pipeline. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |
| `testitem` | `python -m bioetl.cli.main testitem` | Runs the ChEMBL test item ETL pipeline. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |
| `pubchem` | `python -m bioetl.cli.main pubchem` | Runs the PubChem enrichment pipeline. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |
| `gtp_iuphar` | `python -m bioetl.cli.main gtp_iuphar` | Runs the Guide to Pharmacology enrichment pipeline. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |
| `uniprot` | `python -m bioetl.cli.main uniprot` | Runs the UniProt enrichment pipeline. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |
| `openalex` | `python -m bioetl.cli.main openalex` | Runs the OpenAlex enrichment pipeline. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |
| `crossref` | `python -m bioetl.cli.main crossref` | Runs the Crossref enrichment pipeline. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |
| `pubmed` | `python -m bioetl.cli.main pubmed` | Runs the PubMed enrichment pipeline. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |
| `semantic_scholar` | `python -m bioetl.cli.main semantic_scholar` | Runs the Semantic Scholar enrichment pipeline. | `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]` |

## 6. Global Options

Every pipeline command shares a common set of options implemented in `[ref: repo:src/bioetl/cli/command.py@refactoring_001]`. These options are added when `create_pipeline_command` wires a pipeline into the Typer application defined in `[ref: repo:src/bioetl/cli/app.py@refactoring_001]`.

| Option | Type | Default | Notes |
| --- | --- | --- | --- |
| `--config` | `Path` | Required | Path to the pipeline YAML that seeds configuration merging. |
| `--output-dir` | `Path` | Required | Destination directory for deterministic CSV outputs and QC artifacts. |
| `--dry-run` (`-d`) | `bool` | `False` | Perform configuration loading and validation without executing pipeline stages. |
| `--set` (`-S`) | `List[str]` | `[]` | Override configuration keys using dotted notation (`KEY=VALUE`); can be provided multiple times. |
| `--verbose` (`-v`) | `bool` | `False` | Elevate logging to verbose mode for troubleshooting. |
| `--fail-on-schema-drift/--allow-schema-drift` | `bool` | `True` | Control whether unexpected column changes abort the run. |
| `--validate-columns/--no-validate-columns` | `bool` | `True` | Toggle the column contract validation step before writing outputs. |
| `--extended/--no-extended` | `bool` | `False` | Enable additional QC artifacts beyond the default CSV outputs. |
| `--sample` | `Optional[int]` | `None` | Limit execution to the first *N* records for smoke testing. |
| `--golden` | `Optional[Path]` | `None` | Provide a golden dataset for deterministic comparisons. |

## 7. Exit Codes at a Glance

Error handling for every pipeline run is centralized in `[ref: repo:src/bioetl/cli/command.py@refactoring_001]`, which maps exceptions to exit codes documented in `[ref: repo:docs/cli/02-cli-exit_codes.md@refactoring_001]`. The following table summarizes the outcomes.

| Exit Code | Meaning | Typical Triggers |
| --- | --- | --- |
| `0` | Successful execution. | Pipeline stages completed or `--dry-run` finished without errors. |
| `1` | Application-level failure. | Configuration validation errors, Pandera schema violations, or unhandled pipeline exceptions. |
| `2` | Usage error detected by Typer. | Missing required options or invalid flag/value combinations. |

## 8. Usage Examples

The snippets below demonstrate reproducible invocations that rely on the command definitions in `[ref: repo:src/bioetl/cli/app.py@refactoring_001]` and configuration handling in `[ref: repo:src/bioetl/cli/command.py@refactoring_001]`.

1. **List available pipelines**

   ```bash
   python -m bioetl.cli.main list
   ```

   *Expected output*: A plain-text list of command names such as `activity`, `assay`, and `document` sourced from the static registry.

2. **Dry-run the ChEMBL activity pipeline**

   ```bash
   python -m bioetl.cli.main activity \
     --config src/bioetl/configs/pipelines/chembl/activity_chembl.yaml \
     --output-dir data/output/activity/dry_run \
     --dry-run
   ```

   *Expected output*: Configuration merge summary and validation logs with no CSV files written.

3. **Run the document pipeline with deterministic profiles**

   ```bash
   python -m bioetl.cli.main document \
     --config src/bioetl/configs/pipelines/chembl/document_chembl.yaml \
     --output-dir data/output/document/full_load \
     --set profiles.include="['base.yaml','determinism.yaml']"
   ```

   *Expected output*: ETL progress logs, QC reports, and deterministic CSV outputs under `data/output/document/full_load`.

4. **Execute a PubChem enrichment sample**

   ```bash
    python -m bioetl.cli.main pubchem \
      --config src/bioetl/configs/pipelines/pubchem.yaml \
      --output-dir data/output/pubchem/sample \
      --sample 250
    ```

    *Expected output*: Confirmation that only 250 records were processed along with generated CSV/QC artifacts in the specified directory.
