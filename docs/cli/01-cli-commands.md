# CLI Commands

This document provides a reference for the commands available in the `bioetl` CLI.

## `run` Commands

All data processing pipelines are available as subcommands. The general syntax is:

```bash
python -m bioetl.cli.main <pipeline-name> [OPTIONS]
```

### Registered Pipelines

The following pipeline commands are registered in the CLI. The registration is statically defined in `[ref: repo:src/bioetl/cli/registry.py@refactoring_001]`.

-   `activity`
-   `assay`
-   `document`
-   `target`
-   `testitem`
-   `pubchem`
-   `gtp_iuphar`
-   `uniprot`
-   `openalex`
-   `crossref`
-   `pubmed`
-   `semantic_scholar`

### Common Options for All `run` Commands

These options are available for all pipeline commands. They are defined in `[ref: repo:src/bioetl/cli/command.py@refactoring_001]`.

| Option | Shorthand | Description | Default |
| --- | --- | --- | --- |
| `--input-file` | `-i` | Path to the seed dataset used during extraction. | Varies by pipeline |
| `--output-dir` | `-o` | **Required.** Directory where outputs will be saved. | Varies by pipeline |
| `--config` | | **Required.** Path to the pipeline configuration YAML. | Varies by pipeline |
| `--golden` | | Optional golden dataset for deterministic comparisons. | `None` |
| `--sample` | | Process only the first N records for smoke testing. | `None` |
| `--fail-on-schema-drift` / `--allow-schema-drift` | | Fail if schema drift is detected. | `True` |
| `--extended` / `--no-extended` | | Emit extended QC artifacts. | `False` |
| `--mode` | | Execution mode for the pipeline. | `"default"` |
| `--dry-run` | `-d` | Validate configuration without running the pipeline. | `False` |
| `--verbose` | `-v` | Enable verbose (development) logging. | `False` |
| `--validate-columns` / `--no-validate-columns` | | Validate output columns against requirements. | `True` |
| `--set` | `-S` | Override a configuration value (e.g., `KEY=VALUE`). Can be repeated. | `[]` |

### Example

This command runs the `activity` pipeline using its specific configuration, saves the output to a designated directory, and overrides the batch size for this specific run.

```bash
python -m bioetl.cli.main activity \
  --config configs/pipelines/chembl_activity.yaml \
  --output-dir data/output/activity/run_20240101 \
  --set sources.chembl.batch_size=10
```

## `list` Command

The CLI provides a `list` command to display all registered pipeline commands.

```bash
python -m bioetl.cli.main list
```

This command inspects the static registry and prints a list of the available pipeline names.
