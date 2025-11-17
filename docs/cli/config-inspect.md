# `bioetl config inspect`

> **Note**: Implementation status: **implemented**. The Typer subcommand is
> part of `bioetl.cli.cli_app` and ships with the editable package.

`config inspect` is a read-only helper that merges a pipeline YAML file with all
profiles, environment overrides, and CLI flags before any pipeline code is
executed. It surfaces the same typed `PipelineConfig` that the execution runner
receives, making it the fastest way to verify new sections such as
`postprocess.*` and `fallbacks.*`.

## Usage

```bash
bioetl config inspect \
  --config configs/pipelines/activity/activity_chembl.yaml \
  --output-dir data/output/activity_inspect \
  --set postprocess.correlation.enabled=true
```

The command prints two sections:

1. `Configuration summary` — pipeline metadata, active profiles, and the current
   values of `postprocess.correlation` and `fallbacks.*` toggles.
2. `Normalized configuration` — the fully merged payload in YAML (default) or
   JSON format.

## Options

| Flag | Purpose | Default |
| ---- | ------- | ------- |
| `--config, -c` | Path to the main pipeline YAML file. | Required |
| `--output-dir, -o` | Materialization root injected into `materialization.root`. | `data/output/_inspect` |
| `--format, -f` | Serialization format (`yaml` or `json`). | `yaml` |
| `--set, -S` | Repeatable dotted overrides (`section.key=value`). | `[]` |
| `--sample` | Deterministic sample size propagated to `config.cli.sample`. | `null` |
| `--limit` | Maximum number of rows processed during inspection. | `null` |
| `--extended` | Flag that mirrors the pipeline CLI, enabling QC/correlation previews. | `false` |
| `--fail-on-schema-drift / --allow-schema-drift` | Toggle strict column validation. | `--fail-on-schema-drift` |
| `--validate-columns / --no-validate-columns` | Control Pandera column validation mode. | `--validate-columns` |
| `--golden` | Optional path recorded under `config.cli.golden`. | `null` |
| `--input-file, -i` | Optional seed file to inject deterministic ID lists. | `null` |

`--limit` and `--sample` are mutually exclusive, mirroring the runtime CLI.
Validation errors surface with exit code `2` and the same structured feedback as
pipeline commands.

## Example output

```text
$ bioetl config inspect --config configs/pipelines/activity/activity_chembl.yaml
Configuration summary
  config_path: /workspace/.../configs/pipelines/activity/activity_chembl.yaml
  pipeline: activity_chembl
  version: 1.0.0
  owner: Data Acquisition Team
  output_dir: /workspace/.../data/output/_inspect
  Profiles:
    - configs/defaults/base.yaml
    - configs/defaults/determinism.yaml
Domain toggles:
  postprocess.correlation.enabled: False
  fallbacks.enabled: True
  fallbacks.max_depth: unbounded

Normalized configuration
pipeline:
  name: activity_chembl
  version: 1.0.0
  ...
```

Use this command before touching runtime code to confirm that new `postprocess`
and `fallbacks` sections are discoverable through the typed config contract.
