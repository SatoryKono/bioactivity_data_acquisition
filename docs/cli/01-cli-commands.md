# CLI Commands

> **Note**: The Typer CLI shipped in `bioetl.cli.cli_app` is available today.
> All paths referencing `src/bioetl/` correspond to the editable source tree
> that backs the installed package.

This reference drills into every command exposed by
`python -m bioetl.cli.cli_app`, capturing its signature, required and optional
options, default configuration bundles, runnable examples, and determinism
guarantees.

## Invocation pattern

All pipeline entry points share the same Typer invocation form:

```bash
python -m bioetl.cli.cli_app <command> [OPTIONS]
```

После установки пакета доступен идентичный консольный скрипт:

```bash
bioetl <command> [OPTIONS]
```

The CLI loads configuration layers in a fixed precedence: profiles declared via
`extends` (typically `configs/defaults/base.yaml`,
`configs/defaults/network.yaml`, and `configs/defaults/determinism.yaml`), then
the pipeline YAML passed with `--config`, then any `--set` overrides, and
finally environment variables. This merge order keeps defaults predictable while
still allowing per-run overrides.

## Global options

These switches are available to every pipeline command. Options without defaults
must be supplied explicitly whenever the CLI reports them as required.

| Flag                                            | Purpose                                                         | Default                  |
| ----------------------------------------------- | --------------------------------------------------------------- | ------------------------ |
| `--config, -c`                                  | Path to the pipeline configuration YAML.                        | —                        |
| `--output-dir, -o`                              | Directory where pipeline artifacts are written.                 | —                        |
| `--input-file, -i`                              | Optional CSV/Parquet containing seed IDs or records.            | —                        |
| `--dry-run, -d`                                 | Load and validate configuration without executing the pipeline. | `False`                  |
| `--verbose, -v`                                 | Enable verbose (DEBUG-level) logging.                           | `False`                  |
| `--set, -S`                                     | Repeatable `KEY=VALUE` overrides applied after config merge.    | `[]`                     |
| `--sample`                                      | Deterministically sample `N` rows from the input dataset.       | `None`                   |
| `--limit`                                       | Process at most `N` rows (handy for smoke runs).                | `None`                   |
| `--extended`                                    | Emit extended QC sidecars and metrics.                          | `False`                  |
| `--golden`                                      | Path to a golden dataset for determinism checks.                | `None`                   |
| `--fail-on-schema-drift / --allow-schema-drift` | Control whether schema drift raises or logs.                    | `--fail-on-schema-drift` |
| `--validate-columns / --no-validate-columns`    | Toggle strict column validation in post-processing.             | `--validate-columns`     |

## Доступные команды (актуально)

Список синхронизирован с `bioetl.cli.cli_registry.COMMAND_REGISTRY` и отражает
только активные команды CLI.

- `activity_chembl` — ChEMBL activity fact pipeline.
- `assay_chembl` — ChEMBL assay dimension pipeline.
- `target_chembl` — ChEMBL target dimension pipeline.
- `document_chembl` — ChEMBL document pipeline.
- `testitem_chembl` — ChEMBL molecule/test item pipeline.

## Determinism building blocks

Every command inherits the determinism policy enforced by `PipelineBase`: stable
sorting, canonicalised values, SHA256 row and business-key hashes, and atomic
writes. The shared `configs/defaults/determinism.yaml` profile captures these
guarantees, while pipeline-specific configs define the concrete sort keys.

## Determinism and `--golden`

The `--golden` option attaches a reference dataset that the pipeline must match
byte-for-byte. After the write stage completes, the resulting artifact is
compared against the supplied golden file using the deterministic serialization
rules defined in `configs/defaults/determinism.yaml`. Any drift in row order,
column order, canonicalised values, or hash columns is treated as a determinism
failure and surfaces as a non-zero exit code together with structured log
records that describe the mismatch.

Golden checks pair naturally with the `--extended` flag: enabling extended QC
emits the full set of sidecar reports (`quality_report`, correlation metrics,
and `meta.yaml`) that the determinism policy expects. When both flags are
present, the CLI writes QC artefacts and then validates that every emitted file
matches the golden snapshots tracked in source control.

Reference: see `docs/determinism/00-determinism-policy.md` for the full contract
that governs deterministic outputs and golden artefact maintenance.

### Example

```bash
python -m bioetl.cli.cli_app activity_chembl \
  --config configs/pipelines/activity/activity_chembl.yaml \
  --output-dir data/output/activity \
  --golden tests/bioetl/golden/activity/activity.parquet \
  --extended
# Exit code: 0 (outputs identical to the golden snapshot)
```

If the comparison detects any byte-level difference, the command exits with a
non-zero status, leaving the produced artefacts in place for inspection and
emitting structured diagnostics that pinpoint the drift.

## Command reference

### `activity_chembl`

- **Signature**: `python -m bioetl.cli.cli_app activity_chembl [OPTIONS]`
- **Purpose**: Extract biological activity records from ChEMBL `/activity.json`
  and normalise them to the project schema.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, and any
  applicable `--set` overrides.
- **Default profiles**: Always merges `configs/defaults/base.yaml` and
  `configs/defaults/determinism.yaml`; network defaults can be layered when
  referenced in the pipeline YAML.
- **Deterministic output**: Rows are sorted by `assay_id`, `testitem_id`, then
  `activity_id`; `hash_row` and `hash_business_key` are produced with SHA256
  using the canonicalisation rules from the determinism profile. The run emits a
  `meta.yaml` snapshot with the fingerprint of both configuration and outputs.

#### Пример запуска: activity_chembl

```bash
python -m bioetl.cli.cli_app activity_chembl \
  --config configs/pipelines/activity/activity_chembl.yaml \
  --output-dir ./data/output/activity \
  --sample 5
```

### `assay_chembl`

- **Signature**: `python -m bioetl.cli.cli_app assay_chembl [OPTIONS]`
- **Purpose**: Retrieve and normalise assay metadata from ChEMBL `/assay.json`.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set`.
- **Default profiles**: `base.yaml` and `determinism.yaml`, with optional
  network profile via the pipeline config.
- **Deterministic output**: Sorted by `assay_id` before export; SHA256 hashes
  cover the business key and entire row, ensuring reproducible QC and golden
  comparisons.

#### Пример запуска: assay_chembl

```bash
python -m bioetl.cli.cli_app assay_chembl \
  --config configs/pipelines/assay/assay_chembl.yaml \
  --output-dir ./data/output/assay \
  --sample 5
```

### `target_chembl`

- **Signature**: `python -m bioetl.cli.cli_app target_chembl [OPTIONS]`
- **Purpose**: Build the enriched target dimension by combining ChEMBL
  `/target.json` data with UniProt and IUPHAR classifications.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set`
  (for example, to toggle enrichment services).
- **Default profiles**: `base.yaml` + `determinism.yaml`; additional
  network-specific overrides come from the pipeline YAML, including dedicated
  HTTP profiles for external enrichers.
- **Deterministic output**: Sorted by `target_id` and hashed with SHA256; the
  determinism profile guarantees stable canonicalisation, while the pipeline
  config fixes enrichment thresholds and QC expectations.

#### Пример запуска: target_chembl

```bash
python -m bioetl.cli.cli_app target_chembl \
  --config configs/pipelines/target/target_chembl.yaml \
  --output-dir ./data/output/target \
  --sample 5
```

### `document_chembl`

- **Signature**: `python -m bioetl.cli.cli_app document_chembl [OPTIONS]`
- **Purpose**: Extract ChEMBL documents and optionally enrich them with PubMed,
  Crossref, OpenAlex, and Semantic Scholar metadata, depending on the configured
  mode.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--mode` (for example `chembl` vs `all`),
  `--limit`, `--sample`, `--golden`, `--set`.
- **Default profiles**: `base.yaml` and `determinism.yaml`; enrichment adapters
  inherit network defaults specified in the document pipeline config.
- **Deterministic output**: Sorted by `year` and `document_id`, with SHA256
  hashes covering both the full row and business keys. The adapter settings
  ensure canonical source precedence while still producing deterministic outputs
  and metadata.

#### Пример запуска: document_chembl

```bash
python -m bioetl.cli.cli_app document_chembl \
  --config configs/pipelines/document/document_chembl.yaml \
  --output-dir ./data/output/document \
  --sample 5
```

### `testitem_chembl`

- **Signature**: `python -m bioetl.cli.cli_app testitem_chembl [OPTIONS]`
- **Purpose**: Produce the molecule (test item) dimension from ChEMBL
  `/molecule.json`, optionally blending in PubChem enrichment.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set`
  (for example, toggling PubChem enrichment).
- **Default profiles**: `base.yaml`, `determinism.yaml`, plus any network
  overrides included in the pipeline config.
- **Deterministic output**: Sorted by `testitem_id` and hashed
  deterministically; outputs and QC sidecars inherit the shared determinism
  policy.

#### Пример запуска: testitem_chembl

```bash
python -m bioetl.cli.cli_app testitem_chembl \
  --config configs/pipelines/testitem/testitem_chembl.yaml \
  --output-dir ./data/output/testitem \
  --sample 5
```

### Не реализовано

Следующие команды отсутствуют в `COMMAND_REGISTRY` и помечены как **not
implemented**:

- `activity`
- `assay`
- `document_pubmed`
- `document_crossref`
- `document_openalex`
- `document_semantic_scholar`
- `document`
- `pubchem`
- `uniprot`
- `gtp_iuphar`
- `openalex`
- `crossref`
- `pubmed`
- `semantic_scholar`
- `list`

## Summary matrix

| Command           | Data domain                                    | Primary configuration                             | Default profiles applied                                                                                    |
| ----------------- | ---------------------------------------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `activity_chembl` | ChEMBL activity fact table                     | `configs/pipelines/activity/activity_chembl.yaml` | `configs/defaults/base.yaml`, `configs/defaults/determinism.yaml`, optional `configs/defaults/network.yaml` |
| `assay_chembl`    | ChEMBL assay dimension                         | `configs/pipelines/assay/assay_chembl.yaml`       | `base.yaml`, `determinism.yaml`, optional `network.yaml`                                                    |
| `target_chembl`   | ChEMBL target dimension                        | `configs/pipelines/target/target_chembl.yaml`     | `base.yaml`, `determinism.yaml`, optional `network.yaml`                                                    |
| `document_chembl` | ChEMBL documents with optional adapters        | `configs/pipelines/document/document_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml`                                                    |
| `testitem_chembl` | ChEMBL molecules with PubChem enrichment hooks | `configs/pipelines/testitem/testitem_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml`                                                    |

Each matrix entry links the CLI command to its authoritative configuration
bundle, making it easy to trace which YAML files—and therefore which typed
models—govern a run.
