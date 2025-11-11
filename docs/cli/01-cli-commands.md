# CLI Commands

> **Note**: The Typer CLI shipped in `bioetl.cli.app` is available today. All paths referencing `src/bioetl/` correspond to the editable source tree that backs the installed package.

This reference drills into every command exposed by `python -m bioetl.cli.app`, capturing its signature, required and optional options, default configuration bundles, runnable examples, and determinism guarantees.

## Invocation pattern

All pipeline entry points share the same Typer invocation form:

```bash
python -m bioetl.cli.app <command> [OPTIONS]
```

The CLI loads configuration layers in a fixed precedence: profiles declared via `extends` (typically `configs/defaults/base.yaml`, `configs/defaults/network.yaml`, and `configs/defaults/determinism.yaml`), then the pipeline YAML passed with `--config`, then any `--set` overrides, and finally environment variables. This merge order keeps defaults predictable while still allowing per-run overrides.

## Global options

These switches are available to every pipeline command. Flags marked as **required** must be present on the command line; all others default to a safe, deterministic behaviour when omitted.

| Option | Shorthand | Required | Description | Defaults |
| --- | --- | --- | --- | --- |
| `--config PATH` | | **Yes** | Path to the pipeline configuration YAML. | Provided per command |
| `--output-dir PATH` | `-o` | **Yes** | Directory where run artifacts are materialised. | Provided per run |
| `--input-file PATH` | `-i` | No | Optional seed dataset used during extraction for pipelines that require one. | Pipeline specific |
| `--dry-run` | `-d` | No | Load, merge, and validate configuration without executing the pipeline. | `False` |
| `--limit N` | | No | Process at most `N` rows (useful for smoke runs). | `None` |
| `--sample N` | | No | Randomly sample `N` rows; honours deterministic sampling seeds when configured. | `None` |
| `--golden PATH` | | No | Compare outputs against a stored golden dataset for bitwise determinism checks. | `None` |
| `--mode NAME` | | No | Select a pre-defined execution mode (for example, enabling enrichment adapters). | Pipeline specific |
| `--set KEY=VALUE` | `-S` | No | Override individual configuration keys at runtime. Repeatable. | `[]` |
| `--fail-on-schema-drift / --allow-schema-drift` | | No | Toggle failure when output schemas deviate from the expected order. | `--fail-on-schema-drift` |
| `--validate-columns / --no-validate-columns` | | No | Control column validation hooks in the post-processing stage. | `--validate-columns` |
| `--extended / --no-extended` | | No | Enable extended QC artifacts. | `--no-extended` |
| `--verbose` | `-v` | No | Emit verbose (development) logging. | `False` |

## Determinism building blocks

Every command inherits the determinism policy enforced by `PipelineBase`: stable sorting, canonicalised values, SHA256 row and business-key hashes, and atomic writes. The shared `configs/defaults/determinism.yaml` profile captures these guarantees, while pipeline-specific configs define the concrete sort keys.

## Command reference

### `activity_chembl`

- **Signature**: `python -m bioetl.cli.app activity_chembl [OPTIONS]`
- **Purpose**: Extract biological activity records from ChEMBL `/activity.json` and normalise them to the project schema.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, and any applicable `--set` overrides.
- **Default profiles**: Always merges `configs/defaults/base.yaml` and `configs/defaults/determinism.yaml`; network defaults can be layered when referenced in the pipeline YAML.
- **Deterministic output**: Rows are sorted by `assay_id`, `testitem_id`, then `activity_id`; `hash_row` and `hash_business_key` are produced with SHA256 using the canonicalisation rules from the determinism profile. The run emits a `meta.yaml` snapshot with the fingerprint of both configuration and outputs.
- **Example**:

  ```bash
  python -m bioetl.cli.app activity_chembl \
    --config configs/pipelines/activity/activity_chembl.yaml \
    --output-dir data/output/activity \
    --set sources.chembl.batch_size=10
  ```

### `assay_chembl`

- **Signature**: `python -m bioetl.cli.app assay_chembl [OPTIONS]`
- **Purpose**: Retrieve and normalise assay metadata from ChEMBL `/assay.json`.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set`.
- **Default profiles**: `base.yaml` and `determinism.yaml`, with optional network profile via the pipeline config.
- **Deterministic output**: Sorted by `assay_id` before export; SHA256 hashes cover the business key and entire row, ensuring reproducible QC and golden comparisons.
- **Example**:

  ```bash
  python -m bioetl.cli.app assay_chembl \
    --config configs/pipelines/assay/assay_chembl.yaml \
    --output-dir data/output/assay
  ```

### `target_chembl`

- **Signature**: `python -m bioetl.cli.app target_chembl [OPTIONS]`
- **Purpose**: Build the enriched target dimension by combining ChEMBL `/target.json` data with UniProt and IUPHAR classifications.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set` (for example, to toggle enrichment services).
- **Default profiles**: `base.yaml` + `determinism.yaml`; additional network-specific overrides come from the pipeline YAML, including dedicated HTTP profiles for external enrichers.
- **Deterministic output**: Sorted by `target_id` and hashed with SHA256; the determinism profile guarantees stable canonicalisation, while the pipeline config fixes enrichment thresholds and QC expectations.
- **Example**:

  ```bash
  python -m bioetl.cli.app target_chembl \
    --config configs/pipelines/target/target_chembl.yaml \
    --output-dir data/output/target
  ```

### `document_chembl`

- **Signature**: `python -m bioetl.cli.app document_chembl [OPTIONS]`
- **Purpose**: Extract ChEMBL documents and optionally enrich them with PubMed, Crossref, OpenAlex, and Semantic Scholar metadata, depending on the configured mode.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--mode` (for example `chembl` vs `all`), `--limit`, `--sample`, `--golden`, `--set`.
- **Default profiles**: `base.yaml` and `determinism.yaml`; enrichment adapters inherit network defaults specified in the document pipeline config.
- **Deterministic output**: Sorted by `year` and `document_id`, with SHA256 hashes covering both the full row and business keys. The adapter settings ensure canonical source precedence while still producing deterministic outputs and metadata.
- **Example**:

  ```bash
  python -m bioetl.cli.app document_chembl \
    --config configs/pipelines/document/document_chembl.yaml \
    --output-dir data/output/document \
    --mode all
  ```

### `testitem_chembl`

- **Signature**: `python -m bioetl.cli.app testitem_chembl [OPTIONS]`
- **Purpose**: Produce the molecule (test item) dimension from ChEMBL `/molecule.json`, optionally blending in PubChem enrichment.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set` (for example, toggling PubChem enrichment).
- **Default profiles**: `base.yaml`, `determinism.yaml`, plus any network overrides included in the pipeline config.
- **Deterministic output**: Sorted by `testitem_id` and hashed deterministically; outputs and QC sidecars inherit the shared determinism policy.
- **Example**:

  ```bash
  python -m bioetl.cli.app testitem_chembl \
    --config configs/pipelines/testitem/testitem_chembl.yaml \
    --output-dir data/output/testitem
  ```

### Не реализовано

Команды `activity`, `assay`, `document_*` (например, `document_pubmed`, `document_crossref`, `document_openalex`), а также `pubchem`, `uniprot`, `gtp_iuphar`, `openalex`, `crossref`, `pubmed`, `semantic_scholar`, `list` **не активны** в текущей сборке и помечены как «не реализовано».

## Summary matrix

| Command | Data domain | Primary configuration | Default profiles applied |
| --- | --- | --- | --- |
| `activity_chembl` | ChEMBL activity fact table | `configs/pipelines/activity/activity_chembl.yaml` | `configs/defaults/base.yaml`, `configs/defaults/determinism.yaml`, optional `configs/defaults/network.yaml` |
| `assay_chembl` | ChEMBL assay dimension | `configs/pipelines/assay/assay_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `target_chembl` | ChEMBL target dimension | `configs/pipelines/target/target_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `document_chembl` | ChEMBL documents with optional adapters | `configs/pipelines/document/document_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `testitem_chembl` | ChEMBL molecules with PubChem enrichment hooks | `configs/pipelines/testitem/testitem_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |

Each matrix entry links the CLI command to its authoritative configuration bundle, making it easy to trace which YAML files—and therefore which typed models—govern a run.
