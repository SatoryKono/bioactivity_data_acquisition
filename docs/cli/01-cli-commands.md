# CLI Commands

> **Note**: The Typer CLI shipped in `bioetl.cli.main` is available today. All paths referencing `src/bioetl/` correspond to the editable source tree that backs the installed package.

This reference drills into every command exposed by `python -m bioetl.cli.main`, capturing its signature, required and optional options, default configuration bundles, runnable examples, and determinism guarantees.

## Invocation pattern

All pipeline entry points share the same Typer invocation form:

```bash
python -m bioetl.cli.main <command> [OPTIONS]
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
| `--preflight-handshake / --no-preflight-handshake` | | No | Force-enable or disable the ChEMBL preflight handshake guard before extract stages. | Falls back to config |

## Determinism building blocks

Every command inherits the determinism policy enforced by `PipelineBase`: stable sorting, canonicalised values, SHA256 row and business-key hashes, and atomic writes. The shared `configs/defaults/determinism.yaml` profile captures these guarantees, while pipeline-specific configs define the concrete sort keys.

## Command reference

### `activity_chembl`

- **Signature**: `python -m bioetl.cli.main activity_chembl [OPTIONS]`
- **Purpose**: Extract biological activity records from ChEMBL `/activity.json` and normalise them to the project schema.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, and any applicable `--set` overrides.
- **Default profiles**: Always merges `configs/defaults/base.yaml` and `configs/defaults/determinism.yaml`; network defaults can be layered when referenced in the pipeline YAML.
- **Deterministic output**: Rows are sorted by `assay_id`, `testitem_id`, then `activity_id`; `hash_row` and `hash_business_key` are produced with SHA256 using the canonicalisation rules from the determinism profile. The run emits a `meta.yaml` snapshot with the fingerprint of both configuration and outputs.
- **Example**:

  ```bash
  python -m bioetl.cli.main activity_chembl \
    --config configs/pipelines/activity/activity_chembl.yaml \
    --output-dir data/output/activity \
    --set sources.chembl.batch_size=10
  ```

### `assay_chembl`

- **Signature**: `python -m bioetl.cli.main assay_chembl [OPTIONS]`
- **Purpose**: Retrieve and normalise assay metadata from ChEMBL `/assay.json`.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set`.
- **Default profiles**: `base.yaml` and `determinism.yaml`, with optional network profile via the pipeline config.
- **Deterministic output**: Sorted by `assay_id` before export; SHA256 hashes cover the business key and entire row, ensuring reproducible QC and golden comparisons.
- **Example**:

  ```bash
  python -m bioetl.cli.main assay_chembl \
    --config configs/pipelines/assay/assay_chembl.yaml \
    --output-dir data/output/assay
  ```

### `target`

- **Signature**: `python -m bioetl.cli.main target [OPTIONS]`
- **Purpose**: Build the enriched target dimension by combining ChEMBL `/target.json` data with UniProt and IUPHAR classifications.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set` (for example, to toggle enrichment services).
- **Default profiles**: `base.yaml` + `determinism.yaml`; additional network-specific overrides come from the pipeline YAML, including dedicated HTTP profiles for external enrichers.
- **Deterministic output**: Sorted by `target_id` and hashed with SHA256; the determinism profile guarantees stable canonicalisation, while the pipeline config fixes enrichment thresholds and QC expectations.
- **Example**:

  ```bash
  python -m bioetl.cli.main target \
    --config configs/pipelines/target/target_chembl.yaml \
    --output-dir data/output/target
  ```

### `document`

- **Signature**: `python -m bioetl.cli.main document [OPTIONS]`
- **Purpose**: Extract ChEMBL documents and optionally enrich them with PubMed, Crossref, OpenAlex, and Semantic Scholar metadata, depending on the configured mode.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--mode` (for example `chembl` vs `all`), `--limit`, `--sample`, `--golden`, `--set`.
- **Default profiles**: `base.yaml` and `determinism.yaml`; enrichment adapters inherit network defaults specified in the document pipeline config.
- **Deterministic output**: Sorted by `year` and `document_id`, with SHA256 hashes covering both the full row and business keys. The adapter settings ensure canonical source precedence while still producing deterministic outputs and metadata.
- **Example**:

  ```bash
  python -m bioetl.cli.main document \
    --config configs/pipelines/document/document_chembl.yaml \
    --output-dir data/output/document \
    --mode all
  ```

### `testitem`

- **Signature**: `python -m bioetl.cli.main testitem [OPTIONS]`
- **Purpose**: Produce the molecule (test item) dimension from ChEMBL `/molecule.json`, optionally blending in PubChem enrichment.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set` (for example, toggling PubChem enrichment).
- **Default profiles**: `base.yaml`, `determinism.yaml`, plus any network overrides included in the pipeline config.
- **Deterministic output**: Sorted by `testitem_id` and hashed deterministically; outputs and QC sidecars inherit the shared determinism policy.
- **Example**:

  ```bash
  python -m bioetl.cli.main testitem \
    --config configs/pipelines/testitem/testitem_chembl.yaml \
    --output-dir data/output/testitem
  ```

### `pubchem`

- **Signature**: `python -m bioetl.cli.main pubchem [OPTIONS]`
- **Purpose**: Enrich ChEMBL molecules with PubChem properties using the standalone PubChem pipeline.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--set` (for example, adjusting PubChem rate limits or lookup paths).
- **Default profiles**: `base.yaml`, `determinism.yaml`, and the PubChem-specific HTTP settings declared in `configs/pipelines/pubchem.yaml`.
- **Deterministic output**: PubChem enrichment maintains deterministic ordering by the lookup keys (`molecule_chembl_id`, `standard_inchi_key`) before writing artifacts, with SHA256 hashes mirroring the shared policy.
- **Example**:

  ```bash
  python -m bioetl.cli.main pubchem \
    --config src/bioetl/configs/pipelines/pubchem.yaml \
    --output-dir data/output/pubchem
  ```

### `gtp_iuphar`

- **Signature**: `python -m bioetl.cli.main gtp_iuphar [OPTIONS]`
- **Purpose**: Harvest Guide to Pharmacology target data as a standalone pipeline or enrichment service.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, `--mode` (for example, `smoke`), `--set` (to override API credentials or QC thresholds).
- **Default profiles**: `base.yaml`, `determinism.yaml`, and the dedicated HTTP profile embedded in `configs/pipelines/iuphar.yaml`.
- **Deterministic output**: Upholds the standard determinism contract—stable sort keys defined in the config, SHA256 hashing, and atomic artifact writes captured in `meta.yaml`.
- **Example**:

  ```bash
  python -m bioetl.cli.main gtp_iuphar \
    --config src/bioetl/configs/pipelines/iuphar.yaml \
    --output-dir data/output/gtp_iuphar
  ```

### `uniprot`

- **Signature**: `python -m bioetl.cli.main uniprot [OPTIONS]`
- **Purpose**: Fetch UniProt records (including optional ID mapping and ortholog lookups) into a deterministic dataset.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--limit`, `--sample`, `--golden`, and `--set` toggles for specific UniProt subsystems.
- **Default profiles**: `base.yaml`, `determinism.yaml`, with UniProt-specific HTTP settings drawn from `configs/pipelines/uniprot.yaml`.
- **Deterministic output**: Sort order and hashing follow the determinism profile after UniProt enrichment and schema validation, yielding reproducible CSV/Parquet outputs and metadata.
- **Example**:

  ```bash
  python -m bioetl.cli.main uniprot \
    --config src/bioetl/configs/pipelines/uniprot.yaml \
    --output-dir data/output/uniprot
  ```

### `openalex`

- **Signature**: `python -m bioetl.cli.main openalex [OPTIONS]`
- **Purpose**: Convenience alias that runs the document pipeline with the OpenAlex adapter enabled for Works API enrichment.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--mode`, `--limit`, `--sample`, `--golden`, `--set` (to adjust OpenAlex batch sizes or rate limits).
- **Default profiles**: `base.yaml`, `determinism.yaml`, plus the adapter settings defined under `sources.openalex.*` in the document pipeline config.
- **Deterministic output**: Shares the document pipeline’s `year`/`document_id` sort order and SHA256 hashing, ensuring deterministic metadata even when only OpenAlex enrichment is active.
- **Example**:

  ```bash
  python -m bioetl.cli.main openalex \
    --config configs/pipelines/document/document_chembl.yaml \
    --output-dir data/output/openalex \
    --mode all \
    --set sources.pubmed.enabled=false \
    --set sources.crossref.enabled=false \
    --set sources.semantic_scholar.enabled=false \
    --set sources.openalex.enabled=true
  ```

### `crossref`

- **Signature**: `python -m bioetl.cli.main crossref [OPTIONS]`
- **Purpose**: Run the document pipeline with only the Crossref adapter enabled (mail-to headers enforced for polite API usage).
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--mode`, `--limit`, `--sample`, `--golden`, `--set` (for example, to provide `sources.crossref.mailto`).
- **Default profiles**: `base.yaml`, `determinism.yaml`, and Crossref-specific keys inside the document pipeline config.
- **Deterministic output**: Identical ordering and hashing guarantees to the base document command; only the Crossref enrichment branch is activated.
- **Example**:

  ```bash
  python -m bioetl.cli.main crossref \
    --config configs/pipelines/document/document_chembl.yaml \
    --output-dir data/output/crossref \
    --mode all \
    --set sources.crossref.enabled=true \
    --set sources.pubmed.enabled=false \
    --set sources.openalex.enabled=false \
    --set sources.semantic_scholar.enabled=false
  ```

### `pubmed`

- **Signature**: `python -m bioetl.cli.main pubmed [OPTIONS]`
- **Purpose**: Execute the document pipeline with PubMed adapters enabled (optionally alongside other enrichers via `--mode`).
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--mode`, `--limit`, `--sample`, `--golden`, `--set` (for example, `sources.pubmed.tool`, `sources.pubmed.email`).
- **Default profiles**: `base.yaml`, `determinism.yaml`, plus PubMed-specific connection settings inside the document config.
- **Deterministic output**: Retains the document pipeline’s deterministic ordering and hashing rules while enriching from PubMed.
- **Example**:

  ```bash
  python -m bioetl.cli.main pubmed \
    --config configs/pipelines/document/document_chembl.yaml \
    --output-dir data/output/pubmed \
    --mode all \
    --set sources.pubmed.enabled=true \
    --set sources.crossref.enabled=false \
    --set sources.openalex.enabled=false \
    --set sources.semantic_scholar.enabled=false
  ```

### `semantic_scholar`

- **Signature**: `python -m bioetl.cli.main semantic_scholar [OPTIONS]`
- **Purpose**: Run the document pipeline with Semantic Scholar Graph API enrichment enabled.
- **Required options**: `--config`, `--output-dir`.
- **Optional options**: `--dry-run`, `--mode`, `--limit`, `--sample`, `--golden`, `--set` (including `sources.semantic_scholar.api_key`).
- **Default profiles**: `base.yaml`, `determinism.yaml`, and the Semantic Scholar adapter configuration contained in the document pipeline YAML.
- **Deterministic output**: Uses the document pipeline’s deterministic ordering and SHA256 hashing when only Semantic Scholar enrichment is active.
- **Example**:

  ```bash
  python -m bioetl.cli.main semantic_scholar \
    --config configs/pipelines/document/document_chembl.yaml \
    --output-dir data/output/semantic_scholar \
    --mode all \
    --set sources.semantic_scholar.enabled=true \
    --set sources.pubmed.enabled=false \
    --set sources.crossref.enabled=false \
    --set sources.openalex.enabled=false
  ```

### `list`

- **Signature**: `python -m bioetl.cli.main list`
- **Purpose**: Display the statically-registered pipeline commands packaged with the CLI.
- **Behaviour**: No options are required. The command introspects the registry and prints the available pipeline names in deterministic (alphabetical) order for reproducible scripting.

## Summary matrix

| Command | Data domain | Primary configuration | Default profiles applied |
| --- | --- | --- | --- |
| `activity_chembl` | ChEMBL activity fact table | `configs/pipelines/activity/activity_chembl.yaml` | `configs/defaults/base.yaml`, `configs/defaults/determinism.yaml`, optional `configs/defaults/network.yaml` |
| `assay_chembl` | ChEMBL assay dimension | `configs/pipelines/assay/assay_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `target` | ChEMBL target dimension + UniProt/IUPHAR enrichment | `configs/pipelines/target/target_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `document` | ChEMBL documents with optional external enrichers | `configs/pipelines/document/document_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `testitem` | ChEMBL molecules with PubChem enrichment hooks | `configs/pipelines/testitem/testitem_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `pubchem` | PubChem standalone enrichment | `src/bioetl/configs/pipelines/pubchem.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `gtp_iuphar` | Guide to Pharmacology export | `src/bioetl/configs/pipelines/iuphar.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `uniprot` | UniProt protein export | `src/bioetl/configs/pipelines/uniprot.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `openalex` | Document pipeline (OpenAlex adapter focus) | `configs/pipelines/document/document_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `crossref` | Document pipeline (Crossref adapter focus) | `configs/pipelines/document/document_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `pubmed` | Document pipeline (PubMed adapter focus) | `configs/pipelines/document/document_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `semantic_scholar` | Document pipeline (Semantic Scholar adapter focus) | `configs/pipelines/document/document_chembl.yaml` | `base.yaml`, `determinism.yaml`, optional `network.yaml` |
| `list` | Registry discovery | *(no config)* | *(not applicable)* |

Each matrix entry links the CLI command to its authoritative configuration bundle, making it easy to trace which YAML files—and therefore which typed models—govern a run.
