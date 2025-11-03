# ChEMBL Pipelines Catalog

This catalog consolidates the normative configuration, interfaces, and quality controls for the ChEMBL extraction pipelines. Each card lists the required configuration knobs, the expected inputs/outputs, and the QC surface so that runs can be reproduced and audited with confidence.【F:docs/pipelines/06-activity-data-extraction.md†L5-L19】【F:docs/pipelines/05-assay-extraction.md†L1-L33】【F:docs/pipelines/09-document-chembl-extraction.md†L1-L33】

## Pipeline Cards

### Activity (`activity`)

**Purpose.** Extracts activity records from the ChEMBL REST API, normalizes measurement fields, and emits deterministic artifacts with lineage metadata.【F:docs/pipelines/06-activity-data-extraction.md†L5-L39】

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `extends` | `profiles/base.yaml`, `profiles/determinism.yaml` | Guarantees shared logging, determinism, and I/O defaults.【F:docs/configs/00-typed-configs-and-profiles.md†L79-L88】 |
| `sources.chembl.batch_size` | `≤ 25` (required) | Enforced to respect the ChEMBL URL length limit for batched `/activity.json` calls.【F:docs/pipelines/06-activity-data-extraction.md†L168-L181】 |
| `postprocess.correlation.enabled` | `false` by default | When `true`, emits an optional correlation report alongside the dataset.【F:docs/pipelines/06-activity-data-extraction.md†L1041-L1088】 |

**Inputs.** Uses the official ChEMBL `/activity` endpoint via the shared `UnifiedAPIClient`, including a release handshake against `/status` to pin `chembl_release`.【F:docs/pipelines/06-activity-data-extraction.md†L33-L60】

**Outputs.** Writes `activity_{date}.csv`, `activity_{date}_quality_report.csv`, and conditionally `activity_{date}_correlation_report.csv`; the extended profile also records `activity_{date}_meta.yaml` and an optional run manifest.【F:docs/pipelines/06-activity-data-extraction.md†L1041-L1054】

**Quality controls.** Mandatory metrics cover totals, type/unit distributions, missing values, duplicate detection, foreign-key integrity, and ChEMBL validity flags; the pipeline requires duplicate-free `activity_id` values before write-out.【F:docs/pipelines/06-activity-data-extraction.md†L943-L1009】

**CLI usage.**

```bash
# Deterministic run with the canonical config
python -m bioetl.cli.main activity \
  --config configs/pipelines/chembl/activity.yaml \
  --output-dir data/output/activity

# Override batch size for a smoke test (env > --set precedence)
export BIOETL__SOURCES__CHEMBL__BATCH_SIZE=50
python -m bioetl.cli.main activity \
  --config configs/pipelines/chembl/activity.yaml \
  --set sources.chembl.batch_size=10
```
【F:docs/configs/00-typed-configs-and-profiles.md†L90-L98】

---

### Assay (`assay`)

**Purpose.** Runs a multi-stage pipeline (extract → transform → validate → write) for assay metadata, including nested structure expansion, enrichment, and strict schema enforcement.【F:docs/pipelines/05-assay-extraction.md†L5-L33】

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.chembl.batch_size` | `≤ 25` (required) | Hard limit due to ChEMBL URL constraints; validated on load.【F:docs/pipelines/05-assay-extraction.md†L75-L85】 |
| `sources.chembl.max_url_length` | `≤ 2000` | Feeds predictive throttling for request sizing.【F:docs/pipelines/05-assay-extraction.md†L75-L80】 |
| `cache.namespace` | non-empty | Ensures release-scoped cache invalidation.【F:docs/pipelines/05-assay-extraction.md†L75-L81】 |
| `determinism.sort.by` | `['assay_chembl_id', 'row_subtype', 'row_index']` | Keeps output ordering aligned with the Pandera schema column order.【F:docs/pipelines/05-assay-extraction.md†L75-L81】 |

**Inputs.** Pulls assay batches from `/assay.json` via the ChEMBL client, using cache-aware batching and release checks before enrichment with target and assay class lookups.【F:docs/pipelines/05-assay-extraction.md†L11-L25】【F:docs/pipelines/05-assay-extraction.md†L87-L118】

**Outputs.** Atomic writer produces `assay_{date}.csv`, `assay_{date}_qc.csv`, `assay_{date}_quality_report.csv`, and `assay_{date}_meta.yaml`, each hashed for provenance.【F:docs/pipelines/05-assay-extraction.md†L820-L889】

**Quality controls.** Built-in referential-integrity auditing of enriched targets/classes plus a QC profile that enforces non-null IDs, duplicate-free keys, valid ChEMBL patterns, and bounded integrity losses.【F:docs/pipelines/05-assay-extraction.md†L703-L780】

**CLI usage.**

```bash
# Standard production extraction
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay

# Throttle the client for troubleshooting
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --set sources.chembl.batch_size=20
```
【F:docs/pipelines/05-assay-extraction.md†L75-L85】

---

### Target (`target`)

**Purpose.** Collects ChEMBL target definitions and enriches them with UniProt and IUPHAR data to produce a consolidated target gold dataset.【F:docs/pipelines/08-target-data-extraction.md†L7-L33】

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.chembl.*` profile | Shared include supplies base URL, batching, headers, and jitter controls for ChEMBL calls.【F:docs/pipelines/sources/chembl/README.md†L31-L36】 |
| `sources.uniprot.enabled` / `sources.uniprot.batch_size` | Toggle UniProt enrichment and tune paging for ID-mapping/ortholog services.【F:docs/pipelines/sources/uniprot/README.md†L26-L32】 |
| `sources.iuphar.*` | Configure API key, caching, and minimum enrichment ratios for Guide to Pharmacology augmentations.【F:docs/pipelines/sources/iuphar/README.md†L24-L33】 |

**Inputs.** Core extraction hits `/target.json` while enrichment layers fan out to UniProt and IUPHAR clients defined in the source stack matrix.【F:docs/pipelines/08-target-data-extraction.md†L31-L43】【F:docs/sources/INTERFACE_MATRIX.md†L7-L12】

**Outputs.** Emits the unified target dataset and standard determinism metadata/hashes described in the global `meta.yaml` contract, allowing downstream reproducibility checks.【F:docs/determinism/01-determinism-policy.md†L73-L118】

**Quality controls.** Configuration monitors enrichment success rates and fallback usage across the external sources so unexpected coverage drops are surfaced in QC artifacts.【F:docs/pipelines/sources/chembl/README.md†L35-L36】

**CLI usage.**

```bash
# Full enrichment run
python -m bioetl.cli.main target \
  --config configs/pipelines/chembl/target.yaml \
  --output-dir data/output/target

# Disable UniProt enrichment for a connectivity check
python -m bioetl.cli.main target \
  --config configs/pipelines/chembl/target.yaml \
  --set sources.uniprot.enabled=false
```
【F:docs/pipelines/08-target-data-extraction.md†L15-L25】【F:docs/pipelines/sources/uniprot/README.md†L26-L32】

---

### Document (`document`)

**Purpose.** Normalizes publication metadata from ChEMBL and merges external bibliographic sources (PubMed, Crossref, OpenAlex, Semantic Scholar) with deterministic write-out and QC gating.【F:docs/pipelines/09-document-chembl-extraction.md†L7-L19】

**Key configuration.**

| Section | Key | Value / Constraint | Notes |
| --- | --- | --- | --- |
| Pipeline | `pipeline.name` | `document_chembl` | Identifies the run across logs and metadata.【F:docs/pipelines/09-document-chembl-extraction.md†L2778-L2789】 |
| ChEMBL source | `sources.chembl.chunk_size` | `10 (≤ 20)` | Keeps request URLs under ~1800 characters.【F:docs/pipelines/09-document-chembl-extraction.md†L2788-L2794】 |
| PubMed | `sources.pubmed.history.use_history` | `true` | Enables e-utilities history for large batches.【F:docs/pipelines/09-document-chembl-extraction.md†L2788-L2796】 |
| Crossref | `sources.crossref.batching.dois_per_request` | `100 (≤ 200)` | Caps DOIs per call per API guidance.【F:docs/pipelines/09-document-chembl-extraction.md†L2788-L2796】 |
| QC | `qc.max_title_fallback` / `qc.max_s2_access_denied` | `0.15` / `0.05` | Thresholds for title fallback rate and Semantic Scholar access denials.【F:docs/pipelines/09-document-chembl-extraction.md†L2788-L2796】 |

**Inputs.** Combines a base ChEMBL crawl with downstream DOI- and PMID-driven fetches from PubMed, Crossref, OpenAlex, and Semantic Scholar, all orchestrated through dedicated client layers.【F:docs/pipelines/09-document-chembl-extraction.md†L7-L19】

**Outputs.** Generates `documents_{mode}_{date}.csv`, QC and correlation reports, plus `documents_{mode}_{date}_meta.yaml` with per-source fetch statistics.【F:docs/pipelines/09-document-chembl-extraction.md†L2812-L2832】

**Quality controls.** Coverage metrics for DOI/PMID/title/journal/authors, conflict detection across sources, validity flags (DOI regex, year bounds, Semantic Scholar access), and duplicate tracking feed a consolidated QC report and threshold checker.【F:docs/pipelines/09-document-chembl-extraction.md†L2646-L2796】

**CLI usage.**

```bash
# All-source enrichment run
python -m bioetl.cli.main document \
  --config configs/pipelines/chembl/document.yaml \
  --output-dir data/output/document

# ChEMBL + Crossref only (disable PubMed)
python -m bioetl.cli.main document \
  --config configs/pipelines/chembl/document.yaml \
  --set sources.pubmed.enabled=false
```
【F:docs/pipelines/09-document-chembl-extraction.md†L2788-L2803】

---

### TestItem (`testitem`)

**Purpose.** Exports flattened molecule records from ChEMBL and optionally enriches them with PubChem properties while preserving deterministic ordering by molecule ID.【F:docs/pipelines/07-testitem-extraction.md†L7-L83】

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.chembl.batch_size` | `≈ 25` | Controls `/molecule.json` batching to respect URL limits.【F:docs/pipelines/07-testitem-extraction.md†L32-L37】 |
| `sources.pubchem.enabled` | `false`/`true` | Governs optional PubChem enrichment workflow (CID resolution + property fetch).【F:docs/pipelines/07-testitem-extraction.md†L47-L66】 |
| `postprocess` ordering | Fixed in pipeline config | Ensures merged ChEMBL + PubChem columns follow the documented layout and QC monitors duplicates/fallback usage.【F:docs/pipelines/sources/chembl/README.md†L35-L36】 |

**Inputs.** Batches ChEMBL `molecule_chembl_id` values and, when enabled, fans out to PubChem endpoints for CIDs and property payloads with caching and graceful degradation.【F:docs/pipelines/07-testitem-extraction.md†L32-L69】

**Outputs.** Produces the flattened molecule dataset alongside the standard meta/QC artifacts mandated by the determinism contract, capturing column order, hashes, and enrichment lineage.【F:docs/determinism/01-determinism-policy.md†L73-L118】

**Quality controls.** Configuration mandates duplicate tracking and fallback-rate monitoring when PubChem enrichment is active, safeguarding identity/synonym coverage.【F:docs/pipelines/sources/chembl/README.md†L35-L36】

**CLI usage.**

```bash
# Base molecule export
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/testitem.yaml \
  --output-dir data/output/testitem

# Enable PubChem enrichment on demand
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/testitem.yaml \
  --set sources.pubchem.enabled=true
```
【F:docs/pipelines/07-testitem-extraction.md†L18-L27】【F:docs/pipelines/07-testitem-extraction.md†L47-L66】

## Determinism & Invariant Matrix

| Pipeline | Sort keys | Hashing invariants | Schema reference |
| --- | --- | --- | --- |
| Activity | `['assay_id', 'testitem_id', 'activity_id']` | Row/business hashes generated with canonicalized values under the SHA256 policy (`hash_row`, `hash_business_key`).【F:docs/determinism/01-determinism-policy.md†L28-L70】 | `chembl_activity` schema module in the source interface matrix.【F:docs/sources/INTERFACE_MATRIX.md†L7-L13】 |
| Assay | `['assay_id']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/01-determinism-policy.md†L28-L70】 | `chembl_assay` schema per interface matrix.【F:docs/sources/INTERFACE_MATRIX.md†L7-L13】 |
| Target | `['target_id']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/01-determinism-policy.md†L28-L70】 | `chembl_target` schema per interface matrix.【F:docs/sources/INTERFACE_MATRIX.md†L7-L13】 |
| Document | `['year', 'document_id']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/01-determinism-policy.md†L28-L70】 | `chembl_document` schema per interface matrix.【F:docs/sources/INTERFACE_MATRIX.md†L7-L13】 |
| TestItem | `['testitem_id']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/01-determinism-policy.md†L28-L70】 | `chembl_testitem` schema per interface matrix.【F:docs/sources/INTERFACE_MATRIX.md†L7-L13】 |

The determinism policy also standardizes value canonicalization, serialization, and `meta.yaml` shape, ensuring cross-pipeline reproducibility when identical inputs and configurations are supplied.【F:docs/determinism/01-determinism-policy.md†L36-L118】
