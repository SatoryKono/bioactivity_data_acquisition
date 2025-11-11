# Pipelines Catalog

This catalog consolidates the normative configuration, interfaces, and quality controls for all extraction pipelines in the BioETL project. Each card lists the required configuration knobs, the expected inputs/outputs, and the QC surface so that runs can be reproduced and audited with confidence.

**Documentation naming convention:** Pipeline documentation files follow the canonical format `<NN>-<entity>-<source>-<topic>.md` (e.g., `09-document-chembl-extraction.md`). See [Naming Conventions](../styleguide/00-naming-conventions.md#11-pipeline-documentation-file-naming).

## Maintainers

- `@SatoryKono` — глобальный код-оунер и ответственный за документацию репозитория, поэтому он утверждает изменения по пайплайнам и сопутствующим артефактам.【F:.github/CODEOWNERS†L5-L41】

## Public API Overview

The pipelines expose the following public APIs:

### ChEMBL Pipelines

- `bioetl.pipelines.activity.ChemblActivityPipeline` — основной ETL по активности ChEMBL (загрузка `/activity`, управление fallback и валидацией по `ActivitySchema`).【F:src/bioetl/pipelines/activity/activity.py†L106-L460】
- `bioetl.pipelines.assay.ChemblAssayPipeline` — извлечение ассайев ChEMBL с учётом лимитов URL, кешей и статистики fallback.【F:src/bioetl/pipelines/assay/assay.py†L126-L490】
- `bioetl.pipelines.document.ChemblDocumentPipeline` — выгрузка документов ChEMBL и оркестрация обогащения PubMed/Crossref/OpenAlex/Semantic Scholar по режимам `chembl`/`all`.【F:src/bioetl/pipelines/document/document.py†L60-L520】
- `bioetl.pipelines.target.ChemblTargetPipeline` — многостадийный таргет-пайплайн: ChEMBL → UniProt → IUPHAR + постобработка в `target_gold`.【F:src/bioetl/pipelines/target/target.py†L38-L560】
- `bioetl.pipelines.testitem.TestItemChemblPipeline` — загрузка молекул (test items) с полями из `/molecule` и поддержкой PubChem-обогащения.【F:src/bioetl/pipelines/testitem/testitem.py†L52-L620】

### Document Pipelines

- PubMed Document Pipeline — извлечение метаданных публикаций из PubMed E-utilities API.【F:docs/pipelines/document-pubmed/00-document-pubmed-overview.md†L7-L11】
- Crossref Document Pipeline — извлечение метаданных публикаций из Crossref REST API.【F:docs/pipelines/document-crossref/00-document-crossref-overview.md†L9-L22】
- OpenAlex Document Pipeline — извлечение метаданных публикаций из OpenAlex Works API.【F:docs/pipelines/document-openalex/00-document-openalex-overview.md†L9-L32】
- Semantic Scholar Document Pipeline — извлечение метаданных публикаций из Semantic Scholar Graph API.【F:docs/pipelines/document-semantic-scholar/00-document-semantic-scholar-overview.md†L9-L32】

### Target Pipelines

- UniProt Target Pipeline — извлечение и обработка данных о таргетах (белках) из UniProt REST API.【F:docs/pipelines/target-uniprot/00-target-uniprot-overview.md†L9-L26】
- Guide to Pharmacology Target Pipeline — извлечение и обработка данных о фармакологических таргетах из Guide to Pharmacology API.【F:docs/pipelines/target-iuphar/00-target-iuphar-overview.md†L9-L26】

### TestItem Pipelines

- PubChem TestItem Pipeline — извлечение данных о молекулах из PubChem PUG REST API.【F:docs/pipelines/testitem-pubchem/00-testitem-pubchem-overview.md†L9-L24】

### Mapping Pipelines

- ChEMBL → UniProt Mapping Pipeline — маппинг ChEMBL target identifiers в UniProt accession numbers через UniProt ID Mapping API.【F:docs/pipelines/28-chembl2uniprot-mapping.md†L9-L11】

## Pipeline Cards

### Activity (`activity`) {#activity_chembl}

**Purpose.** Extracts activity records from the ChEMBL REST API, normalizes measurement fields, and emits deterministic artifacts with lineage metadata.【F:docs/pipelines/activity-chembl/09-activity-chembl-extraction.md†L5-L39】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-activity-chembl-overview.md](activity-chembl/00-activity-chembl-overview.md) — Pipeline overview
- [09-activity-chembl-extraction.md](activity-chembl/09-activity-chembl-extraction.md) — Extraction from ChEMBL API
- [10-activity-chembl-transformation.md](activity-chembl/10-activity-chembl-transformation.md) — Measurement field normalization
- [11-activity-chembl-validation.md](activity-chembl/11-activity-chembl-validation.md) — Pandera schemas and validation
- [12-activity-chembl-io.md](activity-chembl/12-activity-chembl-io.md) — Output formats and atomic writing
- [13-activity-chembl-determinism.md](activity-chembl/13-activity-chembl-determinism.md) — Determinism, stable sort, hashing
- [14-activity-chembl-qc.md](activity-chembl/14-activity-chembl-qc.md) — QC metrics and thresholds
- [15-activity-chembl-logging.md](activity-chembl/15-activity-chembl-logging.md) — Structured logging format
- [16-activity-chembl-cli.md](activity-chembl/16-activity-chembl-cli.md) — CLI commands and flags
- [17-activity-chembl-config.md](activity-chembl/17-activity-chembl-config.md) — Configuration keys and profiles

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `extends` | `profiles/base.yaml`, `profiles/determinism.yaml` | Guarantees shared logging, determinism, and I/O defaults.【F:docs/configs/00-typed-configs-and-profiles.md†L79-L88】 |
| `sources.chembl.batch_size` | `≤ 25` (required) | Enforced to respect the ChEMBL URL length limit for batched `/activity.json` calls.【F:docs/pipelines/activity-chembl/09-activity-chembl-extraction.md†L168-L181】 |
| `postprocess.correlation.enabled` | `false` by default | When `true`, emits an optional correlation report alongside the dataset.【F:docs/pipelines/activity-chembl/09-activity-chembl-extraction.md†L1041-L1088】 |

**Inputs.** Uses the official ChEMBL `/activity` endpoint via the shared `UnifiedAPIClient`, including a release handshake against `/status` to pin `chembl_release`.【F:docs/pipelines/activity-chembl/09-activity-chembl-extraction.md†L33-L60】

**Outputs.** Writes `activity_{date}.csv`, `activity_{date}_quality_report.csv`, and conditionally `activity_{date}_correlation_report.csv`; the extended profile also records `activity_{date}_meta.yaml` and an optional run manifest.【F:docs/pipelines/activity-chembl/09-activity-chembl-extraction.md†L1041-L1054】

**Quality controls.** Mandatory metrics cover totals, type/unit distributions, missing values, duplicate detection, foreign-key integrity, and ChEMBL validity flags; the pipeline requires duplicate-free `activity_id` values before write-out.【F:docs/pipelines/activity-chembl/09-activity-chembl-extraction.md†L943-L1009】

**CLI usage.**

```bash
# Deterministic run with the canonical config
python -m bioetl.cli.main activity \
  --config configs/pipelines/activity/activity_chembl.yaml \
  --output-dir data/output/activity

# Override batch size for a smoke test (env > --set precedence)
export BIOETL__SOURCES__CHEMBL__BATCH_SIZE=50
python -m bioetl.cli.main activity \
  --config configs/pipelines/activity/activity_chembl.yaml \
  --set sources.chembl.batch_size=10
```

【F:docs/configs/00-typed-configs-and-profiles.md†L90-L98】

---

### Assay (`assay`) {#assay_chembl}

**Purpose.** Runs a multi-stage pipeline (extract → transform → validate → write) for assay metadata, including nested structure expansion, enrichment, and strict schema enforcement.【F:docs/pipelines/assay-chembl/09-assay-chembl-extraction.md†L5-L33】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-assay-chembl-overview.md](assay-chembl/00-assay-chembl-overview.md) — Pipeline overview
- [09-assay-chembl-extraction.md](assay-chembl/09-assay-chembl-extraction.md) — Extraction from ChEMBL API
- [10-assay-chembl-transformation.md](assay-chembl/10-assay-chembl-transformation.md) — Nested structure expansion and transformations
- [11-assay-chembl-validation.md](assay-chembl/11-assay-chembl-validation.md) — Pandera schemas and validation
- [12-assay-chembl-io.md](assay-chembl/12-assay-chembl-io.md) — Output formats and atomic writing
- [13-assay-chembl-determinism.md](assay-chembl/13-assay-chembl-determinism.md) — Determinism, stable sort, hashing
- [14-assay-chembl-qc.md](assay-chembl/14-assay-chembl-qc.md) — QC metrics and thresholds
- [15-assay-chembl-logging.md](assay-chembl/15-assay-chembl-logging.md) — Structured logging format
- [16-assay-chembl-cli.md](assay-chembl/16-assay-chembl-cli.md) — CLI commands and flags
- [17-assay-chembl-config.md](assay-chembl/17-assay-chembl-config.md) — Configuration keys and profiles

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.chembl.batch_size` | `≤ 25` (required) | Hard limit due to ChEMBL URL constraints; validated on load.【F:docs/pipelines/assay-chembl/09-assay-chembl-extraction.md†L75-L85】 |
| `sources.chembl.max_url_length` | `≤ 2000` | Feeds predictive throttling for request sizing.【F:docs/pipelines/assay-chembl/09-assay-chembl-extraction.md†L75-L80】 |
| `cache.namespace` | non-empty | Ensures release-scoped cache invalidation.【F:docs/pipelines/assay-chembl/09-assay-chembl-extraction.md†L75-L81】 |
| `determinism.sort.by` | `['assay_chembl_id', 'row_subtype', 'row_index']` | Keeps output ordering aligned with the Pandera schema column order.【F:docs/pipelines/assay-chembl/09-assay-chembl-extraction.md†L75-L81】 |

**Inputs.** Pulls assay batches from `/assay.json` via the ChEMBL client, using cache-aware batching and release checks before enrichment with target and assay class lookups.【F:docs/pipelines/assay-chembl/09-assay-chembl-extraction.md†L11-L25】【F:docs/pipelines/assay-chembl/09-assay-chembl-extraction.md†L87-L118】

**Outputs.** Atomic writer produces `assay_{date}.csv`, `assay_{date}_qc.csv`, `assay_{date}_quality_report.csv`, and `assay_{date}_meta.yaml`, each hashed for provenance.【F:docs/pipelines/assay-chembl/09-assay-chembl-extraction.md†L820-L889】

**Quality controls.** Built-in referential-integrity auditing of enriched targets/classes plus a QC profile that enforces non-null IDs, duplicate-free keys, valid ChEMBL patterns, and bounded integrity losses.【F:docs/pipelines/assay-chembl/09-assay-chembl-extraction.md†L703-L780】

**CLI usage.**

```bash
# Standard production extraction
python -m bioetl.cli.main assay \
  --config configs/pipelines/assay/assay_chembl.yaml \
  --output-dir data/output/assay

# Throttle the client for troubleshooting
python -m bioetl.cli.main assay \
  --config configs/pipelines/assay/assay_chembl.yaml \
  --set sources.chembl.batch_size=20
```

【F:docs/pipelines/assay-chembl/09-assay-chembl-extraction.md†L75-L85】

---

### Target (`target`) {#target_chembl}

**Purpose.** Collects ChEMBL target definitions and enriches them with UniProt and IUPHAR data to produce a consolidated target gold dataset.【F:docs/pipelines/target-chembl/09-target-chembl-extraction.md†L7-L33】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-target-chembl-overview.md](target-chembl/00-target-chembl-overview.md) — Pipeline overview
- [09-target-chembl-extraction.md](target-chembl/09-target-chembl-extraction.md) — Extraction from ChEMBL API
- [10-target-chembl-transformation.md](target-chembl/10-target-chembl-transformation.md) — Data consolidation and transformations
- [11-target-chembl-validation.md](target-chembl/11-target-chembl-validation.md) — Pandera schemas and validation
- [12-target-chembl-io.md](target-chembl/12-target-chembl-io.md) — Output formats and atomic writing
- [13-target-chembl-determinism.md](target-chembl/13-target-chembl-determinism.md) — Determinism, stable sort, hashing
- [14-target-chembl-qc.md](target-chembl/14-target-chembl-qc.md) — QC metrics and thresholds
- [15-target-chembl-logging.md](target-chembl/15-target-chembl-logging.md) — Structured logging format
- [16-target-chembl-cli.md](target-chembl/16-target-chembl-cli.md) — CLI commands and flags
- [17-target-chembl-config.md](target-chembl/17-target-chembl-config.md) — Configuration keys and profiles

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.chembl.*` profile | Shared include supplies base URL, batching, headers, and jitter controls for ChEMBL calls. See [Common Configuration](#common-configuration) below. | |
| `sources.uniprot.enabled` / `sources.uniprot.batch_size` | Toggle UniProt enrichment and tune paging for ID-mapping/ortholog services. See [UniProt Target Pipeline](target-uniprot/09-target-uniprot-extraction.md) and [ChEMBL to UniProt Mapping Pipeline](28-chembl2uniprot-mapping.md). | |
| `sources.iuphar.*` | Configure API key, caching, and minimum enrichment ratios for Guide to Pharmacology augmentations. See [IUPHAR Target Pipeline](target-iuphar/09-target-iuphar-extraction.md). | |

**Inputs.** Core extraction hits `/target.json` while enrichment layers fan out to UniProt and IUPHAR clients defined in the source stack matrix.【F:docs/pipelines/target-chembl/09-target-chembl-extraction.md†L31-L43】【F:docs/sources/INTERFACE_MATRIX.md†L7-L12】

**Outputs.** Emits the unified target dataset and standard determinism metadata/hashes described in the global `meta.yaml` contract, allowing downstream reproducibility checks.【F:docs/determinism/00-determinism-policy.md†L73-L118】

**Quality controls.** Configuration monitors enrichment success rates and fallback usage across the external sources so unexpected coverage drops are surfaced in QC artifacts. See [Merge Policy Summary](#merge-policy-summary) below.

**CLI usage.**

```bash
# Full enrichment run
python -m bioetl.cli.main target \
  --config configs/pipelines/target/target_chembl.yaml \
  --output-dir data/output/target

# Disable UniProt enrichment for a connectivity check
python -m bioetl.cli.main target \
  --config configs/pipelines/target/target_chembl.yaml \
  --set sources.uniprot.enabled=false
```

【F:docs/pipelines/target-chembl/09-target-chembl-extraction.md†L15-L25】【F:docs/pipelines/target-uniprot/09-target-uniprot-extraction.md†L33-L263】

---

### Document (`document`) {#document_chembl}

**Purpose.** Normalizes publication metadata from ChEMBL and merges external bibliographic sources (PubMed, Crossref, OpenAlex, Semantic Scholar) with deterministic write-out and QC gating.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L7-L19】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-document-chembl-overview.md](document-chembl/00-document-chembl-overview.md) — Pipeline overview
- [09-document-chembl-extraction.md](document-chembl/09-document-chembl-extraction.md) — Extraction from ChEMBL API
- [10-document-chembl-transformation.md](document-chembl/10-document-chembl-transformation.md) — Normalization and transformations
- [11-document-chembl-validation.md](document-chembl/11-document-chembl-validation.md) — Pandera schemas and validation
- [12-document-chembl-io.md](document-chembl/12-document-chembl-io.md) — Output formats and atomic writing
- [13-document-chembl-determinism.md](document-chembl/13-document-chembl-determinism.md) — Determinism, stable sort, hashing
- [14-document-chembl-qc.md](document-chembl/14-document-chembl-qc.md) — QC metrics and thresholds
- [15-document-chembl-logging.md](document-chembl/15-document-chembl-logging.md) — Structured logging format
- [16-document-chembl-cli.md](document-chembl/16-document-chembl-cli.md) — CLI commands and flags
- [17-document-chembl-config.md](document-chembl/17-document-chembl-config.md) — Configuration keys and profiles

**Key configuration.**

| Section | Key | Value / Constraint | Notes |
| --- | --- | --- | --- |
| Pipeline | `pipeline.name` | `document_chembl` | Identifies the run across logs and metadata.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L2778-L2789】 |
| ChEMBL source | `sources.chembl.chunk_size` | `10 (≤ 20)` | Keeps request URLs under ~1800 characters.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L2788-L2794】 |
| PubMed | `sources.pubmed.history.use_history` | `true` | Enables e-utilities history for large batches.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L2788-L2796】 |
| Crossref | `sources.crossref.batching.dois_per_request` | `100 (≤ 200)` | Caps DOIs per call per API guidance.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L2788-L2796】 |
| QC | `qc.max_title_fallback` / `qc.max_s2_access_denied` | `0.15` / `0.05` | Thresholds for title fallback rate and Semantic Scholar access denials.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L2788-L2796】 |

**Inputs.** Combines a base ChEMBL crawl with downstream DOI- and PMID-driven fetches from PubMed, Crossref, OpenAlex, and Semantic Scholar, all orchestrated through dedicated client layers.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L7-L19】

**Outputs.** Generates `documents_{mode}_{date}.csv`, QC and correlation reports, plus `documents_{mode}_{date}_meta.yaml` with per-source fetch statistics.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L2812-L2832】

**Quality controls.** Coverage metrics for DOI/PMID/title/journal/authors, conflict detection across sources, validity flags (DOI regex, year bounds, Semantic Scholar access), and duplicate tracking feed a consolidated QC report and threshold checker.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L2646-L2796】

**CLI usage.**

```bash
# All-source enrichment run
python -m bioetl.cli.main document \
  --config configs/pipelines/document/document_chembl.yaml \
  --output-dir data/output/document

# ChEMBL + Crossref only (disable PubMed)
python -m bioetl.cli.main document \
  --config configs/pipelines/document/document_chembl.yaml \
  --set sources.pubmed.enabled=false
```

【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L2788-L2803】

---

### TestItem (`testitem`) {#testitem_chembl}

**Purpose.** Exports flattened molecule records from ChEMBL while preserving deterministic ordering by molecule ID. The pipeline flattens nested JSON structures from ChEMBL responses to create comprehensive, flat records for each molecule.【F:docs/pipelines/testitem-chembl/09-testitem-chembl-extraction.md†L7-L14】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-testitem-chembl-overview.md](testitem-chembl/00-testitem-chembl-overview.md) — Pipeline overview
- [09-testitem-chembl-extraction.md](testitem-chembl/09-testitem-chembl-extraction.md) — Extraction from ChEMBL API
- [10-testitem-chembl-transformation.md](testitem-chembl/10-testitem-chembl-transformation.md) — Structure flattening and transformations
- [11-testitem-chembl-validation.md](testitem-chembl/11-testitem-chembl-validation.md) — Pandera schemas and validation
- [12-testitem-chembl-io.md](testitem-chembl/12-testitem-chembl-io.md) — Output formats and atomic writing
- [13-testitem-chembl-determinism.md](testitem-chembl/13-testitem-chembl-determinism.md) — Determinism, stable sort, hashing
- [14-testitem-chembl-qc.md](testitem-chembl/14-testitem-chembl-qc.md) — QC metrics and thresholds
- [15-testitem-chembl-logging.md](testitem-chembl/15-testitem-chembl-logging.md) — Structured logging format
- [16-testitem-chembl-cli.md](testitem-chembl/16-testitem-chembl-cli.md) — CLI commands and flags
- [17-testitem-chembl-config.md](testitem-chembl/17-testitem-chembl-config.md) — Configuration keys and profiles

> **Note**: PubChem enrichment is described in a separate document: [PubChem TestItem Pipeline](testitem-pubchem/09-testitem-pubchem-extraction.md).

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.chembl.batch_size` | `≤ 25` (required) | Controls `/molecule.json` batching to respect URL limits. Enforced to respect the ChEMBL URL length limit for batched `/molecule.json` calls.【F:docs/pipelines/testitem-chembl/09-testitem-chembl-extraction.md†L111-L118】 |
| `sources.chembl.max_url_length` | `≤ 2000` | Used for predictive throttling of requests.【F:docs/pipelines/testitem-chembl/09-testitem-chembl-extraction.md†L112】 |
| `determinism.sort.by` | `['molecule_chembl_id']` | First key is `molecule_chembl_id`. Sorting is applied before write; final CSV follows `TestItemSchema._column_order`.【F:docs/pipelines/testitem-chembl/09-testitem-chembl-extraction.md†L114】 |
| `qc.thresholds.testitem.duplicate_ratio` | `0.0` | Critical: duplicates are not allowed.【F:docs/pipelines/testitem-chembl/09-testitem-chembl-extraction.md†L116】 |
| `qc.thresholds.testitem.fallback_ratio` | `0.2` | Percentage of fallback records during API errors.【F:docs/pipelines/testitem-chembl/09-testitem-chembl-extraction.md†L117】 |
| `qc.thresholds.testitem.parent_missing_ratio` | `0.0` | Referential integrity for `parent_chembl_id`.【F:docs/pipelines/testitem-chembl/09-testitem-chembl-extraction.md†L118】 |

**Inputs.** Batches ChEMBL `molecule_chembl_id` values from input CSV and fetches molecule data from `/molecule.json` endpoint with release-scoped caching and graceful degradation.【F:docs/pipelines/testitem-chembl/09-testitem-chembl-extraction.md†L163-L200】

**Outputs.** Produces the flattened molecule dataset alongside the standard meta/QC artifacts mandated by the determinism contract, capturing column order, hashes, and lineage metadata. Outputs include `testitem_{date}.csv`, `testitem_{date}_quality_report.csv`, and optionally `testitem_{date}_meta.yaml` in extended mode.【F:docs/pipelines/testitem-chembl/09-testitem-chembl-extraction.md†L360-L429】

**Quality controls.** Configuration mandates duplicate tracking, fallback-rate monitoring, and referential integrity checks for `parent_chembl_id`. QC metrics include `testitem.duplicate_ratio`, `testitem.fallback_ratio`, and `testitem.parent_missing_ratio` with configurable thresholds.【F:docs/pipelines/testitem-chembl/09-testitem-chembl-extraction.md†L528-L604】

**CLI usage.**

```bash
# Base molecule export
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/chembl_testitem.yaml \
  --output-dir data/output/chembl_testitem

# Override batch size for smoke test
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/chembl_testitem.yaml \
  --output-dir data/output/chembl_testitem \
  --set sources.chembl.batch_size=10 \
  --limit 100
```

【F:docs/pipelines/testitem-chembl/09-testitem-chembl-extraction.md†L35-L53】

---

### Document PubMed (`document_pubmed`) {#document_pubmed}

**Purpose.** Extracts publication metadata from PubMed using the E-utilities API. It provides comprehensive bibliographic information including titles, abstracts, authors, journal details, and publication metadata.【F:docs/pipelines/document-pubmed/00-document-pubmed-overview.md†L7-L11】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-document-pubmed-overview.md](document-pubmed/00-document-pubmed-overview.md) — Pipeline overview
- [09-document-pubmed-extraction.md](document-pubmed/09-document-pubmed-extraction.md) — Extraction from PubMed E-utilities API
- [43-document-pubmed-transformation.md](document-pubmed/43-document-pubmed-transformation.md) — XML parsing and field normalization
- [44-document-pubmed-validation.md](document-pubmed/44-document-pubmed-validation.md) — Pandera schemas and validation
- [45-document-pubmed-io.md](document-pubmed/45-document-pubmed-io.md) — Output formats and atomic writing
- [46-document-pubmed-determinism.md](document-pubmed/46-document-pubmed-determinism.md) — Determinism, stable sort, hashing
- [47-document-pubmed-qc.md](document-pubmed/47-document-pubmed-qc.md) — QC metrics and thresholds
- [48-document-pubmed-logging.md](document-pubmed/48-document-pubmed-logging.md) — Structured logging format
- [49-document-pubmed-cli.md](document-pubmed/49-document-pubmed-cli.md) — CLI commands and flags
- [50-document-pubmed-config.md](document-pubmed/50-document-pubmed-config.md) — Configuration keys and profiles

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.pubmed.history.use_history` | `true` | Enables e-utilities history for large batches.【F:docs/pipelines/document-pubmed/00-document-pubmed-overview.md†L61】 |
| `sources.pubmed.rate_limit.max_calls` | `3` (without API key) | Rate limiting: 3 requests per second without API key.【F:docs/pipelines/document-pubmed/00-document-pubmed-overview.md†L62】 |
| `determinism.sort.by` | `['pmid']` | Primary sort key is `pmid`.【F:docs/pipelines/document-pubmed/46-document-pubmed-determinism.md†L13】 |

**Inputs.** Requires PMID or DOI identifiers from input CSV and fetches document data from PubMed E-utilities API (ESearch, EPost, EFetch) with history server support for batch operations.【F:docs/pipelines/document-pubmed/00-document-pubmed-overview.md†L21-L24】

**Outputs.** Produces publication dataset alongside standard meta/QC artifacts mandated by the determinism contract. Outputs include `document_pubmed_{date}.csv`, `document_pubmed_{date}_quality_report.csv`, and optionally `document_pubmed_{date}_meta.yaml` in extended mode.【F:docs/pipelines/document-pubmed/00-document-pubmed-overview.md†L24】

**Quality controls.** Coverage metrics for PMID/DOI/title/journal/authors, missing value detection, duplicate tracking, and validity checks feed a consolidated QC report.【F:docs/pipelines/document-pubmed/00-document-pubmed-overview.md†L64】

**CLI usage.**

```bash
# Standard extraction from PubMed
python -m bioetl.cli.main document --source pubmed \
  --config configs/pipelines/pubmed/document.yaml \
  --output-dir data/output/document-pubmed

# With input file containing PMIDs
python -m bioetl.cli.main document --source pubmed \
  --config configs/pipelines/pubmed/document.yaml \
  --input-file data/input/pmids.csv \
  --output-dir data/output/document-pubmed
```

【F:docs/pipelines/document-pubmed/49-document-pubmed-cli.md†L14-L23】

---

### Document Crossref (`document_crossref`) {#document_crossref}

**Purpose.** Extracts publication metadata from Crossref REST API. Crossref is a DOI registration agency providing comprehensive bibliographic metadata for scholarly works.【F:docs/pipelines/document-crossref/00-document-crossref-overview.md†L9-L22】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-document-crossref-overview.md](document-crossref/00-document-crossref-overview.md) — Pipeline overview
- [09-document-crossref-extraction.md](document-crossref/09-document-crossref-extraction.md) — Extraction from Crossref REST API
- [59-document-crossref-transformation.md](document-crossref/59-document-crossref-transformation.md) — JSON parsing and field normalization
- [60-document-crossref-validation.md](document-crossref/60-document-crossref-validation.md) — Pandera schemas and validation
- [61-document-crossref-io.md](document-crossref/61-document-crossref-io.md) — Output formats and atomic writing
- [62-document-crossref-determinism.md](document-crossref/62-document-crossref-determinism.md) — Determinism, stable sort, hashing
- [63-document-crossref-qc.md](document-crossref/63-document-crossref-qc.md) — QC metrics and thresholds
- [64-document-crossref-logging.md](document-crossref/64-document-crossref-logging.md) — Structured logging format
- [65-document-crossref-cli.md](document-crossref/65-document-crossref-cli.md) — CLI commands and flags
- [66-document-crossref-config.md](document-crossref/66-document-crossref-config.md) — Configuration keys and profiles

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.crossref.batching.dois_per_request` | `100 (≤ 200)` | Caps DOIs per call per API guidance.【F:docs/pipelines/document-crossref/00-document-crossref-overview.md†L61】 |
| `sources.crossref.rate_limit.max_calls` | `2` | Rate limiting: 2 requests per second.【F:docs/pipelines/document-crossref/00-document-crossref-overview.md†L62】 |
| `determinism.sort.by` | `['doi']` | Primary sort key is `doi`.【F:docs/pipelines/document-crossref/62-document-crossref-determinism.md†L13】 |

**Inputs.** Requires DOI identifiers from input CSV and fetches document data from Crossref `/works` endpoint with batch processing (up to 100 DOIs per request).【F:docs/pipelines/document-crossref/00-document-crossref-overview.md†L21-L24】

**Outputs.** Produces publication dataset alongside standard meta/QC artifacts. Outputs include `document_crossref_{date}.csv`, `document_crossref_{date}_quality_report.csv`, and optionally `document_crossref_{date}_meta.yaml` in extended mode.【F:docs/pipelines/document-crossref/00-document-crossref-overview.md†L24】

**Quality controls.** Coverage metrics for DOI/title/journal/authors, missing value detection, duplicate tracking, and validity checks feed a consolidated QC report.【F:docs/pipelines/document-crossref/00-document-crossref-overview.md†L64】

**CLI usage.**

```bash
# Standard extraction from Crossref
python -m bioetl.cli.main document --source crossref \
  --config configs/pipelines/crossref/document.yaml \
  --output-dir data/output/document-crossref

# With input file containing DOIs
python -m bioetl.cli.main document --source crossref \
  --config configs/pipelines/crossref/document.yaml \
  --input-file data/input/dois.csv \
  --output-dir data/output/document-crossref
```

【F:docs/pipelines/document-crossref/65-document-crossref-cli.md†L14-L23】

---

### Document OpenAlex (`document_openalex`) {#document_openalex}

**Purpose.** Extracts publication metadata from OpenAlex using the Works API. OpenAlex is a free, open-source database of scholarly works with comprehensive metadata including citations, concepts, and open access status.【F:docs/pipelines/document-openalex/00-document-openalex-overview.md†L9-L32】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-document-openalex-overview.md](document-openalex/00-document-openalex-overview.md) — Pipeline overview
- [09-document-openalex-extraction.md](document-openalex/09-document-openalex-extraction.md) — Extraction from OpenAlex Works API
- [51-document-openalex-transformation.md](document-openalex/51-document-openalex-transformation.md) — JSON parsing and field normalization
- [52-document-openalex-validation.md](document-openalex/52-document-openalex-validation.md) — Pandera schemas and validation
- [53-document-openalex-io.md](document-openalex/53-document-openalex-io.md) — Output formats and atomic writing
- [54-document-openalex-determinism.md](document-openalex/54-document-openalex-determinism.md) — Determinism, stable sort, hashing
- [55-document-openalex-qc.md](document-openalex/55-document-openalex-qc.md) — QC metrics and thresholds
- [56-document-openalex-logging.md](document-openalex/56-document-openalex-logging.md) — Structured logging format
- [57-document-openalex-cli.md](document-openalex/57-document-openalex-cli.md) — CLI commands and flags
- [58-document-openalex-config.md](document-openalex/58-document-openalex-config.md) — Configuration keys and profiles

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.openalex.rate_limit.max_calls` | `10` | Rate limiting: 10 requests per second.【F:docs/pipelines/document-openalex/00-document-openalex-overview.md†L61】 |
| `determinism.sort.by` | `['openalex_id']` | Primary sort key is `openalex_id`.【F:docs/pipelines/document-openalex/54-document-openalex-determinism.md†L13】 |

**Inputs.** Requires DOI, PMID, or OpenAlex ID identifiers from input CSV and fetches document data from OpenAlex `/works` endpoint with cursor-based pagination.【F:docs/pipelines/document-openalex/00-document-openalex-overview.md†L21-L24】

**Outputs.** Produces publication dataset alongside standard meta/QC artifacts. Outputs include `document_openalex_{date}.csv`, `document_openalex_{date}_quality_report.csv`, and optionally `document_openalex_{date}_meta.yaml` in extended mode.【F:docs/pipelines/document-openalex/00-document-openalex-overview.md†L24】

**Quality controls.** Coverage metrics for DOI/PMID/title/journal/authors, missing value detection, duplicate tracking, and validity checks feed a consolidated QC report.【F:docs/pipelines/document-openalex/00-document-openalex-overview.md†L64】

**CLI usage.**

```bash
# Standard extraction from OpenAlex
python -m bioetl.cli.main document --source openalex \
  --config configs/pipelines/openalex/document.yaml \
  --output-dir data/output/document-openalex

# With input file containing DOIs
python -m bioetl.cli.main document --source openalex \
  --config configs/pipelines/openalex/document.yaml \
  --input-file data/input/dois.csv \
  --output-dir data/output/document-openalex
```

【F:docs/pipelines/document-openalex/57-document-openalex-cli.md†L14-L23】

---

### Document Semantic Scholar (`document_semantic_scholar`) {#document_semantic_scholar}

**Purpose.** Extracts publication metadata from Semantic Scholar Graph API. Semantic Scholar provides comprehensive bibliographic data including citation metrics, abstract, and fields of study.【F:docs/pipelines/document-semantic-scholar/00-document-semantic-scholar-overview.md†L9-L32】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-document-semantic-scholar-overview.md](document-semantic-scholar/00-document-semantic-scholar-overview.md) — Pipeline overview
- [09-document-semantic-scholar-extraction.md](document-semantic-scholar/09-document-semantic-scholar-extraction.md) — Extraction from Semantic Scholar Graph API
- [67-document-semantic-scholar-transformation.md](document-semantic-scholar/67-document-semantic-scholar-transformation.md) — JSON parsing and field normalization
- [68-document-semantic-scholar-validation.md](document-semantic-scholar/68-document-semantic-scholar-validation.md) — Pandera schemas and validation
- [69-document-semantic-scholar-io.md](document-semantic-scholar/69-document-semantic-scholar-io.md) — Output formats and atomic writing
- [70-document-semantic-scholar-determinism.md](document-semantic-scholar/70-document-semantic-scholar-determinism.md) — Determinism, stable sort, hashing
- [71-document-semantic-scholar-qc.md](document-semantic-scholar/71-document-semantic-scholar-qc.md) — QC metrics and thresholds
- [72-document-semantic-scholar-logging.md](document-semantic-scholar/72-document-semantic-scholar-logging.md) — Structured logging format
- [73-document-semantic-scholar-cli.md](document-semantic-scholar/73-document-semantic-scholar-cli.md) — CLI commands and flags
- [74-document-semantic-scholar-config.md](document-semantic-scholar/74-document-semantic-scholar-config.md) — Configuration keys and profiles

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.semantic_scholar.rate_limit.max_calls` | `1` (without API key) | Rate limiting: 1 request per 1.25 seconds without key, higher with key.【F:docs/pipelines/document-semantic-scholar/00-document-semantic-scholar-overview.md†L61】 |
| `sources.semantic_scholar.api_key` | Optional | API key for higher rate limits.【F:docs/pipelines/document-semantic-scholar/00-document-semantic-scholar-overview.md†L60】 |
| `determinism.sort.by` | `['semantic_scholar_id']` | Primary sort key is `semantic_scholar_id`.【F:docs/pipelines/document-semantic-scholar/70-document-semantic-scholar-determinism.md†L13】 |

**Inputs.** Requires PMID, DOI, or Semantic Scholar Paper ID identifiers from input CSV and fetches document data from Semantic Scholar `/paper/batch` endpoint.【F:docs/pipelines/document-semantic-scholar/00-document-semantic-scholar-overview.md†L21-L24】

**Outputs.** Produces publication dataset alongside standard meta/QC artifacts. Outputs include `document_semantic_scholar_{date}.csv`, `document_semantic_scholar_{date}_quality_report.csv`, and optionally `document_semantic_scholar_{date}_meta.yaml` in extended mode.【F:docs/pipelines/document-semantic-scholar/00-document-semantic-scholar-overview.md†L24】

**Quality controls.** Coverage metrics for DOI/PMID/title/journal/authors, missing value detection, duplicate tracking, access denial rate monitoring, and validity checks feed a consolidated QC report.【F:docs/pipelines/document-semantic-scholar/00-document-semantic-scholar-overview.md†L64】

**CLI usage.**

```bash
# Standard extraction from Semantic Scholar
python -m bioetl.cli.main document --source semantic-scholar \
  --config configs/pipelines/semantic-scholar/document.yaml \
  --output-dir data/output/document-semantic-scholar

# With input file containing PMIDs
python -m bioetl.cli.main document --source semantic-scholar \
  --config configs/pipelines/semantic-scholar/document.yaml \
  --input-file data/input/pmids.csv \
  --output-dir data/output/document-semantic-scholar
```

【F:docs/pipelines/document-semantic-scholar/73-document-semantic-scholar-cli.md†L14-L23】

---

### Target UniProt (`target_uniprot`) {#target_uniprot}

**Purpose.** Extracts and processes target (protein) data from the UniProt REST API. This pipeline provides comprehensive protein information including sequences, features, gene names, and organism data.【F:docs/pipelines/target-uniprot/00-target-uniprot-overview.md†L9-L26】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-target-uniprot-overview.md](target-uniprot/00-target-uniprot-overview.md) — Pipeline overview
- [09-target-uniprot-extraction.md](target-uniprot/09-target-uniprot-extraction.md) — Extraction from UniProt REST API
- [27-target-uniprot-transformation.md](target-uniprot/27-target-uniprot-transformation.md) — Protein data normalization
- [28-target-uniprot-validation.md](target-uniprot/28-target-uniprot-validation.md) — Pandera schemas and validation
- [29-target-uniprot-io.md](target-uniprot/29-target-uniprot-io.md) — Output formats and atomic writing
- [30-target-uniprot-determinism.md](target-uniprot/30-target-uniprot-determinism.md) — Determinism, stable sort, hashing
- [31-target-uniprot-qc.md](target-uniprot/31-target-uniprot-qc.md) — QC metrics and thresholds
- [32-target-uniprot-logging.md](target-uniprot/32-target-uniprot-logging.md) — Structured logging format
- [33-target-uniprot-cli.md](target-uniprot/33-target-uniprot-cli.md) — CLI commands and flags
- [34-target-uniprot-config.md](target-uniprot/34-target-uniprot-config.md) — Configuration keys and profiles

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.uniprot.batch_size` | Configurable | Batch size for UniProt API calls.【F:docs/pipelines/target-uniprot/34-target-uniprot-config.md†L17】 |
| `sources.uniprot.rate_limit.max_calls` | `2` | Critical: ≤ 2 (UniProt API quota).【F:docs/pipelines/target-uniprot/09-target-uniprot-extraction.md†L79】 |
| `determinism.sort.by` | `['uniprot_accession']` | Primary sort key is `uniprot_accession`.【F:docs/pipelines/target-uniprot/34-target-uniprot-config.md†L19】 |

**Inputs.** Requires UniProt accession numbers as input. For mapping ChEMBL target IDs to UniProt accessions, use the separate `chembl2uniprot-mapping` pipeline.【F:docs/pipelines/target-uniprot/00-target-uniprot-overview.md†L11-L24】

**Outputs.** Produces protein target dataset alongside standard meta/QC artifacts. Outputs include `target_uniprot_{date}.csv`, `target_uniprot_{date}_quality_report.csv`, and optionally `target_uniprot_{date}_meta.yaml` in extended mode.【F:docs/pipelines/target-uniprot/00-target-uniprot-overview.md†L26】

**Quality controls.** Coverage metrics for protein data completeness, missing value detection, duplicate tracking, and validity checks feed a consolidated QC report.【F:docs/pipelines/target-uniprot/00-target-uniprot-overview.md†L65】

**CLI usage.**

```bash
# Standard extraction from UniProt
python -m bioetl.cli.main target --source uniprot \
  --config configs/pipelines/uniprot/target.yaml \
  --output-dir data/output/target-uniprot

# With input file containing UniProt accessions
python -m bioetl.cli.main target --source uniprot \
  --config configs/pipelines/uniprot/target.yaml \
  --input-file data/input/uniprot_accessions.csv \
  --output-dir data/output/target-uniprot
```

【F:docs/pipelines/target-uniprot/33-target-uniprot-cli.md†L14-L23】

---

### Target IUPHAR (`target_iuphar`) {#target_iuphar}

**Purpose.** Extracts and processes target (pharmacological target) data from the Guide to Pharmacology (GtP) / IUPHAR database. This pipeline provides comprehensive pharmacological target information including target classifications, receptor families, and pharmacological properties.【F:docs/pipelines/target-iuphar/00-target-iuphar-overview.md†L9-L26】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-target-iuphar-overview.md](target-iuphar/00-target-iuphar-overview.md) — Pipeline overview
- [09-target-iuphar-extraction.md](target-iuphar/09-target-iuphar-extraction.md) — Extraction from Guide to Pharmacology API
- [35-target-iuphar-transformation.md](target-iuphar/35-target-iuphar-transformation.md) — Target classification normalization
- [36-target-iuphar-validation.md](target-iuphar/36-target-iuphar-validation.md) — Pandera schemas and validation
- [37-target-iuphar-io.md](target-iuphar/37-target-iuphar-io.md) — Output formats and atomic writing
- [38-target-iuphar-determinism.md](target-iuphar/38-target-iuphar-determinism.md) — Determinism, stable sort, hashing
- [39-target-iuphar-qc.md](target-iuphar/39-target-iuphar-qc.md) — QC metrics and thresholds
- [40-target-iuphar-logging.md](target-iuphar/40-target-iuphar-logging.md) — Structured logging format
- [41-target-iuphar-cli.md](target-iuphar/41-target-iuphar-cli.md) — CLI commands and flags
- [42-target-iuphar-config.md](target-iuphar/42-target-iuphar-config.md) — Configuration keys and profiles

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.iuphar.api_key` | Optional | API key for Guide to Pharmacology API.【F:docs/pipelines/target-iuphar/09-target-iuphar-extraction.md†L85】 |
| `sources.iuphar.rate_limit.max_calls` | `6` | Rate limiting: 6 requests per second.【F:docs/pipelines/target-iuphar/09-target-iuphar-extraction.md†L79】 |
| `determinism.sort.by` | `['iuphar_object_id']` | Primary sort key is `iuphar_object_id`.【F:docs/pipelines/target-iuphar/42-target-iuphar-config.md†L19】 |

**Inputs.** Supports multiple input formats and automatically resolves identifiers to IUPHAR target_id through the search API. Enrichment from external sources (UniProt, ChEMBL) is handled by separate pipelines.【F:docs/pipelines/target-iuphar/00-target-iuphar-overview.md†L11-L24】

**Outputs.** Produces pharmacological target dataset alongside standard meta/QC artifacts. Outputs include `target_iuphar_{date}.csv`, `target_iuphar_{date}_quality_report.csv`, and optionally `target_iuphar_{date}_meta.yaml` in extended mode.【F:docs/pipelines/target-iuphar/00-target-iuphar-overview.md†L26】

**Quality controls.** Coverage metrics for target classification coverage, missing value detection, duplicate tracking, and validity checks feed a consolidated QC report.【F:docs/pipelines/target-iuphar/00-target-iuphar-overview.md†L65】

**CLI usage.**

```bash
# Standard extraction from IUPHAR
python -m bioetl.cli.main target --source iuphar \
  --config configs/pipelines/iuphar/target.yaml \
  --output-dir data/output/target-iuphar

# With input file containing various identifiers
python -m bioetl.cli.main target --source iuphar \
  --config configs/pipelines/iuphar/target.yaml \
  --input-file data/input/iuphar_targets.csv \
  --output-dir data/output/target-iuphar
```

【F:docs/pipelines/target-iuphar/41-target-iuphar-cli.md†L14-L28】

---

### TestItem PubChem (`testitem_pubchem`) {#testitem_pubchem}

**Purpose.** Extracts testitem (molecule) data from PubChem. It is a standalone pipeline that does not perform any joins or enrichment with other data sources.【F:docs/pipelines/testitem-pubchem/00-testitem-pubchem-overview.md†L9-L24】

**Documentation Structure** (canon: `<NN>-<entity>-<source>-<topic>.md`):

- [00-testitem-pubchem-overview.md](testitem-pubchem/00-testitem-pubchem-overview.md) — Pipeline overview
- [09-testitem-pubchem-extraction.md](testitem-pubchem/09-testitem-pubchem-extraction.md) — Extraction from PubChem PUG REST API
- [10-testitem-pubchem-transformation.md](testitem-pubchem/10-testitem-pubchem-transformation.md) — Molecule property normalization
- [23-testitem-pubchem-validation.md](testitem-pubchem/23-testitem-pubchem-validation.md) — Pandera schemas and validation
- [24-testitem-pubchem-io.md](testitem-pubchem/24-testitem-pubchem-io.md) — Output formats and atomic writing
- [25-testitem-pubchem-determinism.md](testitem-pubchem/25-testitem-pubchem-determinism.md) — Determinism, stable sort, hashing
- [26-testitem-pubchem-qc.md](testitem-pubchem/26-testitem-pubchem-qc.md) — QC metrics and thresholds
- [27-testitem-pubchem-logging.md](testitem-pubchem/27-testitem-pubchem-logging.md) — Structured logging format
- [28-testitem-pubchem-cli.md](testitem-pubchem/28-testitem-pubchem-cli.md) — CLI commands and flags
- [29-testitem-pubchem-config.md](testitem-pubchem/29-testitem-pubchem-config.md) — Configuration keys and profiles

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.pubchem.batch_size` | Configurable | Batch size for PubChem API calls.【F:docs/pipelines/testitem-pubchem/29-testitem-pubchem-config.md†L17】 |
| `determinism.sort.by` | `['pubchem_cid']` | Primary sort key is `pubchem_cid`.【F:docs/pipelines/testitem-pubchem/29-testitem-pubchem-config.md†L19】 |
| `qc.thresholds.testitem.duplicate_ratio` | `0.0` | Critical: duplicates are not allowed.【F:docs/pipelines/testitem-pubchem/29-testitem-pubchem-config.md†L20】 |

**Inputs.** Requires PubChem CID identifiers from input CSV and fetches molecule data from PubChem PUG REST API with batch retrieval.【F:docs/pipelines/testitem-pubchem/00-testitem-pubchem-overview.md†L21-L24】

**Outputs.** Produces molecule dataset alongside standard meta/QC artifacts. Outputs include `testitem_pubchem_{date}.csv`, `testitem_pubchem_{date}_quality_report.csv`, and optionally `testitem_pubchem_{date}_meta.yaml` in extended mode.【F:docs/pipelines/testitem-pubchem/00-testitem-pubchem-overview.md†L25】

**Quality controls.** Coverage metrics for molecule data completeness, missing value detection, duplicate tracking, and validity checks feed a consolidated QC report.【F:docs/pipelines/testitem-pubchem/00-testitem-pubchem-overview.md†L63】

**CLI usage.**

```bash
# Standard extraction from PubChem
python -m bioetl.cli.main testitem --source pubchem \
  --config configs/pipelines/pubchem/testitem.yaml \
  --output-dir data/output/testitem-pubchem

# With input file containing PubChem CIDs
python -m bioetl.cli.main testitem --source pubchem \
  --config configs/pipelines/pubchem/testitem.yaml \
  --input-file data/input/pubchem_cids.csv \
  --output-dir data/output/testitem-pubchem
```

【F:docs/pipelines/testitem-pubchem/28-testitem-pubchem-cli.md†L14-L28】

---

### ChEMBL to UniProt Mapping (`chembl2uniprot_mapping`) {#chembl2uniprot_mapping}

**Purpose.** Creates mappings between ChEMBL target identifiers and UniProt accession numbers through the UniProt ID Mapping API. This mapping is essential for enriching ChEMBL target data with UniProt protein information.【F:docs/pipelines/28-chembl2uniprot-mapping.md†L9-L11】

**Documentation Structure:**

- [28-chembl2uniprot-mapping.md](28-chembl2uniprot-mapping.md) — Pipeline documentation

**Key configuration.**

| Key | Requirement / Default | Notes |
| --- | --- | --- |
| `sources.uniprot_idmapping.batch_size` | `100000` | Batch size for ID mapping (can be large).【F:docs/pipelines/28-chembl2uniprot-mapping.md†L121】 |
| `sources.uniprot_idmapping.rate_limit.max_calls` | `2` | Critical: ≤ 2 (UniProt API quota).【F:docs/pipelines/28-chembl2uniprot-mapping.md†L79】 |
| `sources.uniprot_idmapping.polling.enabled` | `true` | Enables polling for async job status.【F:docs/pipelines/28-chembl2uniprot-mapping.md†L130】 |
| `determinism.sort.by` | `['target_chembl_id', 'uniprot_accession']` | Composite sort keys.【F:docs/pipelines/28-chembl2uniprot-mapping.md†L147】 |

**Inputs.** Requires ChEMBL `target_chembl_id` values from input CSV and uses UniProt ID Mapping API to create mappings to UniProt accession numbers.【F:docs/pipelines/28-chembl2uniprot-mapping.md†L9-L11】

**Outputs.** Produces mapping dataset with columns `target_chembl_id` and `uniprot_accession` alongside standard meta/QC artifacts. Outputs include `chembl2uniprot_mapping_{date}.csv`, `chembl2uniprot_mapping_{date}_quality_report.csv`, and optionally `chembl2uniprot_mapping_{date}_meta.yaml` in extended mode.【F:docs/pipelines/28-chembl2uniprot-mapping.md†L11-L12】

**Quality controls.** Coverage metrics for mapping success rate, missing value detection, duplicate tracking, and validity checks feed a consolidated QC report.【F:docs/pipelines/28-chembl2uniprot-mapping.md†L192-L200】

**CLI usage.**

```bash
# Standard mapping run
python -m bioetl.cli.main chembl2uniprot-mapping \
  --config configs/pipelines/uniprot/chembl2uniprot.yaml \
  --output-dir data/output/chembl2uniprot-mapping

# With input file containing ChEMBL target IDs
python -m bioetl.cli.main chembl2uniprot-mapping \
  --config configs/pipelines/uniprot/chembl2uniprot.yaml \
  --input-file data/input/chembl_targets.csv \
  --output-dir data/output/chembl2uniprot-mapping
```

【F:docs/pipelines/28-chembl2uniprot-mapping.md†L15-L29】

## Determinism & Invariant Matrix

| Pipeline | Sort keys | Hashing invariants | Schema reference |
| --- | --- | --- | --- |
| Activity | `['assay_id', 'testitem_id', 'activity_id']` | Row/business hashes generated with canonicalized values under the SHA256 policy (`hash_row`, `hash_business_key`).【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `chembl_activity` schema module in the source interface matrix.【F:docs/sources/INTERFACE_MATRIX.md†L7-L13】 |
| Assay | `['assay_id']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `chembl_assay` schema per interface matrix.【F:docs/sources/INTERFACE_MATRIX.md†L7-L13】 |
| Target (ChEMBL) | `['target_id']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `chembl_target` schema per interface matrix.【F:docs/sources/INTERFACE_MATRIX.md†L7-L13】 |
| Document (ChEMBL) | `['year', 'document_id']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `chembl_document` schema per interface matrix.【F:docs/sources/INTERFACE_MATRIX.md†L7-L13】 |
| TestItem (ChEMBL) | `['testitem_id']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `chembl_testitem` schema per interface matrix.【F:docs/sources/INTERFACE_MATRIX.md†L7-L13】 |
| Document PubMed | `['pmid']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `document_pubmed` schema per interface matrix. |
| Document Crossref | `['doi']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `document_crossref` schema per interface matrix. |
| Document OpenAlex | `['openalex_id']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `document_openalex` schema per interface matrix. |
| Document Semantic Scholar | `['semantic_scholar_id']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `document_semantic_scholar` schema per interface matrix. |
| Target UniProt | `['uniprot_accession']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `target_uniprot` schema per interface matrix. |
| Target IUPHAR | `['iuphar_object_id']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `target_iuphar` schema per interface matrix. |
| TestItem PubChem | `['pubchem_cid']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `testitem_pubchem` schema per interface matrix. |
| ChEMBL2UniProt Mapping | `['target_chembl_id', 'uniprot_accession']` | Same SHA256-based row/business hashes per determinism policy.【F:docs/determinism/00-determinism-policy.md†L28-L70】 | `chembl2uniprot_mapping` schema per interface matrix. |

The determinism policy also standardizes value canonicalization, serialization, and `meta.yaml` shape, ensuring cross-pipeline reproducibility when identical inputs and configurations are supplied.【F:docs/determinism/00-determinism-policy.md†L36-L118】

## Module Layout

Код пайплайнов `activity` и `assay` перенесён в модули `src/bioetl/pipelines/chembl_<entity>.py`, а в `src/bioetl/sources/chembl/<entity>/pipeline.py` остались прокси-импорты для обратной совместимости CLI и тестов.【F:src/bioetl/pipelines/chembl_activity.py†L1-L210】【F:src/bioetl/pipelines/chembl_assay.py†L1-L210】

## Common Configuration

Общие параметры ChEMBL берутся из include `configs/includes/chembl_source.yaml`: `base_url`, `batch_size`, `max_url_length`, заголовки и флаг `rate_limit_jitter`. Эти ключи наследуются всеми профильными конфигами ChEMBL.【F:src/bioetl/configs/includes/chembl_source.yaml†L1-L11】

Профильные конфиги для отдельных пайплайнов:

- `configs/pipelines/activity.yaml` задаёт собственный `batch_size`, заголовок `User-Agent` и QC-политику дубликатов/единиц; сортировка фиксирована по `activity_id`.【F:src/bioetl/configs/pipelines/activity.yaml†L1-L86】
- `configs/pipelines/assay.yaml` использует такой же include, но переопределяет сортировку (`assay_chembl_id`, `row_subtype`) и ключевые поля BAO для записи; QC следит за `fallback_usage_rate`.【F:src/bioetl/configs/pipelines/assay.yaml†L1-L95】
- `configs/pipelines/document.yaml` подключает внешние источники (`sources.pubmed`, `sources.crossref`, `sources.openalex`, `sources.semantic_scholar`) с их лимитами, переменными окружения (`PUBMED_TOOL`, `PUBMED_EMAIL`, `CROSSREF_MAILTO`, `SEMANTIC_SCHOLAR_API_KEY`) и расширенным QC по покрытию DOI/PMID и конфликтам заголовков.【F:src/bioetl/configs/pipelines/document.yaml†L1-L194】
- `configs/pipelines/target.yaml` объявляет профили HTTP/источников для ChEMBL, UniProt (поиск, idmapping, orthologs) и IUPHAR; QC контролирует успех обогащений и fallback-стратегии.【F:src/bioetl/configs/pipelines/target.yaml†L1-L200】
- `configs/pipelines/testitem.yaml` описывает postprocess-обогащение `pubchem_dataset` и фиксирует порядок колонок для выдачи ChEMBL + PubChem, а QC отслеживает дубликаты и долю fallback.【F:src/bioetl/configs/pipelines/testitem.yaml†L1-L151】

## Merge Policy Summary

Политики слияния данных для различных пайплайнов:

- **Документы:** консолидация DOI/PMID/метаданных в порядке Crossref → PubMed → OpenAlex → ChEMBL с фиксацией источника в `*_source` колонках; реализация опирается на приоритеты из `FIELD_PRECEDENCE` и merge-хелперы.【F:refactoring/DATA_SOURCES.md†L33-L39】【F:src/bioetl/sources/document/merge/policy.py†L17-L200】
- **Таргеты:** ChEMBL даёт каркас, UniProt побеждает по именам/генам, IUPHAR — по фармакологической классификации; итоговая политика описана в матрице источников.【F:refactoring/DATA_SOURCES.md†L35-L37】
- **Ассайы:** конфликтующие тип/формат нормализуются через BAO, приоритет за словарём BAO относительно raw ChEMBL.【F:refactoring/DATA_SOURCES.md†L37-L38】
- **Test items:** поля идентичности и синонимы берутся из PubChem, ChEMBL служит fallback; расхождения помечаются для QC.【F:refactoring/DATA_SOURCES.md†L38-L39】
- **Активности:** ChEMBL — единственный источник, ключ — комбинация (`assay_chembl_id`, `molecule_chembl_id`, `standard_type`, `relation`, `value`, `units`) с выбором записи по корректности единиц.【F:refactoring/DATA_SOURCES.md†L39-L39】

## Tests Overview

- `tests/sources/chembl/test_client.py`, `test_parser.py`, `test_normalizer.py`, `test_schema.py`, `test_pipeline_e2e.py` покрывают клиентские хелперы, парсинг, нормализацию и контроль колонок Activity/Assay/TestItem.【F:tests/sources/chembl/test_client.py†L1-L23】【F:tests/sources/chembl/test_pipeline_e2e.py†L1-L12】
- `tests/unit/test_utils_chembl.py` проверяет утилиты и защиту от регрессий в общих функциях ChEMBL.【F:tests/unit/test_utils_chembl.py†L1-L44】
