# Documentation Naming Conventions

This document outlines the standard naming conventions for documentation files within the `bioetl` project. Following these conventions ensures consistency and predictability in the documentation structure.

## 1. General Principles

- **Language**: All filenames **MUST** be in English.
- **Case**: All filenames **MUST** be in `lowercase`.
- **Separators**: Words in filenames **MUST** be separated by hyphens (`-`). Underscores (`_`) **SHOULD NOT** be used.

### 1.1. Pipeline Documentation File Naming

**Format**: `<NN>-<entity>-<source>-<topic>.md`

Pipeline documentation files **MUST** follow this canonical naming convention:

- `NN`: Two-digit sequential number (00–99) for stable sorting and ordering.
- `entity`: Domain entity name (`activity`, `assay`, `target`, `document`, `testitem`).
- `source`: Data source identifier (`chembl`, `uniprot`, `iuphar`, `pubchem`, `pubmed`, `crossref`, `openalex`, `semantic-scholar`).
- `topic`: Fixed token for the documentation section (see table below).

**Example**: `09-document-chembl-extraction.md` (canonical example).

#### 1.1.1. Topic Tokens and Stage/Contract Mapping

| Architecture/Stage | Topic Token | Notes |
|---|---|---|
| extract | `extraction` | Matches example usage |
| transform | `transformation` | |
| validate (Pandera) | `validation` | Version/column freeze policy |
| write / I/O | `io` | Format, stable sort-keys, atomic write |
| run (orchestration) | `run` | If a separate file is needed |
| determinism & lineage | `determinism` | `hash_row`, `hash_business_key`, `meta.yaml`, UTC |
| QC/QA & golden tests | `qc` | Metrics and thresholds |
| logging/tracing | `logging` | Format and mandatory fields |
| cli | `cli` | Exact commands and exit-codes |
| configuration | `config` | Keys/profiles/defaults |
| schema (Pandera) | `schema` | `schema_id/version`, dtypes |

#### 1.1.2. Link and Anchor Rules

- File H1 heading **MUST** duplicate the file name in title case: `# 09 Document ChEMBL Extraction`.
- Internal anchors are derived from second-level headings in kebab-case.
- All relative links **MUST** pass `.lychee.toml` validation.

**File section (example)**: `09-document-chembl-extraction.md`

## 2. File Naming by Type

### 2.1. Sequenced Content Documents

Documents that are part of a logical sequence or represent a chapter in a larger guide **SHOULD** be prefixed with a two-digit number (`NN-`). This ensures they are ordered correctly in file listings and navigation.

- **Format**: `NN-topic-name.md`
- **Examples**:
  - `docs/cli/00-cli-overview.md`
  - `docs/cli/01-cli-commands.md`
  - `docs/qc/02-golden-tests.md`

### 2.2. Index and Landing Page Documents

Primary index files or landing pages for a directory **SHOULD** be named `INDEX.md` or use an all-caps name that clearly describes their purpose. They **SHOULD NOT** have a numeric prefix.

- **Format**: `INDEX.md` or `OVERVIEW.md`
- **Examples**:
  - `docs/INDEX.md`
  - `docs/sources/INTERFACE_MATRIX.md`

### 2.3. README Files

`README.md` files **SHOULD** be used as landing pages for specific subdirectories, especially within `docs/pipelines/sources/`, to provide a brief overview of the contents.

- **Format**: `README.md`
- **Example**: `docs/pipelines/10-chembl-pipelines-catalog.md` (ChEMBL source documentation was consolidated into the main catalog)

## 3. Summary of Conventions

| Document Type | Naming Convention | Example |
|---|---|---|
| Sequenced Content | `NN-topic-name.md` | `01-metrics-catalog.md` |
| Main Index File | `INDEX.md` | `docs/INDEX.md` |
| Directory Overview | `README.md` or `SECTION.md` | `README.md` |
