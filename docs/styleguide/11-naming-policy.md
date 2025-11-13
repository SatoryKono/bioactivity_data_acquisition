# 11 Naming Policy

## 1. Purpose and scope

This document defines the naming policy for the `bioetl` repository:

- Python source code (`src/bioetl/**`)
- ETL pipelines (`src/bioetl/pipelines/**`)
- Tests (`tests/**`)
- Documentation (`docs/**`)
- Configuration and QC assets (`configs/**`, `qc/**`)

  The goal is:

- deterministic and machine-checkable naming;
- consistency across code, tests, and documentation;
- a stable basis for automated linting and CI checks.

  The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are to be interpreted as describ
  This policy does **not** redefine the full Python style guide or documentation standards; it constrains naming

---

## 2. Scope and sources of truth

Naming rules in this document are consistent with, and subordinate to, the following documents:

- `docs/styleguide/00-naming-conventions.md`
- Global naming conventions for the repository.
- *Source of truth* for documentation file naming and general terminology.
- `docs/styleguide/10-documentation-standards.md`
- Documentation structure, sections, front matter, and quality requirements.
- This policy MUST NOT contradict these standards.
- `docs/styleguide/01-python-code-style.md`
- Python code style (PEP 8 baseline, imports, formatting, etc.).
- This policy only adds naming constraints (identifiers, file names, paths) and MUST remain compatible with it.

  If this policy and any of the documents above conflict, the order of precedence is:

1. `00-naming-conventions.md`
2. `10-documentation-standards.md`
3. `01-python-code-style.md`
4. This `11-naming-policy.md`

---

## 3. Top-level directory layout

This section describes the intended high-level layout. The matrix in section 8 makes it precise.
Top-level logical components (relative to repository root):

- `src/bioetl/api/`

  Public programmatic API surface (facades, high-level entrypoints).

- `src/bioetl/cli/`

  Command-line interfaces, argument parsers, thin wrappers over `core` / `pipelines`.

- `src/bioetl/clients/`

  External service and database clients (ChEMBL, PubChem, UniProt, IUPHAR, PubMed, Crossref, OpenAlex, Semanti

- `src/bioetl/core/`

  Core abstractions and shared logic (ETL primitives, orchestration, common models, shared validation).

- `src/bioetl/pipelines/`

  ETL pipelines organized by `provider/entity/stage`.
  This is the primary focus of the pipeline naming rules.

- `src/bioetl/schemas/`

  Data schemas (Pydantic models, marshmallow schemas, or similar) and schema helpers.

- `src/bioetl/utils/`

  Generic helpers not specific to any provider or pipeline.

- `src/bioetl/tools/`

  Internal tools and maintenance scripts (linting, migration utilities, data inspections) that may be exposed

- `configs/` (or `config/` if present)

  Configuration files for pipelines, providers, environments, CI jobs, and linters.

- `qc/`

  QC reports, golden files, expectations, and metrics definitions.

- `docs/`

  User and developer documentation, including style guides, pipeline docs, and index files.

- `tests/`

  Test suite for code, pipelines, CLI, and docs. Mirrors the layout of `src/` where applicable.
  Each directory MUST conform to the file type and pattern matrix in section 8.

---

## 4. Object naming policy (Python)

### 4.1 General principles

The following rules extend `01-python-code-style.md` and are expressed as **NR-rules**.
Global constraints:

- Identifiers MUST be ASCII letters, digits, and underscores only.
- Names MUST be descriptive and domain-specific enough to be searchable.
- Abbreviations SHOULD be consistent and documented if non-obvious.

### 4.2 Identifier regexes

The canonical regex patterns used by tooling are:

```regex

# Modules / packages (NR-001)

^[a-z0-9_]+$

# Public class names (NR-002)

^[A-Z][A-Za-z0-9]+$

# Function and method names (NR-003)

^[a-z_][a-z0-9_]*$

# Constants (NR-004)

^[A-Z][A-Z0-9_]*$

# Exception classes (NR-007)

^[A-Z][A-Za-z0-9]*Error$

# Schema / model classes (NR-008)

^[A-Z][A-Za-z0-9]*(Schema|Model)$

```

Tooling SHOULD reuse these regexes directly.

### 4.3 Modules and packages

#### NR-001 (MUST, code)

Python modules and packages MUST use `snake_case`:

- Pattern: `^[a-z0-9_]+$`
- Examples:
- OK: `activity_loader.py`, `chembl_client`, `uniprot_parser`
- Violation: `ActivityLoader.py`, `ChEMBLClient.py`, `uniprot-Parser.py`

  Package directories MUST contain `__init__.py`.

### 4.4 Classes

#### NR-002 (MUST, code)

Public classes MUST use `PascalCase`:

- Pattern: `^[A-Z][A-Za-z0-9]+$`
- Examples:
- OK: `ActivityRecord`, `PubchemClient`, `PipelineRunner`
- Violation: `activity_record`, `pubChemClient`, `pipeline_runner`

  **NR-007 (MUST, code)**
  Custom exception classes MUST end with `Error`:

- Pattern: `^[A-Z][A-Za-z0-9]*Error$`
- Examples:
- OK: `InvalidSchemaError`, `ProviderConfigError`
- Violation: `InvalidSchemaException`, `ConfigFailure`

### 4.5 Functions and methods

#### NR-003 (MUST, code)

Functions and methods MUST use `snake_case`:

- Pattern: `^[a-z_][a-z0-9_]*$`
- Examples:
- OK: `load_chembl_activities`, `validate_record`, `run_pipeline`
- Violation: `LoadChemblActivities`, `validateRecord`, `Run-Pipeline`

  Factory and converter helpers:
  **NR-009 (SHOULD, code)**
  Factory/constructor functions SHOULD start with `from_` (`from_config`, `from_raw_row`).
  Converters SHOULD start with `to_` (`to_dataframe`, `to_schema`).

### 4.6 Constants and configuration keys

#### NR-004 (MUST, code)

Python constants MUST use `UPPER_SNAKE_CASE`:

- Pattern: `^[A-Z][A-Z0-9_]*$`
- Examples:
- OK: `DEFAULT_BATCH_SIZE`, `CHEMBL_API_URL`
- Violation: `DefaultBatchSize`, `chemblApiUrl`

  Configuration key naming:
  **NR-010 (SHOULD, config)**
  Configuration keys in `.yaml/.toml/.json` SHOULD use lower `snake_case`:

- Examples:
- OK: `batch_size`, `max_retries`, `provider_name`
- Violation: `BatchSize`, `maxRetries`

### 4.7 Private and dunder names

#### NR-005 (MUST, code)

Private helpers MUST start with a single underscore `_`:

- Examples:
- `_normalize_smiles`, `_split_batches`

  **NR-006 (MUST, code)**
  Double underscore (`__name`) and double-sided dunder names (`__init__`) MUST be reserved for Python internals

### 4.8 Schemas and models

#### NR-008 (SHOULD, code)

Schema and model classes SHOULD end with `Schema` or `Model`:

- Pattern: `^[A-Z][A-Za-z0-9]*(Schema|Model)$`
- Examples:
- OK: `ActivitySchema`, `PublicationModel`
- Violation: `Activity`, `PublicationRecord` (unless intentionally generic)

  Schemas MUST be placed in `src/bioetl/schemas/**` (see matrix).

---

## 5. Pipelines naming

### 5.1 Directory and file structure

Pipeline modules MUST follow the pattern:

```text
src/bioetl/pipelines/&lt;provider&gt;/&lt;entity&gt;/&lt;stage&gt;.py

```

Formal regex:

```regex
^src/bioetl/pipelines/(?P&lt;provider&gt;[a-z0-9_]+)/(?P&lt;entity&gt;[a-z0-9_]+)/(?P&lt;stage&gt;[a-z0-9_]+)\.py$

```

#### PIPE-001 (MUST, code)

`&lt;provider&gt;`, `&lt;entity&gt;`, and `&lt;stage&gt;` MUST all match `^[a-z0-9_]+$`.
Examples:

- OK:

`src/bioetl/pipelines/chembl/activity/extract.py`

`src/bioetl/pipelines/pubchem/compound/transform.py`

`src/bioetl/pipelines/uniprot/protein/validate.py`

- Violation:

`src/bioetl/pipelines/ChEMBL/activityExtract.py`

`src/bioetl/pipelines/pubchem/Compound/transformActivities.py`

### 5.2 Providers

#### PIPE-002 (MUST, code)

`&lt;provider&gt;` MUST be a short, lowercase identifier of the upstream data source. It MUST be unique per external
Pattern:

```regex

^[a-z0-9_]+$

```

Examples (non-exhaustive):

```text

chembl
pubchem
uniprot
iuphar
pubmed
crossref
openalex
semanticscholar

```

The canonical list of providers MUST be defined once (e.g. `configs/providers.yaml`) and used by tooling. New

### 5.3 Entities

#### PIPE-003 (MUST, code)

`&lt;entity&gt;` identifies the logical domain entity being processed for a given provider. It MUST be lowercase `sn

Examples (non-exhaustive):

```text
activity
assay
compound
target
protein
publication
journal
author
mesh_term

```

The canonical list of entities SHOULD be maintained in a shared configuration (e.g. `configs/entities.yaml`) a

### 5.4 Stages

#### PIPE-004 (MUST, code)

`&lt;stage&gt;` MUST represent the pipeline stage type.
Main ETL stages (primary):

```text

extract
transform
validate
normalize
write
run

```

Typical helper / auxiliary stages:

```text

errors # error-specific helpers
descriptor # descriptor calculation
export # exports outside canonical write path
metrics # QC metrics computation
backfill # backfill/repair jobs
cleanup # cleanup and archival jobs

```

Rules:

- Primary ETL stages (`extract`, `transform`, `validate`, `normalize`, `write`, `run`) MUST follow semantic me
- `extract`: raw ingestion from provider.
- `transform`: normalisation, enrichment, structural changes.
- `validate`: validation, QC checks.
- `normalize`: standardisation of units, formats, vocabularies.
- `write`: writing to the canonical storage layer.
- `run`: high-level orchestration for the entire pipeline or entity.
- Helper modules (e.g. `errors.py`, `descriptor.py`) MUST contain logic dedicated to that concern for the `<pr
Examples:

- OK:

`src/bioetl/pipelines/chembl/activity/normalize.py`

`src/bioetl/pipelines/chembl/activity/errors.py`

`src/bioetl/pipelines/pubmed/publication/metrics.py`

- Violation:

`src/bioetl/pipelines/chembl/activity/validator.py` (SHOULD be `validate.py`)

`src/bioetl/pipelines/pubchem/compound/etl.py` (too generic for stage)

### 5.5 Relationship with schemas and configs

#### PIPE-005 (SHOULD, code/docs/config)

For each `src/bioetl/pipelines/&lt;provider&gt;/&lt;entity&gt;/` directory:

- there SHOULD be a corresponding schema module under `src/bioetl/schemas/`:
- Suggested pattern: `src/bioetl/schemas/&lt;provider&gt;_&lt;entity&gt;_schema.py`
- there SHOULD be one or more config files under `configs/pipelines/`:
- Suggested pattern: `configs/pipelines/&lt;provider&gt;/&lt;entity&gt;.yaml`
- there SHOULD be pipeline documentation files (section 7.2).

  This ensures traceability from pipeline code to schemas, configs, and docs.

---

## 6. Tests naming

### 6.1 General layout

Test layout MUST mirror the `src/bioetl` layout where possible.

#### TS-001 (MUST, tests)

Unit tests for module `src/bioetl/.../&lt;module&gt;.py` MUST be located in:

```text
tests/bioetl/.../test_&lt;module&gt;.py

```

with the same subdirectory path under `tests/bioetl/` as under `src/bioetl/`.
Regex:

```regex
^tests/bioetl/(.+)/test_[a-z0-9_]+\.py$

```

Examples:

- Code: `src/bioetl/core/batch_runner.py`

  Tests: `tests/bioetl/core/test_batch_runner.py`

- Code: `src/bioetl/pipelines/chembl/activity/extract.py`

  Tests: `tests/bioetl/pipelines/chembl/activity/test_extract.py`

### 6.2 Pipeline tests

#### TS-002 (MUST, tests)

Pipeline tests MUST follow `&lt;provider&gt;/&lt;entity&gt;/&lt;stage&gt;` layout:

```text
tests/bioetl/pipelines/&lt;provider&gt;/&lt;entity&gt;/test_&lt;stage&gt;.py

```

where `&lt;provider&gt;`, `&lt;entity&gt;`, `&lt;stage&gt;` match those in `src/bioetl/pipelines/`.
Examples:

- OK:

`src/bioetl/pipelines/uniprot/protein/validate.py`

`tests/bioetl/pipelines/uniprot/protein/test_validate.py`

- Violation:

`tests/bioetl/pipelines/uniprot/test_protein_validate.py` (flattened path)

### 6.3 Integration tests

#### TS-003 (SHOULD, tests)

Integration tests SHOULD be separated from unit tests, using either:

- directory level:
- `tests/integration/...`
- or filename suffix:
- `test_&lt;area&gt;_integration.py`
Pattern examples:

```regex

^tests/integration/.+\.py$
^tests/bioetl/.+test_[a-z0-9_]+_integration\.py$

```

The repository MUST choose one primary convention and apply it consistently (documented in `10-documentation-s

### 6.4 Golden tests

Golden tests rely on golden outputs stored in `qc/` or `tests/golden/`.

#### TS-004 (SHOULD, tests)

Recommended pattern:

- Test modules:

```text

tests/golden/test_&lt;area&gt;_golden.py

```

 Regex:

```regex

^tests/golden/test_[a-z0-9_]+_golden\.py$

```

- Golden artefacts:
- placed under `qc/golden/&lt;area&gt;/...`
- file names MUST be deterministic and documented (see `10-documentation-standards.md` / QC docs).
If the project already uses a different, established pattern for golden tests, that pattern MUST be documented

### 6.5 CLI tests

#### TS-005 (SHOULD, tests)

CLI tests SHOULD be placed under:

```text

tests/bioetl/cli/test_&lt;command&gt;.py

```

mirroring modules in `src/bioetl/cli/`.

---

## 7. Documentation naming

Documentation naming is governed primarily by `00-naming-conventions.md`. This policy references that document

### 7.1 General documentation files

#### DOC-001 (MUST, docs)

Sequential documentation files (style guides, high-level docs) MUST follow:

```text

NN-topic-name.md

```

where:

- `NN` is a two-digit sequence number (`00`–`99`);
- `topic-name` is lowercase `kebab-case` or `snake_case` (as defined in `00-naming-conventions.md`).
Regex:

```regex

^\d{2}-[a-z0-9]+(?:[-_][a-z0-9]+)*\.md$

```

Examples:

- OK: `00-naming-conventions.md`, `01-python-code-style.md`, `10-documentation-standards.md`, `11-naming-polic
- Violation: `naming-policy.md`, `docs_11_naming_policy.md`

#### DOC-002 (MAY, docs)

`README.md` MAY be used in directory roots (e.g. repo root, `docs/`, `docs/pipelines/&lt;provider&gt;/&lt;entity&gt;/`) to

#### DOC-003 (SHOULD, docs)

`INDEX.md` SHOULD be used as an index / table of contents inside documentation subtrees where needed.

### 7.2 Pipeline documentation

Pipeline documentation MUST be consistent with pipelines in `src/bioetl/pipelines/`.

#### DOC-004 (MUST, docs)

For pipeline `&lt;provider&gt;/&lt;entity&gt;`, documentation files MUST be placed under:

```text

docs/pipelines/&lt;provider&gt;/&lt;entity&gt;/

```

and use the pattern:

```text

NN-&lt;entity&gt;-&lt;provider&gt;-<topic>.md

```

Regex:

```regex

^docs/pipelines/(?P&lt;provider&gt;[a-z0-9_]+)/(?P&lt;entity&gt;[a-z0-9_]+)/\d{2}-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+\.md$

```

Constraints:

- `&lt;provider&gt;` and `&lt;entity&gt;` MUST match pipeline directories.
- The second component after `NN-` MUST be the `&lt;entity&gt;`.
- The third component MUST be the `&lt;provider&gt;`.
- `<topic>` MUST describe the scope (`overview`, `extract`, `transform`, `validate`, `normalize`, `write`, `ru
Examples:

- OK:

`docs/pipelines/chembl/activity/20-activity-chembl-overview.md`

`docs/pipelines/chembl/activity/21-activity-chembl-extract.md`

`docs/pipelines/pubmed/publication/20-publication-pubmed-overview.md`

- Violation:

`docs/pipelines/chembl/activity/20-chembl-activity-overview.md` (entity/provider order swapped)

`docs/pipelines/chembl/activity/overview.md` (missing prefix)

### 7.3 Styleguide documents

#### DOC-005 (MUST, docs)

All styleguide documents MUST live under:

```text
docs/styleguide/

```

and follow `NN-topic-name.md` pattern. This document is expected at:

```text
docs/styleguide/11-naming-policy.md

```

---

## 8. Directory → File Type → Expected Patterns

This section provides a matrix of expected file patterns per top-level directory.
Patterns are given as regex or glob-like text. Referenced rule IDs are normative.

### 8.1 Matrix

| Directory | File types | Expected patterns
| ----------------------------- | ---------------------------- | ---------------------------------------------
| `src/bioetl/api/` | Python modules | `^[a-z0-9_]+\.py$`
| `src/bioetl/cli/` | Python CLI modules | `^[a-z0-9_]+\.py$` (e.g. `run_pipeline.py`)
| `src/bioetl/clients/` | Provider client modules | `^[a-z0-9_]+\.py$` (e.g. `chembl_client.py`)
| `src/bioetl/core/` | Core modules | `^[a-z0-9_]+\.py$`
| `src/bioetl/pipelines/` | Pipeline packages | `&lt;provider&gt;/&lt;entity&gt;/` directories (`^[a-z0-9
|`src/bioetl/pipelines/` | Pipeline stage modules | `&lt;stage&gt;.py`(`^[a-z0-9_]+\.py$`)
|`src/bioetl/schemas/` | Schema modules | `^[a-z0-9_]+\.py$` (e.g. `chembl_activity_sch
| `src/bioetl/utils/` | Utility modules | `^[a-z0-9_]+\.py$`
| `src/bioetl/tools/` | Internal tools | `^[a-z0-9_]+\.py$` (e.g. `naming_lint.py`)
| `src/bioetl/**/` | Package markers | `^__init__\.py$`
| `configs/` / `config/` | YAML / TOML / JSON configs | `^[a-z0-9_]+\.ya?ml$`, `^[a-z0-9_]+\.toml$`,
| `configs/pipelines/` | Pipeline configs | `&lt;provider&gt;_&lt;entity&gt;\.ya?ml` or `&lt;provider&gt;/<
|`qc/` | QC config / golden artefacts | `^[a-z0-9_]+\.ya?ml$`, deterministic file nam
|`docs/` | General docs | `^\d{2}-[a-z0-9]+(?:[-_][a-z0-9]+)*\.md$`,`R
| `docs/styleguide/` | Styleguide docs | `^\d{2}-[a-z0-9]+(?:[-_][a-z0-9]+)*\.md$`
| `docs/pipelines/<prov>/<ent>` | Pipeline docs | `\d{2}-&lt;entity&gt;-&lt;provider&gt;-<topic>\.md`
| `tests/bioetl/**` | Unit and pipeline tests | `test_[a-z0-9_]+\.py`
| `tests/bioetl/pipelines/**` | Pipeline tests | `test_&lt;stage&gt;\.py`
| `tests/integration/**` | Integration tests | `test_[a-z0-9_]+\.py`
| `tests/golden/**` | Golden tests | `test_[a-z0-9_]+_golden\.py`

Where both `configs/` and `config/` exist, the project MUST document which is canonical (and treat the other a

---

## 9. Enforcement and exceptions

### 9.1 Tooling and CI

#### ENF-001 (MUST)

Naming rules MUST be enforced by an automated linter integrated into CI.
Recommended implementation:

- a Python module, e.g. `src/bioetl/tools/naming_lint.py`;
- a CLI entrypoint (e.g. `bioetl-naming-lint`) that:
- scans the working tree;
- validates file paths and identifiers against regexes and rules in this document;
- produces a machine-readable report (JSON) and a human-readable summary.

  **ENF-002 (MUST)**
  CI MUST run a dedicated job (e.g. `naming-policy-check`) for all pushes and pull requests to protected branche
  **ENF-003 (SHOULD)**
  A pre-commit hook SHOULD be provided to run the naming linter on changed files.

### 9.2 Exceptions and whitelist

#### ENF-004 (MUST)

All naming exceptions MUST be explicitly documented in a machine-readable whitelist, e.g.:

```text
configs/naming_exceptions.yaml

```

Each exception entry MUST include:

- `path`: full repository-relative path;
- `rule_id`: ID of the violated rule (`NR-001`, `PIPE-004`, etc.);
- `reason`: short justification;
- `owner`: team or person responsible;
- `expiry` (optional): target date to remove the exception.

  Example (YAML):

```yaml

- path: src/bioetl/pipelines/legacy_provider/entity/etl.py
 rule_id: PIPE-004
 reason: Legacy monolithic pipeline awaiting refactor
 owner: etl-team
 expiry: 2026-01-01

```

The naming linter MUST treat listed exceptions as allowed, but SHOULD still report them as warnings.

#### ENF-005 (MUST)

No file or identifier may violate a **MUST**-level rule without either:

- being listed in `naming_exceptions.yaml`; or
- being brought into compliance.

---

## Appendix A. Rule catalog

Table of all naming rules with identifiers, scope, and severity.

| Rule ID | Category | Scope | Pattern / constraint (summary)
| -------: | ----------- | ---------------- | ----------------------------------------------------------------
| NR-001 | object | code | Modules/packages `^[a-z0-9_]+$`
| NR-002 | object | code | Classes `^[A-Z][A-Za-z0-9]+$`
| NR-003 | object | code | Functions/methods `^[a-z_][a-z0-9_]*$`
| NR-004 | object | code | Constants `^[A-Z][A-Z0-9_]*$`
| NR-005 | object | code | Private names start with `_`
| NR-006 | object | code | Dunders reserved
| NR-007 | object | code | Exceptions `^[A-Z][A-Za-z0-9]*Error$`
| NR-008 | object | code | Schema/model names end with `Schema` or `Model`
| NR-009 | object | code | Factory `from_…`, converter `to_…`
| NR-010 | object | config | Config keys lower `snake_case`
| FP-001 | file | code | Python files `^[a-z0-9_]+\.py$`
| FP-002 | file | code | Packages contain `__init__.py`
| FP-003 | file | code | Pipeline modules under `src/bioetl/pipelines/&lt;provider&gt;/&lt;entity&gt;
| FP-004 | file | config | Config files in`configs/` as `.yaml/.yml/.toml/.json` with `sna
| PIPE-001 | pipeline | code | `src/bioetl/pipelines/&lt;provider&gt;/&lt;entity&gt;/&lt;stage&gt;.py` with `snak
| PIPE-002 | pipeline | code |`&lt;provider&gt;` lowercase identifier
| PIPE-003 | pipeline | code | `&lt;entity&gt;` lowercase `snake_case` shared across providers
| PIPE-004 | pipeline | code | `&lt;stage&gt;` in `{extract, transform, validate, normalize, write, r
| PIPE-005 | pipeline | code/docs/config | For each `&lt;provider&gt;/&lt;entity&gt;` there SHOULD be schema, config, d
| TS-001 | tests | tests | `tests/bioetl/.../test_&lt;module&gt;.py` mirroring `src/bioetl/.../<m
| TS-002 | tests | tests |`tests/bioetl/pipelines/&lt;provider&gt;/&lt;entity&gt;/test_&lt;stage&gt;.py`
| TS-003 | tests | tests | Integration tests under `tests/integration/**` or `_integration`
| TS-004 | tests | tests | Golden tests as `tests/golden/test_&lt;area&gt;_golden.py`
| TS-005 | tests | tests | CLI tests under `tests/bioetl/cli/test_&lt;command&gt;.py`
| DOC-001 | docs | docs | `NN-topic-name.md` under `docs/`
| DOC-002 | docs | docs | Optional `README.md` in directories
| DOC-003 | docs | docs | Optional `INDEX.md` for directory indexes
| DOC-004 | docs | docs | Pipeline docs `docs/pipelines/&lt;provider&gt;/&lt;entity&gt;/NN-&lt;entity&gt;-<p
| DOC-005 | docs | docs | Styleguide docs under `docs/styleguide/` using `NN-topic-name.md
| ENF-001 | enforcement | infra | Automated naming linter in CI
| ENF-002 | enforcement | infra | Dedicated CI job (e.g.`naming-policy-check`)
| ENF-003 | enforcement | infra | Pre-commit hook for naming linter
| ENF-004 | enforcement | infra |`configs/naming_exceptions.yaml` as whitelist
| ENF-005 | enforcement | infra | MUST-rules violate only with whitelist entry

---

## Appendix B. MUST-level rules summary

Summary of all **MUST** rules with examples.

| Rule ID | Scope | Pattern / constraint | Example
| -------: | ------ | ------------------------------------------------------------------------------ | -------
| NR-001 | code | Modules use `snake_case` (`^[a-z0-9_]+\.py$`) | `activi
| NR-002 | code | Classes use`PascalCase` | `Pubche
| NR-003 | code | Functions/methods use `snake_case` | `load_c
| NR-004 | code | Constants use`UPPER_SNAKE_CASE` | `DEFAUL
| NR-005 | code | Private helpers start with `_` | `_norma
| NR-006 | code | Dunder names only for Python internals |`__init
| NR-007 | code | Exceptions end with `Error` | `Invali
| FP-001 | code | Python files`snake_case`(`^[a-z0-9_]+\.py$`) |`batch_
| FP-002 | code | Packages contain `__init__.py` | `src/bi
| FP-003 | code | Pipeline modules under`src/bioetl/pipelines/&lt;provider&gt;/&lt;entity&gt;/&lt;stage&gt;.py` | `src/bi
| FP-004 | config | Config files in `configs/` as `.yaml/.yml/.toml/.json`, `snake_case` names | `config
| PIPE-001 | code | Pipeline path uses`&lt;provider&gt;/&lt;entity&gt;/&lt;stage&gt;.py` | `src/bi
| PIPE-002 | code | `&lt;provider&gt;` is lowercase identifier | `chembl
| PIPE-003 | code |`&lt;entity&gt;` is lowercase `snake_case` | `activi
| PIPE-004 | code | `&lt;stage&gt;` from controlled set or documented helper | `extrac
| TS-001 | tests | Unit tests mirror`src/bioetl` layout: `tests/bioetl/.../test_&lt;module&gt;.py` | `src/bi
| TS-002 | tests | Pipeline tests: `tests/bioetl/pipelines/&lt;provider&gt;/&lt;entity&gt;/test_&lt;stage&gt;.py` | `tests/
| DOC-001 | docs | General docs:`NN-topic-name.md` | `10-doc
| DOC-004 | docs | Pipeline docs: `docs/pipelines/<prov>/<ent>/NN-&lt;entity&gt;-&lt;provider&gt;-<topic>.md` | `docs/p
| DOC-005 | docs | Styleguide docs under`docs/styleguide/` with `NN-topic-name.md` | `docs/s
| ENF-001 | infra | Automated naming linter in CI | CI job
| ENF-002 | infra | Dedicated CI job for naming policy | `naming
| ENF-004 | infra |`configs/naming_exceptions.yaml` for whitelist | Legacy
| ENF-005 | infra | MUST-rule violations only allowed if whitelisted | Legacy

---

## Appendix C. Naming policy vs current repository tree

The table below defines the required format for reporting discrepancies between this policy and the current re

| path | current_name | expected_pattern | violation_type | suggested_action
| ---- | ------------ | ---------------- | -------------------- | ---------------------------------
| … | … | … | `pattern_mismatch` | `rename_to:<new_name>`
| … | … | … | `directory_mismatch` | `move_to:<new_path>`
| … | … | … | `legacy_exception` | `document_as_exception:<rule_id>`
| … | … | … | `rule_too_strict` | `relax_rule:<rule_id>`

The actual rows MUST be produced by running the naming linter against the real file tree, using the rules and
