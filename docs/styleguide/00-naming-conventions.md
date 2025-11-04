# Documentation Naming Conventions

This document outlines the standard naming conventions for documentation files within the `bioetl` project. Following these conventions ensures consistency and predictability in the documentation structure.

## 1. General Principles

-   **Language**: All filenames **MUST** be in English.
-   **Case**: All filenames **MUST** be in `lowercase`.
-   **Separators**: Words in filenames **MUST** be separated by hyphens (`-`). Underscores (`_`) **SHOULD NOT** be used.

## 2. File Naming by Type

### 2.1. Sequenced Content Documents

Documents that are part of a logical sequence or represent a chapter in a larger guide **SHOULD** be prefixed with a two-digit number (`NN-`). This ensures they are ordered correctly in file listings and navigation.

-   **Format**: `NN-topic-name.md`
-   **Examples**:
    -   `docs/cli/00-cli-overview.md`
    -   `docs/cli/01-cli-commands.md`
    -   `docs/qc/02-golden-tests.md`

### 2.2. Index and Landing Page Documents

Primary index files or landing pages for a directory **SHOULD** be named `INDEX.md` or use an all-caps name that clearly describes their purpose. They **SHOULD NOT** have a numeric prefix.

-   **Format**: `INDEX.md` or `OVERVIEW.md`
-   **Examples**:
    -   `docs/INDEX.md`
    -   `docs/sources/INTERFACE_MATRIX.md`

### 2.3. README Files

`README.md` files **SHOULD** be used as landing pages for specific subdirectories, especially within `docs/pipelines/sources/`, to provide a brief overview of the contents.

-   **Format**: `README.md`
-   **Example**: `docs/pipelines/10-chembl-pipelines-catalog.md` (ChEMBL source documentation was consolidated into the main catalog)

## 3. Summary of Conventions

| Document Type | Naming Convention | Example |
|---|---|---|
| Sequenced Content | `NN-topic-name.md` | `01-metrics-catalog.md` |
| Main Index File | `INDEX.md` | `docs/INDEX.md` |
| Directory Overview | `README.md` or `SECTION.md` | `README.md` |
