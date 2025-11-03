# Documentation Remediation Plan — branch refactoring_001

This document provides a detailed plan for resolving the inconsistencies identified in `AUDIT_DIFF.md`. Each section corresponds to a Remediation-ID and is designed to be an atomic set of changes.

---

### **FP-GLOBAL-001 — Fix Incorrect Branch in All `[ref]` Links**

-   **Scope**: All `*.md` files in the `docs/` directory and the root `README.md`.
-   **Actions**:
    -   [ ] Perform a global search-and-replace for `@test_refactoring_32` and replace it with `@refactoring_001` in all `[ref: repo:...]` links.
-   **Acceptance**:
    -   [ ] A `grep` for `@test_refactoring_32` in the repository's markdown files returns no results.
-   **PR Package**: Atomic
-   **Effort/Risk**: Low. This is a simple, low-risk, automated change.

---

### **FP-NAV-001 — Rebuild Navigation for Missing Files**

-   **Scope**: `[ref: repo:docs/INDEX.md@refactoring_001]`
-   **Actions**:
    -   [ ] Remove all links pointing to non-existent directories: `docs/requirements/`, `docs/qc/`, and `docs/architecture/`.
    -   [ ] Restructure the index to reflect the actual documentation present in the `docs/` directory. Create new sections for `ETL Contract`, `Determinism`, `HTTP`, and `Logging`.
-   **Acceptance**:
    -   [ ] All links in `docs/INDEX.md` point to existing files.
    -   [ ] The navigation structure in `docs/INDEX.md` logically represents the content in the `docs/` directory.
-   **PR Package**: Batch (best combined with other navigation and structural fixes)
-   **Effort/Risk**: Medium. Requires careful restructuring of the main navigation file.

---

### **FP-NAV-002 — Address Missing `PIPELINES.md`**

-   **Scope**: `[ref: repo:docs/INDEX.md@refactoring_001]`, `[ref: repo:README.md@refactoring_001]`
-   **Actions**:
    -   [ ] Since `docs/pipelines/PIPELINES.md` is missing, update the links in `INDEX.md` and `README.md` to point to a suitable replacement, such as `[ref: repo:docs/pipelines/10-chembl-pipelines-catalog.md@refactoring_001]`.
    -   [ ] *Alternatively*, create a new `PIPELINES.md` that acts as a proper catalog, summarizing the content from the other files in the `docs/pipelines/` directory.
-   **Acceptance**:
    -   [ ] The links for "Pipeline Contracts" in `INDEX.md` and `README.md` are no longer broken.
-   **PR Package**: Atomic
-   **Effort/Risk**: Low to Medium, depending on whether a new file needs to be created.

---

### **FP-STRUCT-001 — Remove Redundant Navigation Pages**

-   **Scope**: `[ref: repo:docs/INDEX.md@refactoring_001]`, `[ref: repo:README.md@refactoring_001]`, `[ref: repo:docs/cli/CLI.md@refactoring_001]`, `[ref: repo:docs/configs/CONFIGS.md@refactoring_001]`
-   **Actions**:
    -   [ ] Update `INDEX.md` and `README.md` to link directly to the content files: `docs/cli/00-cli-overview.md` and `docs/configs/00-typed-configs-and-profiles.md`.
    -   [ ] Delete the now-obsolete `docs/cli/CLI.md` and `docs/configs/CONFIGS.md` files.
-   **Acceptance**:
    -   [ ] Navigation from `README.md` and `INDEX.md` goes directly to the content.
    -   [ ] The redundant files are removed.
-   **PR Package**: Batch (combine with FP-NAV-001)
-   **Effort/Risk**: Low.

---

### **FP-LANG-001 — Standardize Language**

-   **Scope**: `[ref: repo:docs/INDEX.md@refactoring_001]`
-   **Actions**:
    -   [ ] Translate the Russian sections (`карта-документации`, `как-поддерживать-согласованность`) into English.
-   **Acceptance**:
    -   [ ] The entire `INDEX.md` file is in English.
-   **PR Package**: Atomic
-   **Effort/Risk**: Low.

---

### **FP-CLI-001-to-007 & FP-CONF-001-to-002 — Synchronize CLI and Config Documentation**

-   **Scope**: All files in `docs/cli/`, `docs/configs/`, `docs/etl_contract/`, and `README.md`.
-   **Actions**:
    -   [ ] **Establish a Single Source of Truth**: Decide whether the CLI uses a **static registry** or **automatic discovery** and make all documents consistent with that decision. (FP-CLI-001)
    -   [ ] **Standardize Configuration Syntax**: Decide whether `extends` or `profile` is the correct key for inheritance and enforce it in all YAML examples. (FP-CONF-001)
    -   [ ] **Unify Command Lists & Naming**: Update the `README.md` to include all commands listed in `docs/cli/01-cli-commands.md`. Standardize on a single naming convention (e.g., `activity`). (FP-CLI-002, FP-CLI-006)
    -   [ ] **Unify Config Paths**: Standardize all configuration path examples to a single, consistent format (e.g., `configs/pipelines/...`). (FP-CLI-003, FP-CONF-002)
    -   [ ] **Update CLI Flags**: Add the `--input-file` and `--set` flags to all relevant overviews. Clarify that `--config` is required. (FP-CLI-004, FP-CLI-005, FP-CLI-007)
-   **Acceptance**:
    -   [ ] A single, consistent explanation of CLI discovery and config inheritance is used everywhere.
    -   [ ] All examples of CLI commands and config paths are identical and correct.
    -   [ ] All lists of CLI flags are complete and consistent.
-   **PR Package**: Batch (this is a large, interconnected set of changes)
-   **Effort/Risk**: High. This requires a definitive decision on the correct architecture and syntax, followed by careful and widespread changes.

---

### **FP-PIPE-001-to-003 — Restructure Pipeline Documentation**

-   **Scope**: All files in `docs/pipelines/`.
-   **Actions**:
    -   [ ] Create a clear, concise documentation card for the `target` pipeline. (FP-PIPE-001)
    -   [ ] Consolidate the two `testitem` documents (`07a...` and `07b...`) into a single, coherent file. (FP-PIPE-002)
    -   [ ] Evaluate `CONTRACT.md`, merge any unique, valuable information into `00-pipeline-base.md`, and delete the file. (FP-PIPE-003)
-   **Acceptance**:
    -   [ ] All pipelines mentioned in `README.md` have a clear and consistent documentation entry point.
    -   [ ] There are no redundant or confusingly structured files in the `docs/pipelines/` directory.
-   **PR Package**: Batch
-   **Effort/Risk**: Medium. Requires content consolidation and restructuring.
