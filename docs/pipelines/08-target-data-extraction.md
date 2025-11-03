# ChEMBL Target Extraction Pipeline

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

This document describes the `target` pipeline, which is responsible for extracting and processing target data from the ChEMBL database.

## 1. Overview

The `target` pipeline extracts information about macromolecular targets of bioactive compounds. This data is essential for understanding drug-target interactions and mechanisms of action. The pipeline enriches the core ChEMBL target data with information from external sources like UniProt and IUPHAR/BPS Guide to PHARMACOLOGY.

## 2. CLI Command

The pipeline is executed via the `target` CLI command.

**Usage:**
```bash
python -m bioetl.cli.main target [OPTIONS]
```

**Example:**
```bash
python -m bioetl.cli.main target \
  --config configs/pipelines/chembl/target.yaml \
  --output-dir data/output/target
```

## 3. Configuration

The pipeline's behavior is controlled by a YAML configuration file, typically located at `configs/pipelines/chembl/target.yaml`. This file specifies the data sources, extraction parameters, and enrichment options.

-   **Primary Source**: ChEMBL API `/target.json` endpoint.
-   **Enrichment Sources**: UniProt, IUPHAR.

## 4. Component Architecture

The `target` pipeline follows the standard source architecture, utilizing a stack of specialized components for its operation.

| Component | Implementation |
|---|---|
| **Client** | `[ref: repo:src/bioetl/sources/chembl/target/client/target_client.py@refactoring_001]` |
| **Parser** | `[ref: repo:src/bioetl/sources/chembl/target/parser/target_parser.py@refactoring_001]` |
| **Normalizer** | `[ref: repo:src/bioetl/sources/chembl/target/normalizer/target_normalizer.py@refactoring_001]` |
| **Schema** | `[ref: repo:src/bioetl/schemas/chembl_target.py@refactoring_001]` |

## 5. Key Identifiers

-   **Business Key**: `target_chembl_id`
-   **Sort Key**: `target_chembl_id`
