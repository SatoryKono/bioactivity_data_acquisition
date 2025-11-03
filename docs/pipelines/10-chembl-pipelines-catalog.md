# ChEMBL Pipelines Catalog

This document provides a detailed catalog for each of the core ChEMBL data extraction pipelines. Each card describes the pipeline's purpose, configuration, and behavior, strictly within the scope of extracting data from the ChEMBL API.

## 1. Coverage Matrix

| Pipeline | CLI Command | Configuration | ChEMBL Endpoint | Output Artifact |
|---|---|---|---|---|
| **Activity** | `activity` | `[ref: repo:src/bioetl/configs/pipelines/chembl/activity.yaml@refactoring_001]` | `/activity.json` | CSV/Parquet |
| **Assay** | `assay` | `[ref: repo:src/bioetl/configs/pipelines/chembl/assay.yaml@refactoring_001]` | `/assay.json` | CSV/Parquet |
| **Target** | `target` | `[ref: repo:src/bioetl/configs/pipelines/chembl/target.yaml@refactoring_001]` | `/target.json` | CSV/Parquet |
| **Document** | `document` | `[ref: repo:src/bioetl/configs/pipelines/chembl/document.yaml@refactoring_001]`| `/document.json` | CSV/Parquet |
| **TestItem** | `testitem` | `[ref: repo:src/bioetl/configs/pipelines/chembl/testitem.yaml@refactoring_001]` | `/molecule.json` | CSV/Parquet |

---

## 2. Pipeline Cards

### ChEMBL Activity Pipeline

-   **Name**: `activity`
-   **CLI Command**: `python -m bioetl.cli.main activity`
-   **Configuration**: `[ref: repo:src/bioetl/configs/pipelines/chembl/activity.yaml@refactoring_001]`
-   **Status**: Production

#### Purpose and Scope
This pipeline extracts detailed activity data from the ChEMBL `/activity.json` endpoint. It retrieves measurements of the biological effect of a molecule on a target. Required fields for a valid output row include `activity_id`.

#### Inputs
-   **`--config`**: Path to the `activity.yaml` config file.
-   **`--output-dir`**: Directory to save the output files.
-   **`--dry-run`**: Validates configuration without running.
-   **`--limit` / `--sample`**: Processes only the first N records.
-   **Profiles**: The configuration extends `base.yaml` and `determinism.yaml`, which are merged before the main config and CLI flags.

#### Extraction
-   **Client**: `ActivityChEMBLClient` (`[ref: repo:src/bioetl/clients/chembl_activity.py@refactoring_001]`) uses the `UnifiedAPIClient` for requests.
-   **Pagination**: The client uses ID batching, sending multiple `activity_id` values in each request.
-   **Parser**: Raw JSON responses are processed by `ActivityParser` (`[ref: repo:src/bioetl/sources/chembl/activity/parser/activity_parser.py@refactoring_001]`).

#### Normalization and Validation
-   **Normalization**: Values are canonicalized by `ActivityNormalizer` and the central normalizer registry.
-   **Validation**: The resulting DataFrame is validated against the `ActivitySchema` (`[ref: repo:src/bioetl/schemas/chembl_activity.py@refactoring_001]`).
-   **Business Key**: `activity_id`

#### Outputs and Determinism
-   **Format**: CSV and/or Parquet.
-   **Sort Keys**: `["assay_id", "testitem_id", "activity_id"]`
-   **`meta.yaml`**: Contains `row_count`, `schema_version`, `hash_algo`, `config_fingerprint`, etc.

#### QC Metrics (Extraction Level)
-   `response_count`, `pages_total`, `duplicate_count` (on `activity_id`), `missing_required_fields`, `retry_events`.

#### Errors and Exit Codes
-   **Exit 0**: Success.
-   **Exit 1**: Failure during any stage (network, parsing, validation, etc.).

#### Examples
```bash
# Minimal run
python -m bioetl.cli.main activity --config src/bioetl/configs/pipelines/chembl/activity.yaml --output-dir ./data/output

# Dry run
python -m bioetl.cli.main activity --config src/bioetl/configs/pipelines/chembl/activity.yaml --dry-run
```

---

### ChEMBL Assay Pipeline

-   **Name**: `assay`
-   **CLI Command**: `python -m bioetl.cli.main assay`
-   **Configuration**: `[ref: repo:src/bioetl/configs/pipelines/chembl/assay.yaml@refactoring_001]`
-   **Status**: Production

#### Purpose and Scope
This pipeline extracts descriptions of experimental assays from the ChEMBL `/assay.json` endpoint. Required fields for a valid output row include `assay_chembl_id`.

#### Inputs
-   **`--config`**: Path to the `assay.yaml` config file.
-   **`--output-dir`**: Directory to save the output files.

#### Extraction
-   **Client**: `AssayChEMBLClient` (`[ref: repo:src/bioetl/clients/chembl_assay.py@refactoring_001]`).
-   **Pagination**: ID Batching.
-   **Parser**: `AssayParser` (`[ref: repo:src/bioetl/sources/chembl/assay/parser/assay_parser.py@refactoring_001]`).

#### Normalization and Validation
-   **Normalization**: `AssayNormalizer`.
-   **Validation**: `AssaySchema` (`[ref: repo:src/bioetl/schemas/chembl_assay.py@refactoring_001]`).
-   **Business Key**: `assay_chembl_id`

#### Outputs and Determinism
-   **Format**: CSV/Parquet.
-   **Sort Keys**: `["assay_id"]` (as per config, though `assay_chembl_id` is the business key).
-   **`meta.yaml`**: Contains `row_count`, `schema_version`, etc.

#### QC Metrics (Extraction Level)
-   `response_count`, `duplicate_count` (on `assay_chembl_id`), `retry_events`.

#### Examples
```bash
# Minimal run
python -m bioetl.cli.main assay --config src/bioetl/configs/pipelines/chembl/assay.yaml --output-dir ./data/output
```

---

### ChEMBL Target Pipeline

-   **Name**: `target`
-   **CLI Command**: `python -m bioetl.cli.main target`
-   **Configuration**: `[ref: repo:src/bioetl/configs/pipelines/chembl/target.yaml@refactoring_001]`
-   **Status**: Production

#### Purpose and Scope
This pipeline extracts information about drug targets from the ChEMBL `/target.json` endpoint. Required fields for a valid output row include `target_chembl_id`.

#### Inputs
-   **`--config`**: Path to the `target.yaml` config file.
-   **`--output-dir`**: Directory to save the output files.

#### Extraction
-   **Client**: Wrapper at `[ref: repo:src/bioetl/sources/chembl/target/client/target_client.py@refactoring_001]`.
-   **Pagination**: ID Batching.
-   **Parser**: `TargetParser` (`[ref: repo:src/bioetl/sources/chembl/target/parser/target_parser.py@refactoring_001]`).

#### Normalization and Validation
-   **Validation**: `TargetSchema` (`[ref: repo:src/bioetl/schemas/chembl_target.py@refactoring_001]`).
-   **Business Key**: `target_chembl_id`

#### Outputs and Determinism
-   **Format**: CSV/Parquet.
-   **Sort Keys**: `["target_id"]`.
-   **`meta.yaml`**: Contains `row_count`, `schema_version`, etc.

#### Examples
```bash
# Minimal run
python -m bioetl.cli.main target --config src/bioetl/configs/pipelines/chembl/target.yaml --output-dir ./data/output
```

---

### ChEMBL Document Pipeline

-   **Name**: `document`
-   **CLI Command**: `python -m bioetl.cli.main document`
-   **Configuration**: `[ref: repo:src/bioetl/configs/pipelines/chembl/document.yaml@refactoring_001]`
-   **Status**: Production

#### Purpose and Scope
This pipeline extracts bibliographic information for publications from the ChEMBL `/document.json` endpoint. Required fields for a valid output row include `document_chembl_id`.

#### Inputs
-   **`--config`**: Path to the `document.yaml` config file.
-   **`--output-dir`**: Directory to save the output files.

#### Extraction
-   **Client**: Wrapper at `[ref: repo:src/bioetl/sources/chembl/document/client/document_client.py@refactoring_001]`.
-   **Pagination**: ID Batching.
-   **Parser**: `DocumentParser` (`[ref: repo:src/bioetl/sources/chembl/document/parser/document_parser.py@refactoring_001]`).

#### Normalization and Validation
-   **Validation**: `DocumentSchema` (`[ref: repo:src/bioetl/schemas/chembl_document.py@refactoring_001]`).
-   **Business Key**: `document_chembl_id`

#### Outputs and Determinism
-   **Format**: CSV/Parquet.
-   **Sort Keys**: `["year", "document_id"]`.
-   **`meta.yaml`**: Contains `row_count`, `schema_version`, etc.

#### Examples
```bash
# Minimal run
python -m bioetl.cli.main document --config src/bioetl/configs/pipelines/chembl/document.yaml --output-dir ./data/output
```

---

### ChEMBL Molecule (TestItem) Pipeline

-   **Name**: `testitem`
-   **CLI Command**: `python -m bioetl.cli.main testitem`
-   **Configuration**: `[ref: repo:src/bioetl/configs/pipelines/chembl/testitem.yaml@refactoring_001]`
-   **Status**: Production

#### Purpose and Scope
This pipeline extracts information about chemical compounds (molecules) from the ChEMBL `/molecule.json` endpoint. Internally, this entity is referred to as `testitem`, and its primary business key is `testitem_id`, which corresponds to the molecule's `molecule_chembl_id`.

#### Inputs
-   **`--config`**: Path to the `testitem.yaml` config file.
-   **`--output-dir`**: Directory to save the output files.

#### Extraction
-   **Client**: Wrapper at `[ref: repo:src/bioetl/sources/chembl/testitem/client/testitem_client.py@refactoring_001]`.
-   **Pagination**: ID Batching.
-   **Parser**: `TestItemParser` (`[ref: repo:src/bioetl/sources/chembl/testitem/parser/testitem_parser.py@refactoring_001]`).

#### Normalization and Validation
-   **Validation**: `TestItemSchema` (`[ref: repo:src/bioetl/schemas/chembl_testitem.py@refactoring_001]`).
-   **Business Key**: `testitem_id`

#### Outputs and Determinism
-   **Format**: CSV/Parquet.
-   **Sort Keys**: `["testitem_id"]`.
-   **`meta.yaml`**: Contains `row_count`, `schema_version`, etc.

#### Examples
```bash
# Minimal run
python -m bioetl.cli.main testitem --config src/bioetl/configs/pipelines/chembl/testitem.yaml --output-dir ./data/output
```
