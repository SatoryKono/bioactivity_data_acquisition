# Activity ChEMBL: Transformation

This document details the `transform` stage of the ChEMBL Activity pipeline. The `ChemblActivityPipeline.transform()` method is responsible for cleaning, normalizing, and structuring the raw data fetched from the ChEMBL API into a format suitable for validation and writing.

## 1. Overview

The transformation process is a multi-step pipeline that takes the raw `pandas.DataFrame` from the `extract` stage and applies a series of normalization functions. Each function targets a specific set of columns or data types to ensure consistency, correctness, and adherence to the target schema.

## 2. Transformation Steps

The `transform` method executes the following normalization routines in sequence:

### 2.1. Identifier Normalization (`_normalize_identifiers`)

This step ensures that all ChEMBL and BAO (BioAssay Ontology) identifiers are in a canonical format.
- **Fields**: `molecule_chembl_id`, `assay_chembl_id`, `target_chembl_id`, `document_chembl_id`, `bao_endpoint`, `bao_format`.
- **Actions**:
    - Converts all identifiers to uppercase.
    - Trims leading/trailing whitespace.
    - Validates identifiers against their expected regular expressions (`^CHEMBL\d+$` for ChEMBL, `^BAO_\d{7}$` for BAO).
    - Invalid identifiers are logged and set to `None`.

### 2.2. Measurement Normalization (`_normalize_measurements`)

This is a critical step that cleans and standardizes the core activity measurement fields.
- **`standard_value`**:
    - Converts the field to a string to handle non-numeric inputs.
    - Removes common non-numeric characters (e.g., commas, spaces).
    - Extracts the first valid numeric value from ranges (e.g., "10-20" becomes 10).
    - Converts the cleaned string to a numeric type. Any values that cannot be converted become `NaN`.
    - Any negative values are considered invalid and are set to `None`.
- **`standard_relation`**:
    - Maps common Unicode inequality symbols (e.g., `≤`, `≥`) to their ASCII equivalents (`<=`, `>=`).
    - Validates the relation against a known set of allowed values (`=`, `<`, `>`, `<=`, `>=`, `~`). Invalid relations are set to `None`.
- **`standard_type`**:
    - Validates the measurement type against a known set of standard types (e.g., `IC50`, `Ki`). Invalid types are set to `None`.
- **`standard_units`**:
    - Normalizes various unit representations to a canonical form (e.g., "micromolar", "uM", "µM" all become "μM").

### 2.3. String Field Normalization (`_normalize_string_fields`)

This function cleans and standardizes free-text string fields.
- **Fields**: `canonical_smiles`, `bao_label`, `target_organism`, `data_validity_comment`.
- **Actions**:
    - Trims leading/trailing whitespace.
    - Converts empty strings (`""`) to `None`.
    - Applies title-casing to `target_organism`.
    - Truncates `bao_label` to a maximum length of 128 characters.

### 2.4. Nested Structure Serialization (`_normalize_nested_structures`)

The ChEMBL API can return nested JSON objects or arrays within certain fields. This step ensures they are stored as canonical JSON strings.
- **Fields**: `ligand_efficiency`, `activity_properties`.
- **Actions**:
    - If the field contains a Python `dict` or `list`, it is serialized into a sorted-key JSON string.
    - If serialization fails, the value is logged and set to `None`.

### 2.5. Data Type Conversion (`_normalize_data_types`)

This is the final normalization step, ensuring that all columns conform to the data types expected by the Pandera schema.
- **Numeric Fields**: Converts columns like `activity_id` and `target_tax_id` to `Int64`, and `standard_value` to `float64`.
- **Boolean Fields**: Converts boolean-like fields (e.g., `is_citation`) to a proper boolean type.

### 2.6. Foreign Key Validation (`_validate_foreign_keys`)

As a final check within the transform stage, this function validates the format of all ChEMBL ID fields to ensure they match the `^CHEMBL\d+$` pattern before the data is passed to the more stringent `validate` stage.

## 3. Output

The `transform` stage returns a cleaned and normalized `pandas.DataFrame`. This DataFrame has the same records as the input, but with standardized values and corrected data types, ready for the final validation against the `ActivitySchema`.
