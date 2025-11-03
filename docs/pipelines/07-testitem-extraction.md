# ChEMBL TestItem Extraction Pipeline

This document specifies the `testitem` pipeline, which extracts molecule data from the ChEMBL API and optionally enriches it with data from PubChem.

## 1. Overview

The `testitem` pipeline is responsible for fetching detailed information about chemical compounds (molecules) from the ChEMBL database. It flattens nested structures from the ChEMBL API response to create a comprehensive, flat record for each molecule.

-   **Primary Source**: ChEMBL API `/molecule.json` endpoint.
-   **Optional Enrichment**: PubChem PUG REST API for additional properties and identifiers.

## 2. CLI Command

The pipeline is executed via the `testitem` CLI command.

**Usage:**
```bash
python -m bioetl.cli.main testitem [OPTIONS]
```

**Example:**
```bash
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/testitem.yaml \
  --output-dir data/output/testitem
```

## 3. ChEMBL Data Extraction

### 3.1. Batch Extraction

The pipeline extracts data in batches to comply with the ChEMBL API's URL length limitations.
-   **Endpoint**: `/molecule.json?molecule_chembl_id__in={ids}`
-   **Batch Size**: Configurable via `sources.chembl.batch_size`, typically `25`.

### 3.2. Field Extraction and Flattening

The pipeline extracts over 80 fields and flattens several nested JSON structures from the ChEMBL response:
-   **`molecule_hierarchy`**: Flattened to `parent_chembl_id` and `parent_molregno`.
-   **`molecule_properties`**: Flattened into ~22 distinct physicochemical properties (e.g., `mw_freebase`, `alogp`, `hba`).
-   **`molecule_structures`**: Flattened to `canonical_smiles`, `standard_inchi`, and `standard_inchi_key`.
-   **`molecule_synonyms`**: Aggregated into an `all_names` field (for search) and also preserved as a JSON string.
-   Other nested objects like `atc_classifications` and `cross_references` are stored as canonical JSON strings.

## 4. PubChem Enrichment (Optional)

The pipeline can optionally enrich the ChEMBL data with information from PubChem. This feature is controlled by the `sources.pubchem.enabled` flag in the configuration.

### 4.1. CID Resolution

The core of the enrichment process is resolving a PubChem Compound ID (CID) for each molecule. A cascaded strategy is used, prioritizing the most reliable methods first:
1.  **Cache Lookup**: Check a persistent local cache for a known CID.
2.  **Direct CID**: Use a CID if already present in ChEMBL cross-references.
3.  **InChIKey Lookup**: Use the molecule's `standard_inchi_key`. This is the most reliable method.
4.  **SMILES Lookup**: Use the `canonical_smiles` as a fallback.
5.  **Name Lookup**: Use the `pref_name` as a last resort.

### 4.2. Batch Property Fetching

Once CIDs are resolved, the pipeline fetches properties from PubChem in batches (typically 100 CIDs per request) for efficiency.
-   **Endpoint**: `/compound/cid/{cids}/property/{properties}/JSON`
-   **Properties**: `MolecularFormula`, `MolecularWeight`, `CanonicalSMILES`, `InChIKey`, etc.

### 4.3. Caching and Resilience

-   **Multi-Level Caching**: A combination of an in-memory TTL cache and a persistent file-based cache is used to minimize redundant API calls.
-   **Graceful Degradation**: The entire PubChem enrichment process is designed to be optional. Any failure in fetching data from PubChem will be logged, but it will **not** stop the main ChEMBL pipeline from completing.

## 5. Component Architecture

| Component | Implementation |
|---|---|
| **Client** | `[ref: repo:src/bioetl/sources/chembl/testitem/client/testitem_client.py@refactoring_001]` |
| **Parser** | `[ref: repo:src/bioetl/sources/chembl/testitem/parser/testitem_parser.py@refactoring_001]` |
| **Normalizer** | `[ref: repo:src/bioetl/sources/chembl/testitem/normalizer/testitem_normalizer.py@refactoring_001]` |
| **Schema** | `[ref: repo:src/bioetl/schemas/chembl_testitem.py@refactoring_001]` |

## 6. Key Identifiers

-   **Business Key**: `molecule_chembl_id`
-   **Sort Key**: `molecule_chembl_id`
