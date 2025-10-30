# Schema Compliance Report

**Date:** 2025-10-28
**Status:** ✅ Basic compliance achieved, enhancements needed

## Summary

All 5 pipelines have basic schemas implemented. Current schemas cover core fields but are simplified compared to the full specifications in `IO_SCHEMAS_AND_DIAGRAMS.md`.

## Analysis by Pipeline

### 1. Assay Pipeline

**Status:** ⚠️ Basic schema, missing fields

**Current Schema:** `src/bioetl/schemas/assay.py`

- Fields: `assay_chembl_id`, `assay_type`, `description`, `target_chembl_id`, `confidence_score`


**Expected Schema:** From `IO_SCHEMAS_AND_DIAGRAMS.md` lines 57-124

- Primary key: `[assay_chembl_id, row_subtype, row_index]`

- Missing fields:

  - `row_subtype` (assay, param, variant)
  - `row_index` (for determinism)
  - `pref_name`
  - `hash_row` (SHA256)
  - `hash_business_key` (SHA256)
  - Additional fields: `assay_class_id`, `src_id`, `src_name`, etc.


**Compliance:** 20% - Core structure missing

---

### 2. Activity Pipeline

**Status:** ✅ Good coverage

**Current Schema:** `src/bioetl/schemas/activity.py`

- Fields: Core IDs, activity measures, BAO annotations, molecular properties, target info


**Expected Schema:** From `IO_SCHEMAS_AND_DIAGRAMS.md` lines 202-474

- Primary key: `[activity_id]`

- Required fields present


**Missing Fields:**

- `published_type`, `published_relation`, `published_value`, `published_units`
- `standard_flag`, `lower_bound`, `upper_bound`, `is_censored`
- `activity_comment`
- `hash_row`, `hash_business_key`, `index` (system fields)


**Compliance:** 60% - Core activity data covered

---

### 3. TestItem Pipeline

**Status:** ✅ Good coverage

**Current Schema:** `src/bioetl/schemas/testitem.py`

- Fields: Identifiers, structure, properties, Lipinski, synonyms, classification, PubChem


**Expected Schema:** From `IO_SCHEMAS_AND_DIAGRAMS.md` lines 557-655

- Primary key: `[molecule_chembl_id]`

- Most fields present


**Missing Fields:**

- `pref_name`
- `max_phase`
- `structure_type`, `molecule_type`
- `mw_freebase`, `qed_weighted`
- `standardized_smiles` (vs `canonical_smiles`)
- `hash_row`, `hash_business_key`


**Compliance:** 70% - Good molecular data coverage

---

### 4. Target Pipeline

**Status:** ⚠️ Multiple schemas needed

**Current Schema:** `src/bioetl/schemas/target.py`

- Has 4 schemas: `TargetSchema`, `TargetComponentSchema`, `ProteinClassSchema`, `XrefSchema` ✅

- Basic structure in place


**Expected Schema:** From `IO_SCHEMAS_AND_DIAGRAMS.md` lines 738-898

- 4 output tables required ✅

- Need more fields in each schema


**TargetSchema Missing:**

- `pref_name`, `organism`, `tax_id`
- `uniprot_id_primary`, `uniprot_ids_all`, `gene_symbol`, `hgnc_id`
- `protein_class_pred_L1`, `isoform_count`


**TargetComponentSchema Missing:**

- `component_type`

- `sequence`

- `isoform_variant`


**Compliance:** 40% - Structure correct, fields incomplete

---

### 5. Document Pipeline

**Status:** ⚠️ Missing multi-source fields

**Current Schema:** `src/bioetl/schemas/document.py`

- Uses `ChEMBLDocumentSchema` (external schemas)


**Expected Schema:** From `IO_SCHEMAS_AND_DIAGRAMS.md` lines 957-1314

- Primary key: `[document_chembl_id]`

- Multi-source fields with prefixes (`chembl_`, `pubmed_`, `crossref_`, `openalex_`, `semantic_scholar_`)


**Missing:**

- Separate fields for each source (title, abstract, authors, DOI, PMID, etc.)
- Coverage/validation fields (`valid_doi`, `invalid_doi`, etc.)
- Error fields (`pubmed_error`, `crossref_error`, etc.)
- `hash_row`, `hash_business_key`, `index`


**Compliance:** 30% - Schema exists but lacks multi-source structure

---

## System Fields Compliance

All schemas inherit from `BaseSchema` which includes:

- ✅ `pipeline_version`

- ✅ `source_system`

- ✅ `chembl_release`

- ✅ `extracted_at`


**Missing from all schemas:**

- ❌ `hash_row` (SHA256 of canonicalized row)

- ❌ `hash_business_key` (SHA256 of business key)

- ❌ `index` (deterministic ordering)


---

## Recommendations

### Priority 1: Add System Fields

Add to `BaseSchema`:

```python

hash_row: Series[str] = pa.Field(nullable=False, regex=r'^[0-9a-f]{64}$')
hash_business_key: Series[str] = pa.Field(nullable=False, regex=r'^[0-9a-f]{64}$')
index: Series[int] = pa.Field(nullable=False, ge=0)

```

### Priority 2: Enhance Activity Schema

Add published fields and completeness flags.

### Priority 3: Fix Document Schema

Create multi-source schema with prefixes for all 5 sources.

### Priority 4: Enhance Assay Schema

Add `row_subtype`, `row_index` for explode functionality.

### Priority 5: Complete Target Schemas

Add all missing UniProt, IUPHAR, and classification fields.

---

## Overall Compliance: 45%

**Strengths:**

- All schemas inherit from `BaseSchema` ✅

- Basic data types correct ✅

- Nullable fields handled properly ✅

- TestItem and Activity have good coverage ✅


**Gaps:**

- System fields (hashes, index) missing ⚠️

- Document schema lacks multi-source structure ⚠️

- Target schemas incomplete ⚠️

- Assay needs explode fields ⚠️


**Next Steps:**

1. Add hash fields to output generation
2. Implement full Document schema
3. Complete Target enrichment fields
4. Add exploded fields to Assay
