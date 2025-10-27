# Testitem Script Analysis Report

## Execution Summary

**Command Executed:**
```bash
python src\scripts\get_testitem_data.py --input data\input\testitem.csv --config configs\config_testitem.yaml --limit 100
```

**Status:** ❌ FAILED - Validation errors prevented completion

## Processing Results

### Input Data
- **Input file:** `data\input\testitem.csv`
- **Records processed:** 5 molecules
- **Limit applied:** 100 (but only 5 records available)

### Data Extraction
- **ChEMBL extraction:** ✅ SUCCESS - 3/5 records enriched (60.0%)
- **PubChem extraction:** ✅ SUCCESS - 3/5 records enriched (60.0%)
- **Data normalization:** ✅ SUCCESS - 5 records normalized

### Validation Results
- **Schema validation:** ❌ FAILED
- **Output files created:** ❌ NONE (validation failure prevented export)

## Critical Issues Found

### 1. Data Type Validation Errors

#### Error 1: `first_approval` Column
- **Expected:** String type
- **Actual:** Integer (1950)
- **Issue:** Date field contains integer year instead of ISO 8601 string format

#### Error 2: `nstereo` Column  
- **Expected:** int64 type
- **Actual:** object type
- **Issue:** Numeric field stored as object, likely contains mixed types or NaN values

### 2. Logging Format Errors
Multiple logging errors throughout execution:
- **Error Type:** `TypeError: not all arguments converted during string formatting`
- **Affected modules:** 
  - `library.testitem.validate`
  - `library.testitem.normalize`
- **Impact:** Non-critical but indicates code quality issues

## Column Analysis

### Configuration vs File Column Comparison

**Expected columns from config (107 total):**
- molecule_chembl_id, molregno, pref_name, pref_name_key, parent_chembl_id, parent_molregno, max_phase, therapeutic_flag, dosed_ingredient, first_approval, structure_type, molecule_type, mw_freebase, alogp, hba, hbd, psa, rtb, ro3_pass, num_ro5_violations, acd_most_apka, acd_most_bpka, acd_logp, acd_logd, molecular_species, full_mwt, aromatic_rings, heavy_atoms, qed_weighted, mw_monoisotopic, full_molformula, hba_lipinski, hbd_lipinski, num_lipinski_ro5_violations, oral, parenteral, topical, black_box_warning, natural_product, first_in_class, chirality, prodrug, inorganic_flag, polymer_flag, usan_year, availability_type, usan_stem, usan_substem, usan_stem_definition, indication_class, withdrawn_flag, withdrawn_year, withdrawn_country, withdrawn_reason, mechanism_of_action, direct_interaction, molecular_mechanism, drug_chembl_id, drug_name, drug_type, drug_substance_flag, drug_indication_flag, drug_antibacterial_flag, drug_antiviral_flag, drug_antifungal_flag, drug_antiparasitic_flag, drug_antineoplastic_flag, drug_immunosuppressant_flag, drug_antiinflammatory_flag, pubchem_cid, pubchem_molecular_formula, pubchem_molecular_weight, pubchem_canonical_smiles, pubchem_isomeric_smiles, pubchem_inchi, pubchem_inchi_key, pubchem_registry_id, pubchem_rn, standardized_inchi, standardized_inchi_key, standardized_smiles, atc_classifications, biotherapeutic, chemical_probe, cross_references, helm_notation, molecule_hierarchy, molecule_properties, molecule_structures, molecule_synonyms, orphan, veterinary, standard_inchi, chirality_chembl, molecule_type_chembl, nstereo, salt_chembl_id, index, pipeline_version, source_system, chembl_release, extracted_at, hash_row, hash_business_key

**Note:** Cannot analyze actual file columns due to input file access restrictions and validation failure preventing data export.

## Recommendations

### Immediate Fixes Required

1. **Fix Data Type Issues:**
   - Convert `first_approval` from integer to ISO 8601 string format
   - Ensure `nstereo` is properly converted to int64 type
   - Add proper data type conversion in normalization step

2. **Fix Logging Issues:**
   - Update logging format strings to use proper formatting
   - Replace `%d` with `{}` format or fix argument passing

3. **Schema Validation:**
   - Update Pandera schema to handle data type conversions
   - Add proper type coercion before validation

### Data Quality Issues

- **Missing data:** 40% of records failed ChEMBL/PubChem enrichment
- **Data type mismatches:** Critical validation failures
- **Schema compliance:** Data doesn't match expected schema

## Next Steps

1. Fix the data type conversion issues in the normalization module
2. Update the validation schema to handle edge cases
3. Fix logging format errors
4. Re-run the script after fixes
5. Perform column analysis on successful output

## Files Status

- **Input file:** `data\input\testitem.csv` - ✅ Exists (5 records)
- **Output files:** None created due to validation failure
- **Log files:** Not created due to early failure
- **QC reports:** Not generated due to validation failure

---
*Report generated: 2025-10-26 15:11:09*
*Script execution time: ~10 seconds*
*Status: FAILED - Requires fixes before successful execution*
