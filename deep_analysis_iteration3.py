"""Deep analysis iteration 3 - Compare data flow."""
import pandas as pd
import json

# Read current output
current_df = pd.read_csv('data/output/targets/targets_20251027.csv')
print("=== ITERATION 3: DEEP DATA FLOW ANALYSIS ===\n")
print(f'Current: {len(current_df)} rows, {len(current_df.columns)} columns\n')

# Analyze one specific record
if len(current_df) > 0:
    sample = current_df.iloc[0]
    print("Sample record (first row):")
    print(f"  target_chembl_id: {sample['CHEMBL.TARGETS.target_chembl_id']}")
    print(f"  pref_name: {sample['CHEMBL.TARGETS.pref_name']}")
    
    # Check ChEMBL fields
    print(f"\n--- ChEMBL fields ---")
    print(f"  CHEMBL.PROTEIN_CLASSIFICATION.pref_name: '{sample['CHEMBL.PROTEIN_CLASSIFICATION.pref_name']}'")
    print(f"  reaction_ec_numbers: '{sample['reaction_ec_numbers']}'")
    print(f"  CHEMBL.TARGET_COMPONENTS.xref_id: '{sample['CHEMBL.TARGET_COMPONENTS.xref_id']}'")
    
    # Check UniProt fields
    print(f"\n--- UniProt fields ---")
    print(f"  uniprot_id_primary: '{sample['uniprot_id_primary']}'")
    print(f"  mapping_uniprot_id: '{sample['mapping_uniprot_id']}'")
    print(f"  taxon_id: '{sample['taxon_id']}'")
    print(f"  gene_symbol: '{sample['gene_symbol']}'")
    print(f"  protein_name_alt: '{sample['protein_name_alt']}'")
    print(f"  sequence_length: '{sample['sequence_length']}'")
    
    # Check IUPHAR fields
    print(f"\n--- IUPHAR fields ---")
    print(f"  iuphar_target_id: '{sample['iuphar_target_id']}'")
    print(f"  iuphar_family_id: '{sample['iuphar_family_id']}'")
    print(f"  iuphar_type: '{sample['iuphar_type']}'")
    
    # Check protein classification
    print(f"\n--- Protein classification ---")
    print(f"  protein_class_pred_L1: '{sample['protein_class_pred_L1']}'")
    print(f"  protein_class_pred_L2: '{sample['protein_class_pred_L2']}'")
    print(f"  protein_class_pred_L3: '{sample['protein_class_pred_L3']}'")

# Now check reference
print("\n\n=== REFERENCE FILE ANALYSIS ===\n")
ref_df = pd.read_csv('e:/github/ChEMBL_data_acquisition6/config/dictionary/_target/output.targets.csv', nrows=10)
print(f'Reference: {len(ref_df)} rows, {len(ref_df.columns)} columns\n')

if len(ref_df) > 0:
    ref_sample = ref_df.iloc[0]
    print("Reference sample record (first row):")
    print(f"  target_chembl_id: {ref_sample['target_chembl_id']}")
    print(f"  pref_name: {ref_sample.get('pref_name', 'N/A')}")
    
    print(f"\n--- ChEMBL fields ---")
    print(f"  protein_classifications: '{ref_sample.get('protein_classifications', 'N/A')}'")
    print(f"  reaction_ec_numbers: '{ref_sample.get('reaction_ec_numbers', 'N/A')}'")
    print(f"  cross_references: '{ref_sample.get('cross_references', 'N/A')[:100] if ref_sample.get('cross_references') else 'N/A'}'...")
    
    print(f"\n--- UniProt fields ---")
    print(f"  uniprot_id_primary: '{ref_sample.get('uniprot_id_primary', 'N/A')}'")
    print(f"  taxon_id: '{ref_sample.get('taxon_id', 'N/A')}'")
    print(f"  gene_symbol: '{ref_sample.get('gene_symbol', 'N/A')}'")
    print(f"  protein_name_alt: '{ref_sample.get('protein_name_alt', 'N/A')[:80] if ref_sample.get('protein_name_alt') else 'N/A'}'...")
    print(f"  sequence_length: '{ref_sample.get('sequence_length', 'N/A')}'")
    
    print(f"\n--- IUPHAR fields ---")
    print(f"  iuphar_target_id: '{ref_sample.get('iuphar_target_id', 'N/A')}'")
    print(f"  iuphar_family_id: '{ref_sample.get('iuphar_family_id', 'N/A')}'")
    print(f"  iuphar_type: '{ref_sample.get('iuphar_type', 'N/A')}'")
    
    print(f"\n--- Protein classification ---")
    print(f"  protein_class_pred_L1: '{ref_sample.get('protein_class_pred_L1', 'N/A')}'")
    print(f"  protein_class_pred_L2: '{ref_sample.get('protein_class_pred_L2', 'N/A')}'")
    print(f"  protein_class_pred_L3: '{ref_sample.get('protein_class_pred_L3', 'N/A')}'")

print("\n\n=== KEY FINDINGS ===")
print("Empty in current but filled in reference:")
empty_cols = []
for col in ['protein_name_alt', 'taxon_id', 'gene_symbol', 'iuphar_target_id', 'protein_class_pred_L1']:
    curr_val = sample.get(col, '') if col in sample.index else ''
    ref_val = ref_sample.get(col, '') if col in ref_df.columns else ''
    if (pd.isna(curr_val) or str(curr_val) == '') and (not pd.isna(ref_val) and str(ref_val) != ''):
        empty_cols.append(col)
        print(f"  - {col}: current='{curr_val}', reference='{ref_val}'")

print(f"\nTotal problem columns: {len(empty_cols)}")

