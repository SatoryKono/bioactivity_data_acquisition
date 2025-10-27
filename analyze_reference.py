"""Analyze reference target output."""
import pandas as pd

ref_path = 'e:/github/ChEMBL_data_acquisition6/config/dictionary/_target/output.targets.csv'
ref_df = pd.read_csv(ref_path, nrows=10)  # Read only first 10 rows for comparison

print(f'Reference: {len(ref_df)} rows (sample), {len(ref_df.columns)} columns')
print(f'\nReference columns:')
for i, col in enumerate(ref_df.columns, 1):
    print(f'{i:3d}. {col}')

# Check problem columns
problem_cols = [
    'protein_classifications',
    'CHEMBL.PROTEIN_CLASSIFICATION.pref_name',
    'protein_name_alt',
    'protein_synonym_list',
    'taxon_id',
    'gene_symbol',
    'iuphar_target_id',
    'iuphar_family_id',
    'gtop_target_id',
    'protein_class_pred_L1',
    'reaction_ec_numbers',
]

print(f'\n=== PROBLEM COLUMNS STATUS IN REFERENCE ===')
for col in problem_cols:
    if col in ref_df.columns:
        non_empty = ref_df[col].notna() & (ref_df[col].astype(str) != '')
        count = non_empty.sum()
        print(f'{col:50s}: {count}/{len(ref_df)} filled')
        if count > 0:
            print(f'  Sample value: {ref_df[col][non_empty].iloc[0] if count > 0 else "N/A"}')
    else:
        print(f'{col:50s}: NOT FOUND')

