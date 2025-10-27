"""Compare current target output with reference."""
import pandas as pd
import sys

# Read current output
current_df = pd.read_csv('data/output/targets/targets_20251027.csv')
print(f'Current output: {len(current_df)} rows, {len(current_df.columns)} columns')

# Try to find reference
ref_paths = [
    'e:/github/ChEMBL_data_acquisition6/output/targets.csv',
    'e:/github/ChEMBL_data_acquisition6/data/targets.csv',
]

ref_df = None
for path in ref_paths:
    try:
        ref_df = pd.read_csv(path)
        print(f'\nReference found: {path}')
        print(f'Reference: {len(ref_df)} rows, {len(ref_df.columns)} columns')
        break
    except:
        continue

if ref_df is None:
    print('\nERROR: Could not find reference file')
    print('Please provide correct path to reference targets.csv')
    sys.exit(1)

# Compare columns
current_cols = set(current_df.columns)
ref_cols = set(ref_df.columns)

print(f'\n=== COLUMN COMPARISON ===')
print(f'Columns only in current: {len(current_cols - ref_cols)}')
for col in sorted(current_cols - ref_cols)[:10]:
    print(f'  + {col}')

print(f'\nColumns only in reference: {len(ref_cols - current_cols)}')
for col in sorted(ref_cols - current_cols)[:10]:
    print(f'  - {col}')

# Check specific columns in reference
problem_cols = [
    'protein_classifications',
    'protein_name_alt',
    'protein_synonym_list',
    'taxon_id',
    'gene_symbol',
    'iuphar_target_id',
]

print(f'\n=== PROBLEM COLUMNS IN REFERENCE ===')
for col in problem_cols:
    if col in ref_df.columns:
        empty_pct = (ref_df[col].isna() | (ref_df[col].astype(str) == '')).sum() / len(ref_df) * 100
        print(f'{col}: {empty_pct:.1f}% empty in reference')

