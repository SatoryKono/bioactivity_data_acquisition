"""Analyze empty columns in target output."""
import pandas as pd

df = pd.read_csv('data/output/targets/targets_20251027.csv')
print(f'Total rows: {len(df)}')
print(f'Total columns: {len(df.columns)}')
print('\nEmpty columns analysis:')

empty_cols = []
for col in df.columns:
    if df[col].isna().all() or (df[col].astype(str) == '').all():
        empty_cols.append(col)

print(f'\nCompletely empty columns: {len(empty_cols)}')
for i, col in enumerate(empty_cols[:50], 1):
    print(f'{i:3d}. {col}')

# Check specific columns mentioned by user
problem_cols = [
    'CHEMBL.PROTEIN_CLASSIFICATION.pref_name',
    'protein_name_alt',
    'protein_synonym_list',
    'taxon_id',
    'lineage_superkingdom',
    'lineage_phylum',
    'sequence_length',
    'xref_uniprot',
    'reaction_ec_numbers_uniprot',
    'uniprot_version',
    'gene_symbol',
    'gene_symbol_list',
    'iuphar_target_id',
    'iuphar_family_id',
    'gtop_target_id',
    'protein_class_pred_L1',
    'protein_class_pred_L2',
    'protein_class_pred_L3',
]

print('\n\nUser-specified problem columns:')
for col in problem_cols:
    if col in df.columns:
        empty_pct = (df[col].isna() | (df[col].astype(str) == '')).sum() / len(df) * 100
        print(f'  {col}: {empty_pct:.1f}% empty')
    else:
        print(f'  {col}: NOT FOUND in output')

