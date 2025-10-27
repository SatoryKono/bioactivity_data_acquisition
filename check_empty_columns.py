"""Temporary script to check empty columns in target output."""
import pandas as pd

# Read the output file
df = pd.read_csv('data/output/targets/targets_20251027.csv', nrows=10)

# List of columns to check
columns_to_check = [
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
]

print(f"Total rows analyzed: {len(df)}")
print(f"Total columns: {len(df.columns)}")
print("\n" + "="*80)

for col in columns_to_check:
    if col in df.columns:
        empty_count = df[col].isna().sum() + (df[col] == '').sum()
        print(f"{col:50s} | Empty: {empty_count}/{len(df)}")
        if empty_count < len(df):
            # Show first non-empty value
            non_empty = df[df[col].notna() & (df[col] != '')][col].head(1)
            if not non_empty.empty:
                val = str(non_empty.iloc[0])
                print(f"  Sample: {val[:100]}...")
    else:
        print(f"{col:50s} | COLUMN NOT FOUND")
    print("-"*80)

