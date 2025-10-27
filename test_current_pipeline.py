"""Test current pipeline to see what's actually happening."""
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')

# Read current output
df = pd.read_csv('data/output/targets/targets_20251027.csv')
print("=== CURRENT OUTPUT ANALYSIS ===")
print(f"Total rows: {len(df)}")
print(f"Total columns: {len(df.columns)}")

# Check what UniProt IDs are available
sample = df.iloc[0]
print("\n=== UNIPROT ID COLUMNS (sample row) ===")
uniprot_cols = ['mapping_uniprot_id', 'uniprot_id_primary', 'CHEMBL.TARGET_COMPONENTS.accession']
for col in uniprot_cols:
    if col in df.columns:
        val = sample[col]
        print(f"  {col}: '{val}' (type: {type(val).__name__})")
    else:
        print(f"  {col}: NOT FOUND")

# Check if UniProt enrichment fields exist
print("\n=== UNIPROT ENRICHMENT FIELDS ===")
uniprot_fields = ['taxon_id', 'gene_symbol', 'protein_name_alt', 'sequence_length']
for col in uniprot_fields:
    if col in df.columns:
        empty_count = (df[col].isna() | (df[col].astype(str) == '')).sum()
        print(f"  {col}: {empty_count}/{len(df)} empty")
    else:
        print(f"  {col}: NOT FOUND")

# Check IUPHAR enrichment
print("\n=== IUPHAR ENRICHMENT FIELDS ===")
iuphar_fields = ['iuphar_target_id', 'iuphar_family_id', 'iuphar_type']
for col in iuphar_fields:
    if col in df.columns:
        empty_count = (df[col].isna() | (df[col].astype(str) == '')).sum()
        print(f"  {col}: {empty_count}/{len(df)} empty")
    else:
        print(f"  {col}: NOT FOUND")

# Check protein classification
print("\n=== PROTEIN CLASSIFICATION FIELDS ===")
class_fields = ['protein_class_pred_L1', 'protein_class_pred_L2', 'protein_class_pred_L3']
for col in class_fields:
    if col in df.columns:
        empty_count = (df[col].isna() | (df[col].astype(str) == '')).sum()
        print(f"  {col}: {empty_count}/{len(df)} empty")
    else:
        print(f"  {col}: NOT FOUND")

# Check ChEMBL protein classification
print("\n=== CHEMBL PROTEIN_CLASSIFICATION ===")
if 'CHEMBL.PROTEIN_CLASSIFICATION.pref_name' in df.columns:
    empty_count = (df['CHEMBL.PROTEIN_CLASSIFICATION.pref_name'].isna() | 
                   (df['CHEMBL.PROTEIN_CLASSIFICATION.pref_name'].astype(str) == '')).sum()
    print(f"  Empty: {empty_count}/{len(df)}")
    print(f"  Sample value: {sample['CHEMBL.PROTEIN_CLASSIFICATION.pref_name']}")
else:
    print("  NOT FOUND")

# Look at logs to understand what happened
print("\n=== KEY QUESTION ===")
print("Did UniProt enrichment actually run?")
print("Check logs for: 'Enriching with UniProt data...'")

