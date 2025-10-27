#!/usr/bin/env python3
"""Check if ChEMBL data is being fetched at all."""
import pandas as pd

df = pd.read_csv('data/output/documents/documents_20251027.csv', nrows=10)

# Check basic document fields
print("Basic fields:")
print(f"  document_chembl_id: {df['document_chembl_id'].notna().sum()}/{len(df)}")
print(f"  doi: {df['doi'].notna().sum()}/{len(df)}")
print(f"  pubmed_id: {df['pubmed_id'].notna().sum()}/{len(df)}")

# Check for error columns
error_cols = [c for c in df.columns if 'error' in c.lower()]
print(f"\nError columns: {error_cols}")
for col in error_cols:
    non_empty = df[col].notna().sum()
    if non_empty > 0:
        print(f"  {col}: {non_empty}/{len(df)} have errors")
        print(f"    Sample error: {df[col].dropna().iloc[0] if non_empty > 0 else 'N/A'}")

# Check legacy chembl fields
legacy_chembl = [c for c in df.columns if c.startswith('chembl_') and 'CHEMBL.' not in c]
print(f"\nLegacy chembl_* fields ({len(legacy_chembl)}):")
for col in legacy_chembl[:10]:  # first 10
    non_empty = df[col].notna().sum()
    print(f"  {col}: {non_empty}/{len(df)}")

print("\nSample document_chembl_id values:")
print(df['document_chembl_id'].head())

