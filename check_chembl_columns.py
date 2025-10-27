#!/usr/bin/env python3
"""Check CHEMBL.* columns in document output."""
import pandas as pd

df = pd.read_csv('data/output/documents/documents_20251027.csv', nrows=10)
chembl_cols = [c for c in df.columns if 'CHEMBL' in c]

print(f"Found {len(chembl_cols)} CHEMBL columns:")
for col in chembl_cols:
    non_empty = df[col].notna().sum()
    print(f"  {col}: {non_empty}/{len(df)} non-empty")

print("\nFirst row values:")
for col in chembl_cols:
    val = df[col].iloc[0]
    print(f"  {col}: {val}")

