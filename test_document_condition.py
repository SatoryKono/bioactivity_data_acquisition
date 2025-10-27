#!/usr/bin/env python3
"""Test document extraction condition."""
import pandas as pd
import sys

# Force UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

# Load input data (same as the script does)
df = pd.read_csv('data/input/documents.csv', nrows=5)

print("Columns in input CSV:")
print(df.columns.tolist())

print("\nFirst 3 document_chembl_id values:")
print(df['document_chembl_id'].head(3).tolist())

print("\nChecking condition:")
for idx, row in df.head(3).iterrows():
    doc_id = row.get("document_chembl_id")
    print(f"\nRow {idx}:")
    print(f"  document_chembl_id: {doc_id} (type: {type(doc_id).__name__})")
    print(f"  pd.notna: {pd.notna(doc_id)}")
    if pd.notna(doc_id):
        print(f"  str().strip(): '{str(doc_id).strip()}'")
        print(f"  Condition passes: {pd.notna(doc_id) and str(doc_id).strip()}")
    else:
        print(f"  Condition passes: False (doc_id is NA)")
