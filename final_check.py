# -*- coding: utf-8 -*-
import pandas as pd

df = pd.read_csv('data/output/documents_check/documents_20251028.csv')

print('=== FINAL CHECK ===')
print(f'Total rows: {len(df)}')
print(f'Total columns: {len(df.columns)}')

pubmed_cols = [c for c in df.columns if c.startswith('pubmed_')]
print(f'PubMed columns: {len(pubmed_cols)}')

print('\nFill rate for ALL PubMed fields:')
for col in sorted(pubmed_cols):
    filled = df[col].notna().sum()
    pct = (filled/len(df)*100)
    status = 'OK' if filled > 0 else 'EMPTY'
    print(f'{status:5s} {col:35s}: {filled:2d}/{len(df)} ({pct:5.1f}%)')

if len(df) > 0:
    print('\nSample values from first row:')
    sample = df.iloc[0]
    for col in sorted(pubmed_cols)[:15]:
        val = sample[col]
        if pd.notna(val) and val != '':
            val_str = str(val)[:60] + '...' if len(str(val)) > 60 else str(val)
            print(f'{col}: {val_str}')

