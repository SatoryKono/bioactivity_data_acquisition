#!/usr/bin/env python3
"""
Script to analyze document CSV columns and compare with config
"""

import pandas as pd
import yaml
import json
from pathlib import Path

def analyze_document_columns():
    # Read the CSV file
    csv_path = 'data/output/documents/documents_20251026.csv'
    df = pd.read_csv(csv_path)
    
    print('=== DOCUMENT COLUMN ANALYSIS ===')
    print(f'File: {csv_path}')
    print(f'Rows: {len(df)}')
    print(f'Total columns: {len(df.columns)}')
    print()
    
    # Count empty and filled columns
    empty_cols = []
    filled_cols = []
    
    for col in df.columns:
        # Check if column is completely empty (all NaN or empty strings)
        if df[col].isna().all() or (df[col].astype(str).str.strip() == '').all():
            empty_cols.append(col)
        else:
            filled_cols.append(col)
    
    print(f'Empty columns: {len(empty_cols)}')
    print(f'Filled columns: {len(filled_cols)}')
    print()
    
    # Read config to get expected columns
    config_path = 'configs/config_document.yaml'
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Get expected columns from config
    expected_cols = []
    if 'output' in config and 'column_order' in config['output']:
        expected_cols = config['output']['column_order']
    
    print(f'Expected columns from config: {len(expected_cols)}')
    
    # Find columns in file but not in config
    in_file_not_config = [col for col in df.columns if col not in expected_cols]
    print(f'Columns in file but not in config: {len(in_file_not_config)}')
    
    # Find columns in config but not in file
    in_config_not_file = [col for col in expected_cols if col not in df.columns]
    print(f'Columns in config but not in file: {len(in_config_not_file)}')
    print()
    
    # Detailed lists
    print('=== EMPTY COLUMNS ===')
    for i, col in enumerate(empty_cols, 1):
        print(f'{i:3d}. {col}')
    print()
    
    print('=== FILLED COLUMNS ===')
    for i, col in enumerate(filled_cols, 1):
        print(f'{i:3d}. {col}')
    print()
    
    print('=== COLUMNS IN FILE BUT NOT IN CONFIG ===')
    for i, col in enumerate(in_file_not_config, 1):
        print(f'{i:3d}. {col}')
    print()
    
    print('=== COLUMNS IN CONFIG BUT NOT IN FILE ===')
    for i, col in enumerate(in_config_not_file, 1):
        print(f'{i:3d}. {col}')
    print()
    
    # Save analysis to file
    analysis = {
        'file_info': {
            'path': csv_path,
            'rows': len(df),
            'total_columns': len(df.columns),
            'empty_columns_count': len(empty_cols),
            'filled_columns_count': len(filled_cols)
        },
        'config_info': {
            'path': config_path,
            'expected_columns_count': len(expected_cols)
        },
        'comparison': {
            'in_file_not_config_count': len(in_file_not_config),
            'in_config_not_file_count': len(in_config_not_file)
        },
        'empty_columns': empty_cols,
        'filled_columns': filled_cols,
        'in_file_not_config': in_file_not_config,
        'in_config_not_file': in_config_not_file
    }
    
    # Save to JSON file
    output_file = 'document_column_analysis.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)
    
    print(f'Analysis saved to: {output_file}')
    
    return analysis

if __name__ == '__main__':
    analyze_document_columns()
