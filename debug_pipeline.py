#!/usr/bin/env python3
"""Debug script to trace the activity pipeline step by step."""

import pandas as pd
import sys
import os
sys.path.append('src')

from library.activity.pipeline import ActivityPipeline
from library.activity.config import load_activity_config
from library.clients.chembl import ChEMBLClient
from library.config import APIClientConfig

def debug_pipeline():
    """Debug the activity pipeline step by step."""
    
    # Load config
    config = load_activity_config("configs/config_activity.yaml")
    config.runtime.limit = 2  # Only process 2 records
    
    # Load input data
    input_data = pd.read_csv("data/input/activity.csv")
    print(f"Loaded {len(input_data)} input records")
    print("Input data columns:", list(input_data.columns))
    print("First few rows:")
    print(input_data.head())
    
    # Create pipeline
    pipeline = ActivityPipeline(config)
    
    # Step 1: Extract
    print("\n=== EXTRACTION ===")
    extracted_data = pipeline.extract(input_data)
    print(f"Extracted {len(extracted_data)} records")
    print("Extracted data columns:", list(extracted_data.columns))
    
    if len(extracted_data) > 0:
        print("First extracted record:")
        for col in ['activity_id', 'standard_type', 'standard_value', 'standard_units']:
            if col in extracted_data.columns:
                print(f"  {col}: {extracted_data.iloc[0][col]}")
    
    # Step 2: Normalize
    print("\n=== NORMALIZATION ===")
    normalized_data = pipeline.normalize(extracted_data)
    print(f"Normalized {len(normalized_data)} records")
    print("Normalized data columns:", list(normalized_data.columns))
    
    if len(normalized_data) > 0:
        print("First normalized record:")
        for col in ['activity_id', 'standard_type', 'standard_value', 'standard_units']:
            if col in normalized_data.columns:
                print(f"  {col}: {normalized_data.iloc[0][col]}")
    
    # Step 3: Quality filter
    print("\n=== QUALITY FILTER ===")
    accepted_data, rejected_data = pipeline.quality_filter.apply_moderate_quality_filter(normalized_data)
    print(f"Quality filter: {len(accepted_data)} accepted, {len(rejected_data)} rejected")
    
    if len(rejected_data) > 0:
        print("Rejection reasons:")
        if 'rejection_reason' in rejected_data.columns:
            print(rejected_data['rejection_reason'].value_counts())
        else:
            print("No rejection_reason column found")
            
        print("First rejected record:")
        for col in ['activity_id', 'standard_type', 'standard_value', 'standard_units']:
            if col in rejected_data.columns:
                print(f"  {col}: {rejected_data.iloc[0][col]}")

if __name__ == "__main__":
    debug_pipeline()
