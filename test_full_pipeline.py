#!/usr/bin/env python3
"""Test full target pipeline with fixes."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from library.target.pipeline import run_target_etl
from library.target.config import load_target_config

def main():
    """Run full target pipeline test."""
    print("Loading config...")
    config = load_target_config("configs/config_target.yaml")
    
    print("Running target ETL pipeline...")
    try:
        result = run_target_etl(config, input_path=Path("data/input/target.csv"))
        print(f"Pipeline completed successfully!")
        print(f"Output shape: {result.shape}")
        
        # Check specific columns
        columns_to_check = [
            "CHEMBL.PROTEIN_CLASSIFICATION.pref_name",
            "protein_name_alt", 
            "protein_synonym_list",
            "taxon_id",
            "lineage_superkingdom",
            "gene_symbol",
            "protein_class_pred_L1",
            "protein_class_pred_L2",
            "protein_class_pred_L3"
        ]
        
        print("\nChecking fixed columns:")
        for col in columns_to_check:
            if col in result.columns:
                non_empty = result[col].notna().sum() + (result[col] != '').sum()
                print(f"  {col}: {non_empty}/{len(result)} non-empty")
            else:
                print(f"  {col}: COLUMN NOT FOUND")
                
    except Exception as e:
        print(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
