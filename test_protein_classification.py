#!/usr/bin/env python3
"""Test protein classification with real data."""

import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from library.target.protein_classification import append_protein_class_predictions

def main():
    """Test protein classification."""
    print("Testing protein classification...")
    
    # Create test data with fields from our fixes
    test_data = pd.DataFrame({
        'CHEMBL.TARGETS.target_chembl_id': ['CHEMBL2343'],
        'mapping_uniprot_id': ['O00141'],
        'uniprot_id_primary': ['O00141'],
        'iuphar_target_id': ['1534'],
        'iuphar_family_id': ['616'],
        'iuphar_name': ['serum/glucocorticoid regulated kinase 1'],
        'CHEMBL.PROTEIN_CLASSIFICATION.pref_name': ['["Enzyme.Kinase"]'],
        'molecular_function': ['protein kinase activity'],
        'reaction_ec_numbers': ['2.7.11.1']
    })
    
    print(f"Input data: {test_data.shape}")
    print("Input columns:", list(test_data.columns))
    
    try:
        # Test classification
        result = append_protein_class_predictions(test_data)
        
        print(f"\nOutput shape: {result.shape}")
        print("Output columns:", list(result.columns))
        
        # Check prediction fields
        prediction_fields = [
            'protein_class_pred_L1',
            'protein_class_pred_L2', 
            'protein_class_pred_L3',
            'protein_class_pred_rule_id',
            'protein_class_pred_evidence',
            'protein_class_pred_confidence'
        ]
        
        print("\nPrediction field values:")
        for field in prediction_fields:
            if field in result.columns:
                value = result[field].iloc[0] if len(result) > 0 else ""
                print(f"  {field}: {value}")
            else:
                print(f"  {field}: COLUMN NOT FOUND")
                
    except Exception as e:
        print(f"Protein classification failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
