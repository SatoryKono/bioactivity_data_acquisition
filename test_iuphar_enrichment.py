#!/usr/bin/env python3
"""Test IUPHAR enrichment directly."""

import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from library.target.iuphar_adapter import enrich_targets_with_iuphar, IupharApiCfg

def main():
    """Test IUPHAR enrichment."""
    print("Testing IUPHAR enrichment...")
    
    # Create test data
    test_data = pd.DataFrame({
        'CHEMBL.TARGETS.target_chembl_id': ['CHEMBL2343'],
        'mapping_uniprot_id': ['O00141'],
        'uniprot_id_primary': ['O00141']
    })
    
    print(f"Input data: {test_data.shape}")
    print(test_data)
    
    # Configure IUPHAR
    cfg = IupharApiCfg(
        use_csv=True,
        dictionary_path="configs/dictionary/_target/_IUPHAR/",
        timeout=30.0
    )
    
    try:
        # Test enrichment
        result = enrich_targets_with_iuphar(test_data, cfg)
        
        print(f"\nOutput shape: {result.shape}")
        print(f"Output columns: {list(result.columns)}")
        
        # Check IUPHAR fields
        iuphar_fields = [
            'iuphar_target_id',
            'iuphar_family_id', 
            'iuphar_full_id_path',
            'iuphar_full_name_path',
            'iuphar_gene_symbol',
            'iuphar_hgnc_id',
            'iuphar_hgnc_name',
            'iuphar_uniprot_id_primary',
            'iuphar_uniprot_name',
            'iuphar_organism',
            'iuphar_taxon_id',
            'iuphar_description',
            'iuphar_abbreviation'
        ]
        
        print("\nIUPHAR field values:")
        for field in iuphar_fields:
            if field in result.columns:
                value = result[field].iloc[0] if len(result) > 0 else ""
                print(f"  {field}: {value}")
            else:
                print(f"  {field}: COLUMN NOT FOUND")
                
    except Exception as e:
        print(f"IUPHAR enrichment failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
