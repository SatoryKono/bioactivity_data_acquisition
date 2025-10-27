#!/usr/bin/env python3
"""Final integration test for all target pipeline fixes."""

import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from library.target.uniprot_adapter import enrich_targets_with_uniprot
from library.target.iuphar_adapter import enrich_targets_with_iuphar, IupharApiCfg
from library.target.gtopdb_adapter import enrich_targets_with_gtopdb, GtopdbApiCfg
from library.target.protein_classification import append_protein_class_predictions
from library.target.normalize import TargetNormalizer

def main():
    """Test complete target pipeline with all fixes."""
    print("=== FINAL INTEGRATION TEST ===")
    
    # Create test data
    test_data = pd.DataFrame({
        'CHEMBL.TARGETS.target_chembl_id': ['CHEMBL2343'],
        'CHEMBL.TARGET_COMPONENTS.accession': ['O00141'],
        'mapping_uniprot_id': ['O00141'],
        'uniprot_id_primary': ['O00141']
    })
    
    print(f"Input data: {test_data.shape}")
    print("Input columns:", list(test_data.columns))
    
    try:
        # Step 1: UniProt enrichment
        print("\n1. Testing UniProt enrichment...")
        uniprot_cfg = {"enabled": True, "timeout": 30.0, "batch_size": 10}
        result = enrich_targets_with_uniprot(test_data, uniprot_cfg)
        print(f"   UniProt output: {result.shape}")
        
        # Step 2: IUPHAR enrichment
        print("\n2. Testing IUPHAR enrichment...")
        iuphar_cfg = IupharApiCfg(use_csv=True, dictionary_path="configs/dictionary/_target/_IUPHAR/")
        result = enrich_targets_with_iuphar(result, iuphar_cfg)
        print(f"   IUPHAR output: {result.shape}")
        
        # Step 3: GtoPdb enrichment
        print("\n3. Testing GtoPdb enrichment...")
        gtopdb_cfg = GtopdbApiCfg()
        result = enrich_targets_with_gtopdb(result, gtopdb_cfg)
        print(f"   GtoPdb output: {result.shape}")
        
        # Step 4: Protein classification
        print("\n4. Testing protein classification...")
        result = append_protein_class_predictions(result)
        print(f"   Classification output: {result.shape}")
        
        # Skip normalization for now (requires full config)
        print("\n5. Skipping normalization (requires full config)")
        
        # Check all previously empty columns
        print("\n=== RESULTS CHECK ===")
        
        columns_to_check = [
            "CHEMBL.PROTEIN_CLASSIFICATION.pref_name",
            "protein_name_alt",
            "protein_synonym_list", 
            "taxon_id",
            "lineage_superkingdom",
            "lineage_phylum",
            "sequence_length",
            "xref_uniprot",
            "reaction_ec_numbers_uniprot",
            "uniprot_version",
            "gene_symbol",
            "gene_symbol_list",
            "iuphar_target_id",
            "iuphar_family_id",
            "iuphar_full_id_path",
            "iuphar_full_name_path",
            "iuphar_gene_symbol",
            "iuphar_hgnc_id",
            "iuphar_hgnc_name",
            "iuphar_uniprot_id_primary",
            "iuphar_uniprot_name",
            "iuphar_organism",
            "iuphar_taxon_id",
            "iuphar_description",
            "iuphar_abbreviation",
            "gtop_target_id",
            "gtop_synonyms",
            "gtop_natural_ligands_n",
            "gtop_interactions_n",
            "gtop_function_text_short",
            "unified_tax_id",
            "protein_class_pred_L1",
            "protein_class_pred_L2",
            "protein_class_pred_L3",
            "protein_class_pred_evidence",
            "protein_class_pred_rule_id"
        ]
        
        print("\nChecking previously empty columns:")
        fixed_count = 0
        total_count = len(columns_to_check)
        
        for col in columns_to_check:
            if col in result.columns:
                value = result[col].iloc[0] if len(result) > 0 else ""
                is_empty = (pd.isna(value) or value == "" or value == "0")
                status = "FIXED" if not is_empty else "EMPTY"
                if not is_empty:
                    fixed_count += 1
                print(f"  {col:<50} | {status} | {str(value)[:50]}")
            else:
                print(f"  {col:<50} | MISSING")
        
        print(f"\n=== SUMMARY ===")
        print(f"Fixed columns: {fixed_count}/{total_count} ({fixed_count/total_count*100:.1f}%)")
        
        if fixed_count >= total_count * 0.8:  # 80% success rate
            print("SUCCESS: Most columns are now populated!")
            return 0
        else:
            print("PARTIAL: Some columns still need work")
            return 1
                
    except Exception as e:
        print(f"Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
