"""Test enrichment functions directly to see if they work."""
import pandas as pd
from src.library.target.uniprot_adapter import enrich_targets_with_uniprot, UniprotApiCfg

# Create test data
test_data = pd.DataFrame({
    'CHEMBL.TARGETS.target_chembl_id': ['CHEMBL2343'],
    'CHEMBL.TARGET_COMPONENTS.accession': ['O00141'],
    'mapping_uniprot_id': ['O00141'],
})

print("Input data:")
print(test_data.columns.tolist())
print(test_data.head())

# Test UniProt enrichment
cfg = UniprotApiCfg(
    base_url="https://rest.uniprot.org",
    timeout=60.0,
    max_retries=3,
    batch_size=10,
)

print("\nTesting UniProt enrichment...")
try:
    enriched = enrich_targets_with_uniprot(test_data, cfg)
    print(f"Output columns count: {len(enriched.columns)}")
    print(f"Output columns: {enriched.columns.tolist()}")
    
    # Check specific fields
    check_fields = [
        'protein_name_alt',
        'protein_synonym_list', 
        'taxon_id',
        'lineage_superkingdom',
        'sequence_length',
        'gene_symbol',
    ]
    
    print("\nField values:")
    for field in check_fields:
        if field in enriched.columns:
            val = enriched[field].iloc[0] if not enriched.empty else None
            print(f"  {field}: {val}")
        else:
            print(f"  {field}: MISSING")
            
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

