"""Detailed analysis of which fields are actually filled."""
import pandas as pd

df = pd.read_csv('data/output/targets/targets_20251027.csv')
print("=== DETAILED FIELD ANALYSIS ===\n")

# Group fields by source
field_groups = {
    'UniProt Basic': ['uniprot_id_primary', 'uniProtkbId', 'mapping_uniprot_id', 'CHEMBL.TARGET_COMPONENTS.accession'],
    'UniProt Names': ['recommendedName', 'geneName', 'gene_symbol', 'protein_name_alt', 'protein_synonym_list'],
    'UniProt Taxonomy': ['CHEMBL.TARGETS.organism', 'taxon_id', 'lineage_superkingdom', 'lineage_phylum', 'genus'],
    'UniProt Sequence': ['sequence_length'],
    'UniProt Functions': ['molecular_function', 'cellular_component', 'subcellular_location'],
    'UniProt XRefs': ['xref_uniprot', 'xref_ensembl', 'xref_pdb', 'xref_alphafold'],
    'UniProt PTM': ['transmembrane', 'glycosylation', 'phosphorylation', 'signal_peptide'],
    'IUPHAR': ['iuphar_target_id', 'iuphar_name', 'iuphar_family_id', 'iuphar_type'],
    'GtoPdb': ['gtop_target_id', 'gtop_synonyms', 'gtop_natural_ligands_n'],
    'Protein Class': ['protein_class_pred_L1', 'protein_class_pred_L2', 'protein_class_pred_L3'],
    'ChEMBL': ['CHEMBL.PROTEIN_CLASSIFICATION.pref_name', 'reaction_ec_numbers'],
}

for group_name, fields in field_groups.items():
    print(f"\n{group_name}:")
    for field in fields:
        if field not in df.columns:
            print(f"  {field:45s}: NOT IN OUTPUT")
            continue
        
        empty = (df[field].isna() | (df[field].astype(str).str.strip() == '')).sum()
        filled = len(df) - empty
        pct = (filled / len(df)) * 100
        
        status = "OK" if filled == len(df) else ("PARTIAL" if filled > 0 else "EMPTY")
        print(f"  {status} {field:45s}: {filled:2d}/{len(df)} filled ({pct:5.1f}%)")
        
        # Show sample for filled fields
        if filled > 0:
            sample_idx = df[field].notna() & (df[field].astype(str).str.strip() != '')
            if sample_idx.any():
                sample_val = str(df.loc[sample_idx, field].iloc[0])
                print(f"     Sample: {sample_val[:60]}{'...' if len(sample_val) > 60 else ''}")

print("\n\n=== SUMMARY ===")
total_fields = sum(len(fields) for fields in field_groups.values())
total_exists = sum(1 for fields in field_groups.values() for f in fields if f in df.columns)
print(f"Fields in analysis: {total_fields}")
print(f"Fields in output: {total_exists}")

# Count completely empty
completely_empty = []
for fields in field_groups.values():
    for field in fields:
        if field in df.columns:
            empty = (df[field].isna() | (df[field].astype(str).str.strip() == '')).sum()
            if empty == len(df):
                completely_empty.append(field)

print(f"Completely empty fields: {len(completely_empty)}")

