"""UniProt schema validation tests."""

from __future__ import annotations

import unittest

import pandas as pd

from bioetl.schemas.target import TargetSchema


class TestUniProtSchema(unittest.TestCase):
    """Validate that UniProt target records satisfy :class:`TargetSchema`."""

    def test_target_schema_accepts_minimal_record(self) -> None:
        record = {
            "target_chembl_id": ["CHEMBL123"],
            "isoform_ids": [None],
            "isoform_names": [None],
            "isoforms": [None],
            "pref_name": ["Test protein"],
            "target_type": ["PROTEIN"],
            "organism": ["Homo sapiens"],
            "tax_id": [pd.NA],
            "gene_symbol": ["ABC1"],
            "hgnc_id": [None],
            "lineage": [None],
            "primaryAccession": ["P12345"],
            "target_names": [None],
            "target_uniprot_id": ["P12345"],
            "organism_chembl": [None],
            "species_group_flag": [None],
            "target_components": [None],
            "protein_classifications": [None],
            "cross_references": [None],
            "target_names_chembl": [None],
            "pH_dependence": [None],
            "pH_dependence_chembl": [None],
            "target_organism": [None],
            "target_tax_id": [None],
            "target_uniprot_accession": ["P12345"],
            "target_isoform": [None],
            "isoform_ids_chembl": [None],
            "isoform_names_chembl": [None],
            "isoforms_chembl": [None],
            "uniprot_accession": ["P12345"],
            "uniprot_id_primary": ["P12345"],
            "uniprot_ids_all": ["P12345"],
            "isoform_count": [0],
            "has_alternative_products": [pd.NA],
            "has_uniprot": [pd.NA],
            "has_iuphar": [pd.NA],
            "iuphar_type": [None],
            "iuphar_class": [None],
            "iuphar_subclass": [None],
            "data_origin": ["test"],
        }

        frame = pd.DataFrame(record)
        validated = TargetSchema.validate(frame)
        self.assertFalse(validated.empty)
        self.assertIn("target_chembl_id", validated.columns)
