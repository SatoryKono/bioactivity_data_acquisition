"""Schema level tests for PubChem outputs."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.sources.pubchem.schema import PubChemSchema


def test_schema_accepts_minimal_valid_row() -> None:
    columns = PubChemSchema.get_column_order()
    row = {column: None for column in columns}
    row.update(
        {
            "index": 0,
            "hash_row": "hash-row",
            "hash_business_key": "hash-bk",
            "pipeline_version": "1.0.0",
            "run_id": "run-1",
            "source_system": "pubchem",
            "chembl_release": None,
            "extracted_at": "2024-01-01T00:00:00+00:00",
            "molecule_chembl_id": "CHEMBL1",
            "standard_inchi_key": "ABCDEFGHIJKLMN-OPQRSTUVWX-Y",
            "pubchem_cid": 123,
            "pubchem_molecular_formula": "C2H6",
            "pubchem_molecular_weight": 46.07,
            "pubchem_canonical_smiles": "CC",
            "pubchem_isomeric_smiles": "C[C@H](O)H",
            "pubchem_inchi": "InChI=1S/C2H6O",
            "pubchem_inchi_key": "LFQSCWFLJHTTHZ-UHFFFAOYSA-N",
            "pubchem_iupac_name": "ethanol",
            "pubchem_registry_id": "64-17-5",
            "pubchem_rn": "64-17-5",
            "pubchem_synonyms": "[]",
            "pubchem_enriched_at": "2024-01-01T00:00:00+00:00",
            "pubchem_cid_source": "inchikey",
            "pubchem_fallback_used": False,
            "pubchem_enrichment_attempt": 1,
            "pubchem_lookup_inchikey": "ABCDEFGHIJKLMN-OPQRSTUVWX-Y",
        }
    )
    frame = pd.DataFrame([row], columns=columns)

    validated = PubChemSchema.validate(frame)
    assert len(validated) == 1


def test_schema_rejects_invalid_molecule_id() -> None:
    frame = pd.DataFrame(
        {
            "molecule_chembl_id": ["INVALID"],
            "standard_inchi_key": [None],
        }
    )

    with pytest.raises(Exception):
        PubChemSchema.validate(frame, lazy=True)
