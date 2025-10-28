"""Pandera schemas for TestItem data."""

import pandera as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema


class TestItemSchema(BaseSchema):
    """Schema for ChEMBL TestItem (molecule) data."""

    # ChEMBL identifiers
    molecule_chembl_id: Series[str] = pa.Field(nullable=False)
    molregno: Series[int] = pa.Field(nullable=True, ge=1)
    parent_chembl_id: Series[str] = pa.Field(nullable=True)

    # Structure
    canonical_smiles: Series[str] = pa.Field(nullable=True)
    standard_inchi: Series[str] = pa.Field(nullable=True)
    standard_inchi_key: Series[str] = pa.Field(nullable=True)

    # Properties
    molecular_weight: Series[float] = pa.Field(nullable=True, ge=0)
    heavy_atoms: Series[int] = pa.Field(nullable=True, ge=0)
    aromatic_rings: Series[int] = pa.Field(nullable=True, ge=0)
    rotatable_bonds: Series[int] = pa.Field(nullable=True, ge=0)
    hba: Series[int] = pa.Field(nullable=True, ge=0)  # Hydrogen bond acceptors
    hbd: Series[int] = pa.Field(nullable=True, ge=0)  # Hydrogen bond donors

    # Lipinski
    lipinski_ro5_violations: Series[int] = pa.Field(nullable=True, ge=0)
    lipinski_ro5_pass: Series[bool] = pa.Field(nullable=True)

    # Synonyms
    all_names: Series[str] = pa.Field(nullable=True)
    molecule_synonyms: Series[str] = pa.Field(nullable=True)  # JSON string

    # Classification
    atc_classifications: Series[str] = pa.Field(nullable=True)  # JSON string

    # PubChem enrichment (optional)
    pubchem_cid: Series[int] = pa.Field(nullable=True, ge=1)
    pubchem_synonyms: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False

