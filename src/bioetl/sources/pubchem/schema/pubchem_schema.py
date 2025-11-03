"""Pandera schema describing the standalone PubChem enrichment dataset."""

from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series
from bioetl.schemas.base import BaseSchema

__all__ = ["PubChemSchema"]


class PubChemSchema(BaseSchema):
    """Schema for PubChem enrichment outputs keyed by ChEMBL molecules."""

    molecule_chembl_id: Series[str] = pa.Field(
        nullable=False,
        regex=r"^CHEMBL\d+$",
        description="Primary molecule identifier from ChEMBL",
    )
    standard_inchi_key: Series[str] = pa.Field(
        nullable=True,
        regex=r"^[A-Z\-]+$",
        description="Standard InChIKey used for PubChem lookups",
    )

    pubchem_cid: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Resolved PubChem compound identifier",
    )
    pubchem_molecular_formula: Series[str] = pa.Field(
        nullable=True,
        description="Molecular formula provided by PubChem",
    )
    pubchem_molecular_weight: Series[pd.Float64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Molecular weight reported by PubChem",
    )
    pubchem_canonical_smiles: Series[str] = pa.Field(
        nullable=True,
        description="Canonical SMILES string from PubChem",
    )
    pubchem_isomeric_smiles: Series[str] = pa.Field(
        nullable=True,
        description="Isomeric SMILES string from PubChem",
    )
    pubchem_inchi: Series[str] = pa.Field(
        nullable=True,
        description="IUPAC International Chemical Identifier",
    )
    pubchem_inchi_key: Series[str] = pa.Field(
        nullable=True,
        description="InChIKey returned by PubChem",
    )
    pubchem_iupac_name: Series[str] = pa.Field(
        nullable=True,
        description="IUPAC name resolved by PubChem",
    )
    pubchem_registry_id: Series[str] = pa.Field(
        nullable=True,
        description="Registry identifier provided by PubChem",
    )
    pubchem_rn: Series[str] = pa.Field(
        nullable=True,
        description="Registry Number (RN) assigned by PubChem",
    )
    pubchem_synonyms: Series[str] = pa.Field(
        nullable=True,
        description="JSON encoded list of PubChem synonyms",
    )
    pubchem_enriched_at: Series[str] = pa.Field(
        nullable=True,
        description="ISO8601 timestamp when enrichment was executed",
    )
    pubchem_cid_source: Series[str] = pa.Field(
        nullable=True,
        description="Source used to resolve the PubChem CID",
    )
    pubchem_fallback_used: Series[pd.BooleanDtype] = pa.Field(
        nullable=True,
        description="Flag indicating whether fallback logic was triggered",
    )
    pubchem_enrichment_attempt: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Number of attempts performed during enrichment",
    )
    pubchem_lookup_inchikey: Series[str] = pa.Field(
        nullable=True,
        description="Identifier submitted to PubChem when the InChIKey was missing",
    )

    class Config:
        strict = True
        coerce = True
        ordered = False

    _column_order = [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "run_id",
        "source_system",
        "chembl_release",
        "extracted_at",
        "molecule_chembl_id",
        "standard_inchi_key",
        "pubchem_cid",
        "pubchem_molecular_formula",
        "pubchem_molecular_weight",
        "pubchem_canonical_smiles",
        "pubchem_isomeric_smiles",
        "pubchem_inchi",
        "pubchem_inchi_key",
        "pubchem_iupac_name",
        "pubchem_registry_id",
        "pubchem_rn",
        "pubchem_synonyms",
        "pubchem_enriched_at",
        "pubchem_cid_source",
        "pubchem_fallback_used",
        "pubchem_enrichment_attempt",
        "pubchem_lookup_inchikey",
    ]

    # Guard for Pandera's postponed evaluation when resolving annotations.
    _TYPING_ANY_SENTINEL: Any | None = None
