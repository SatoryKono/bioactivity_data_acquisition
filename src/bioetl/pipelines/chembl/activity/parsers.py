"""Parsing helpers for ChEMBL activity payloads."""

from typing import Any

import pandas as pd

from bioetl.clients.chembl import (
    ColumnMapping,
    build_records_from_payload,
)


_ACTIVITY_MAPPINGS = [
    # Core activity identifiers
    ColumnMapping("activity_id", ("activity_id", "activity_chembl_id")),

    # Row metadata
    ColumnMapping("row_subtype", ("row_subtype",)),
    ColumnMapping("row_index", ("row_index",)),

    # Assay information
    ColumnMapping("assay_chembl_id", ("assay_chembl_id", "assay_id")),
    ColumnMapping("assay_type", ("assay_type",)),
    ColumnMapping("assay_description", ("assay_description",)),
    ColumnMapping("assay_organism", ("assay_organism",)),
    ColumnMapping("assay_tax_id", ("assay_tax_id",)),

    # Test item/molecule information
    ColumnMapping(
        "testitem_chembl_id", ("testitem_chembl_id",)
    ),
    ColumnMapping(
        "molecule_chembl_id",
        ("molecule_chembl_id", "molecule_chembl_id"),
    ),
    ColumnMapping("parent_molecule_chembl_id", ("parent_molecule_chembl_id",)),
    ColumnMapping("molecule_pref_name", ("molecule_pref_name",)),

    # Target information
    ColumnMapping("target_chembl_id", ("target_chembl_id", "target_id")),
    ColumnMapping("target_pref_name", ("target_pref_name",)),
    ColumnMapping("target_organism", ("target_organism",)),
    ColumnMapping("target_tax_id", ("target_tax_id",)),

    # Document and source information
    ColumnMapping("document_chembl_id", ("document_chembl_id",)),
    ColumnMapping("record_id", ("record_id",)),
    ColumnMapping("src_id", ("src_id",)),

    # Activity measurements
    ColumnMapping("type", ("type",)),
    ColumnMapping("relation", ("relation",)),
    ColumnMapping("value", ("standard_value", "value")),
    ColumnMapping("units", ("standard_units", "units")),
    ColumnMapping("standard_type", ("standard_type",)),
    ColumnMapping("standard_relation", ("standard_relation",)),
    ColumnMapping("standard_value", ("standard_value",)),
    ColumnMapping("standard_units", ("standard_units",)),
    ColumnMapping("standard_flag", ("standard_flag",)),
    ColumnMapping("pchembl_value", ("pchembl_value",)),
    ColumnMapping("uo_units", ("uo_units",)),
    ColumnMapping("qudt_units", ("qudt_units",)),

    # Comments and annotations
    ColumnMapping("activity_comment", ("activity_comment",)),
    ColumnMapping("bao_endpoint", ("bao_endpoint",)),
    ColumnMapping("bao_format", ("bao_format",)),
    ColumnMapping("bao_label", ("bao_label",)),

    # Compound information
    ColumnMapping("canonical_smiles", ("canonical_smiles",)),
    ColumnMapping("ligand_efficiency", ("ligand_efficiency",)),
    ColumnMapping("compound_key", ("compound_key",)),
    ColumnMapping("compound_name", ("compound_name",)),

    # Data validity and curation
    ColumnMapping("data_validity_comment", ("data_validity_comment",)),
    ColumnMapping("data_validity_description", ("data_validity_description",)),
    ColumnMapping("potential_duplicate", ("potential_duplicate",)),
    ColumnMapping("curated", ("curated",)),
    ColumnMapping("removed", ("removed",)),

    # Activity properties (JSON string)
    ColumnMapping("activity_properties", ("activity_properties",)),
]


class ActivityParser:
    """Convert raw API responses into a normalized tabular form."""

    def parse(self, raw_json: Any) -> pd.DataFrame:
        """Parse raw API JSON into a dataframe with canonical columns."""
        records = build_records_from_payload(raw_json, _ACTIVITY_MAPPINGS)
        columns = [mapping.column for mapping in _ACTIVITY_MAPPINGS]
        return pd.DataFrame.from_records(records, columns=columns)


__all__ = ["ActivityParser"]
