from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.pipelines.target_gold import (
    annotate_source_rank,
    coalesce_by_priority,
    expand_xrefs,
    merge_components,
    split_accession_field,
)
from bioetl.sources.chembl.target.parser import (
    expand_json_column,
    expand_protein_classifications,
)

__all__ = ["build_gold_outputs", "build_targets_gold"]


def build_gold_outputs(
    df: pd.DataFrame,
    component_enrichment: pd.DataFrame,
    iuphar_gold: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Assemble deterministic gold-level DataFrames."""

    chembl_components = expand_json_column(df, "target_components")
    components_df = merge_components(
        chembl_components,
        component_enrichment,
        targets=df,
    )

    if not components_df.empty:
        components_df = annotate_source_rank(
            components_df,
            source_column="data_origin",
            output_column="merge_rank",
        )
        sort_columns = [
            col
            for col in [
                "target_chembl_id",
                "canonical_accession",
                "isoform_accession",
                "component_id",
            ]
            if col in components_df.columns
        ]
        if sort_columns:
            components_df = components_df.sort_values(sort_columns, kind="stable")
        components_df = components_df.reset_index(drop=True)

    protein_class_df = expand_protein_classifications(df)
    if not protein_class_df.empty:
        protein_class_df = protein_class_df.sort_values(
            ["target_chembl_id", "class_level", "class_name"],
            kind="stable",
        ).reset_index(drop=True)

    xref_df = expand_xrefs(df)
    if not xref_df.empty:
        sort_columns = [
            col
            for col in ["target_chembl_id", "xref_src_db", "xref_id", "component_id"]
            if col in xref_df.columns
        ]
        if sort_columns:
            xref_df = xref_df.sort_values(sort_columns, kind="stable").reset_index(drop=True)

    targets_gold = build_targets_gold(df, components_df, iuphar_gold)
    targets_gold = targets_gold.sort_values(["target_chembl_id"], kind="stable").reset_index(drop=True)

    return targets_gold, components_df, protein_class_df, xref_df


def build_targets_gold(
    df: pd.DataFrame,
    components_df: pd.DataFrame,
    iuphar_gold: pd.DataFrame,
) -> pd.DataFrame:
    """Construct the final targets table with aggregated attributes."""

    working = df.copy()

    coalesce_map = {
        "pref_name": [
            ("pref_name", "chembl"),
            ("uniprot_protein_name", "uniprot"),
            ("iuphar_name", "iuphar"),
        ],
        "organism": [
            ("organism", "chembl"),
            ("uniprot_taxonomy_name", "uniprot"),
        ],
        "tax_id": [
            ("tax_id", "chembl"),
            ("taxonomy", "chembl"),
            ("uniprot_taxonomy_id", "uniprot"),
        ],
        "gene_symbol": [
            ("gene_symbol", "chembl"),
            ("uniprot_gene_primary", "uniprot"),
        ],
        "hgnc_id": [
            ("hgnc_id", "chembl"),
            ("uniprot_hgnc_id", "uniprot"),
            ("hgnc", "uniprot"),
        ],
        "lineage": [
            ("lineage", "chembl"),
            ("uniprot_lineage", "uniprot"),
        ],
        "uniprot_id_primary": [
            ("uniprot_canonical_accession", "uniprot"),
            ("uniprot_accession", "chembl"),
            ("primaryAccession", "chembl"),
        ],
    }

    working = coalesce_by_priority(working, coalesce_map, source_suffix="_origin")

    if not iuphar_gold.empty:
        iuphar_subset = iuphar_gold.drop_duplicates("target_chembl_id")
        working = working.merge(
            iuphar_subset,
            on="target_chembl_id",
            how="left",
            suffixes=("", "_iuphar"),
        )

    if not components_df.empty and "isoform_accession" in components_df.columns:
        isoform_counts = components_df.groupby("target_chembl_id")["isoform_accession"].nunique()
        isoform_map = (
            components_df.groupby("target_chembl_id")["isoform_accession"]
            .apply(
                lambda values: sorted(
                    {str(value) for value in values if value not in {None, "", pd.NA}}
                )
            )
            .to_dict()
        )
    else:
        isoform_counts = pd.Series(dtype="Int64")
        isoform_map: dict[Any, list[str]] = {}

    working["isoform_count"] = (
        working["target_chembl_id"].map(isoform_counts).fillna(0).astype("Int64")
    )
    working["has_alternative_products"] = (working["isoform_count"] > 1).astype("boolean")

    working["has_uniprot"] = working["uniprot_id_primary"].notna().astype("boolean")
    if "iuphar_target_id" in working.columns:
        working["has_iuphar"] = working["iuphar_target_id"].notna().astype("boolean")
    else:
        working["has_iuphar"] = pd.Series(False, index=working.index, dtype="boolean")

    if "uniprot_secondary_accessions" in working.columns:
        secondary_map = {
            key: split_accession_field(value)
            for key, value in working.set_index("target_chembl_id")["uniprot_secondary_accessions"].items()
        }
    else:
        secondary_map = {}

    def combine_ids(row: pd.Series) -> Any:
        accs: list[str] = []
        primary = row.get("uniprot_id_primary")
        if pd.notna(primary):
            accs.append(str(primary))
        accs.extend(isoform_map.get(row.get("target_chembl_id"), []))
        accs.extend(secondary_map.get(row.get("target_chembl_id"), []))
        seen: set[str] = set()
        unique: list[str] = []
        for acc in accs:
            if not acc:
                continue
            if acc not in seen:
                seen.add(acc)
                unique.append(acc)
        return unique

    working["uniprot_all_accessions"] = working.apply(combine_ids, axis=1)
    working["uniprot_all_accessions"] = working["uniprot_all_accessions"].apply(
        lambda values: values if values else []
    )

    return working.convert_dtypes()
