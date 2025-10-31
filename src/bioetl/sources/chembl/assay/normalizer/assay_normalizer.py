"""Normalization helpers dedicated to the assay pipeline."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.normalizers import registry

from ..constants import ASSAY_CLASS_ENRICHMENT_WHITELIST, TARGET_ENRICHMENT_WHITELIST

logger = UnifiedLogger.get(__name__)


class AssayNormalizer:
    """Normalize raw assay payloads and enrichment frames."""

    def normalize_assay(self, assay: dict[str, Any], chembl_release: str | None) -> dict[str, Any]:
        """Normalise a single assay payload from ChEMBL."""

        classifications = assay.get("assay_classifications")
        classifications_str = None
        if classifications:
            try:
                classifications_str = json.dumps(
                    classifications,
                    ensure_ascii=False,
                    sort_keys=True,
                )
            except (TypeError, ValueError):
                try:
                    classifications_str = " | ".join(classifications)
                except TypeError:
                    classifications_str = None

        params = assay.get("assay_parameters")
        params_json = json.dumps(params, ensure_ascii=False) if params is not None else None

        record: dict[str, Any] = {
            "assay_chembl_id": registry.normalize("chemistry.chembl_id", assay.get("assay_chembl_id")),
            "assay_type": assay.get("assay_type"),
            "assay_category": assay.get("assay_category"),
            "assay_cell_type": assay.get("assay_cell_type"),
            "assay_classifications": classifications_str,
            "assay_group": assay.get("assay_group"),
            "assay_organism": assay.get("assay_organism"),
            "assay_parameters_json": params_json,
            "assay_strain": assay.get("assay_strain"),
            "assay_subcellular_fraction": assay.get("assay_subcellular_fraction"),
            "assay_tax_id": assay.get("assay_tax_id"),
            "assay_test_type": assay.get("assay_test_type"),
            "assay_tissue": assay.get("assay_tissue"),
            "assay_type_description": assay.get("assay_type_description"),
            "bao_format": registry.normalize("chemistry.bao_id", assay.get("bao_format")),
            "bao_label": registry.normalize("chemistry.string", assay.get("bao_label"), max_length=128),
            "cell_chembl_id": registry.normalize("chemistry.chembl_id", assay.get("cell_chembl_id")),
            "confidence_description": assay.get("confidence_description"),
            "confidence_score": assay.get("confidence_score"),
            "assay_description": registry.normalize("chemistry.string", assay.get("description")),
            "document_chembl_id": registry.normalize("chemistry.chembl_id", assay.get("document_chembl_id")),
            "relationship_description": assay.get("relationship_description"),
            "relationship_type": assay.get("relationship_type"),
            "src_assay_id": assay.get("src_assay_id"),
            "src_id": assay.get("src_id"),
            "target_chembl_id": registry.normalize("chemistry.chembl_id", assay.get("target_chembl_id")),
            "tissue_chembl_id": registry.normalize("chemistry.chembl_id", assay.get("tissue_chembl_id")),
        }

        assay_class = assay.get("assay_class")
        if isinstance(assay_class, dict):
            record.update(
                {
                    "assay_class_id": assay_class.get("assay_class_id"),
                    "assay_class_bao_id": registry.normalize("chemistry.bao_id", assay_class.get("bao_id")),
                    "assay_class_type": assay_class.get("assay_class_type"),
                    "assay_class_l1": assay_class.get("class_level_1"),
                    "assay_class_l2": assay_class.get("class_level_2"),
                    "assay_class_l3": assay_class.get("class_level_3"),
                    "assay_class_description": assay_class.get("assay_class_description"),
                }
            )

        variant_sequences = assay.get("variant_sequence")
        variant_records: list[dict[str, Any]] = []

        if isinstance(variant_sequences, dict):
            variant_records = [dict(variant_sequences)]
        elif isinstance(variant_sequences, (list, tuple)):
            variant_records = [dict(variant) for variant in variant_sequences if isinstance(variant, dict)]

        def _variant_value(variant: dict[str, Any], *candidates: str) -> Any:
            for key in candidates:
                if key is None:
                    continue
                value = variant.get(key)
                if value is not None:
                    return value
            return None

        variant_sequence_json = None
        if variant_records:
            primary_variant = variant_records[0]
            record.update(
                {
                    "variant_id": primary_variant.get("variant_id"),
                    "variant_base_accession": _variant_value(
                        primary_variant,
                        "accession",
                        "base_accession",
                    ),
                    "variant_mutation": _variant_value(primary_variant, "mutation"),
                    "variant_sequence": _variant_value(
                        primary_variant,
                        "sequence",
                        "variant_seq",
                    ),
                    "variant_accession_reported": _variant_value(
                        primary_variant,
                        "accession",
                        "accession_reported",
                    ),
                }
            )

            try:
                variant_sequence_json = json.dumps(
                    variant_records,
                    ensure_ascii=False,
                    sort_keys=True,
                )
            except (TypeError, ValueError):
                try:
                    sanitized: list[dict[str, Any]] = []
                    for variant in variant_records:
                        sanitized.append(
                            {
                                key: variant.get(key)
                                for key in (
                                    "variant_id",
                                    "accession",
                                    "base_accession",
                                    "sequence",
                                    "variant_seq",
                                    "mutation",
                                    "tax_id",
                                    "version",
                                    "accession_reported",
                                )
                                if key in variant
                            }
                        )
                    variant_sequence_json = json.dumps(
                        sanitized,
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                except (TypeError, ValueError):
                    variant_sequence_json = None

        record["variant_sequence_json"] = variant_sequence_json
        record["source_system"] = "chembl"
        record["chembl_release"] = chembl_release
        return record

    # ------------------------------------------------------------------
    # Enrichment helpers
    # ------------------------------------------------------------------
    def enrich_targets(
        self,
        base_df: pd.DataFrame,
        reference_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Enrich the base frame with target reference metadata."""

        if base_df.empty or reference_df.empty:
            return base_df

        allowed_columns = [
            column for column in reference_df.columns if column in TARGET_ENRICHMENT_WHITELIST
        ]
        reference_df = reference_df[allowed_columns]

        merged = base_df.merge(
            reference_df,
            on="target_chembl_id",
            how="left",
            suffixes=("", "_ref"),
        )

        if "pref_name" in merged.columns:
            pref_loss_mask = merged["target_chembl_id"].notna() & merged["pref_name"].isna()
            missing_count = int(pref_loss_mask.sum())
            if missing_count:
                logger.warning("target_enrichment_join_loss", missing_count=missing_count)

        return merged

    def enrich_assay_classes(
        self,
        classes_df: pd.DataFrame,
        reference_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Enrich classification rows with reference metadata."""

        if classes_df.empty or reference_df.empty:
            return classes_df

        allowed_columns = [
            column for column in reference_df.columns if column in ASSAY_CLASS_ENRICHMENT_WHITELIST.values()
        ]
        reference_df = reference_df[allowed_columns]

        merged = classes_df.merge(
            reference_df,
            on="assay_class_id",
            how="left",
            suffixes=("", "_ref"),
        )

        for column in ASSAY_CLASS_ENRICHMENT_WHITELIST.values():
            if column == "assay_class_id":
                continue
            ref_column = f"{column}_ref"
            if ref_column in merged.columns:
                merged[column] = merged[column].fillna(merged[ref_column])
                merged.drop(columns=[ref_column], inplace=True)

        if "assay_class_bao_id" in merged.columns:
            missing_count = int(
                (
                    merged["assay_class_id"].notna()
                    & merged["assay_class_bao_id"].isna()
                ).sum()
            )
            if missing_count:
                logger.warning("assay_class_enrichment_join_loss", missing_count=missing_count)

        return merged
