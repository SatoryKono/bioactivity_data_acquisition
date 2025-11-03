"""Shared constants for the ChEMBL assay pipeline."""

from __future__ import annotations

TARGET_ENRICHMENT_WHITELIST: list[str] = [
    "target_chembl_id",
    "pref_name",
    "organism",
    "target_type",
    "species_group_flag",
    "tax_id",
    "component_count",
]

ASSAY_CLASS_ENRICHMENT_WHITELIST: dict[str, str] = {
    "assay_class_id": "assay_class_id",
    "bao_id": "assay_class_bao_id",
    "class_type": "assay_class_type",
    "l1": "assay_class_l1",
    "l2": "assay_class_l2",
    "l3": "assay_class_l3",
    "description": "assay_class_description",
}

NULLABLE_INT_COLUMNS: tuple[str, ...] = (
    "assay_tax_id",
    "confidence_score",
    "species_group_flag",
    "tax_id",
    "component_count",
    "assay_class_id",
    "variant_id",
    "src_id",
)

ASSAY_FALLBACK_BUSINESS_COLUMNS: tuple[str, ...] = (
    "assay_chembl_id",
    "source_system",
    "chembl_release",
    "fallback_reason",
    "fallback_error_type",
    "fallback_error_code",
    "fallback_error_message",
    "fallback_http_status",
    "fallback_retry_after_sec",
    "fallback_attempt",
    "fallback_timestamp",
    "run_id",
    "git_commit",
    "config_hash",
)

__all__ = [
    "TARGET_ENRICHMENT_WHITELIST",
    "ASSAY_CLASS_ENRICHMENT_WHITELIST",
    "NULLABLE_INT_COLUMNS",
    "ASSAY_FALLBACK_BUSINESS_COLUMNS",
]
