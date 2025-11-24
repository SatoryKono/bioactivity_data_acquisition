"""Упрощённый Assay-пайплайн."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, cast

import pandas as pd
from bioetl.chembl.common.normalize import add_row_metadata
from bioetl.pipelines.chembl._constants import (
    API_ASSAY_FIELDS,
    ASSAY_MUST_HAVE_FIELDS,
)
from bioetl.pipelines.chembl.common import BaseChemblPipeline
from bioetl.pipelines.chembl.helpers import build_dataframe
from bioetl.pipelines.chembl.mixins import FieldMappingRule


class ChemblAssayPipeline(BaseChemblPipeline):
    entity_name = "assay"
    id_column = "assay_chembl_id"
    descriptor_must_have_fields: tuple[str, ...] = ASSAY_MUST_HAVE_FIELDS
    descriptor_default_select_fields = API_ASSAY_FIELDS

    def __init__(
        self,
        config: Any,
        run_id: str,
        source: Iterable[dict[str, Any]] | None = None,
        *,
        writer=None,
    ) -> None:
        super().__init__(config, run_id, source, writer=writer)

    def get_normalization_rules(self) -> Mapping[str, Any]:
        spec: dict[str, FieldMappingRule]
        spec = {
            "assay_chembl_id": FieldMappingRule(source="assay_chembl_id"),
            "assay_id": FieldMappingRule(source="assay_id"),
            "description": FieldMappingRule(source="description"),
            "assay_category": FieldMappingRule(source="assay_category"),
            "assay_strain": FieldMappingRule(source="assay_strain"),
            "assay_group": FieldMappingRule(source="assay_group"),
            "assay_type": FieldMappingRule(source="assay_type"),
            "assay_type_description": FieldMappingRule(
                source="assay_type_description",
            ),
            "assay_test_type": FieldMappingRule(source="assay_test_type"),
            "assay_organism": FieldMappingRule(source="assay_organism"),
            "assay_tax_id": FieldMappingRule(source="assay_tax_id"),
            "assay_tissue": FieldMappingRule(source="assay_tissue"),
            "assay_cell_type": FieldMappingRule(source="assay_cell_type"),
            "assay_subcellular_fraction": FieldMappingRule(
                source="assay_subcellular_fraction",
            ),
            "target_chembl_id": FieldMappingRule(source="target_chembl_id"),
            "document_chembl_id": FieldMappingRule(
                source="document_chembl_id",
            ),
            "src_assay_id": FieldMappingRule(source="src_assay_id"),
            "src_id": FieldMappingRule(source="src_id"),
            "cell_chembl_id": FieldMappingRule(source="cell_chembl_id"),
            "tissue_chembl_id": FieldMappingRule(source="tissue_chembl_id"),
            "confidence_score": FieldMappingRule(source="confidence_score"),
            "confidence_description": FieldMappingRule(
                source="confidence_description",
            ),
            "variant_sequence": FieldMappingRule(source="variant_sequence"),
            "curation_level": FieldMappingRule(source="curation_level"),
        }

        return self.build_normalization_rules_from_spec(spec)

    def get_schema(self):
        return {
            "assay_id": lambda series: series.notna(),
            "assay_type": lambda series: series.notna(),
            "assay_type_description": lambda series: series.notna(),
        }


    def save_results(
        self,
        df: pd.DataFrame,
        output_dir: Any,
        *,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
        **kwargs: Any,
    ) -> Any:
        """Persist results while preserving determinism and QC options.

        This override simply forwards all relevant flags to the base
        implementation so that behaviour stays aligned with other
        Chembl pipelines and the shared PipelineBase contract.
        """

        include_correlation_val = (
            include_correlation if include_correlation is not None else False
        )
        include_qc_metrics_val = (
            include_qc_metrics if include_qc_metrics is not None else False
        )

        return super().save_results(
            df,
            output_dir,
            extended=extended,
            include_correlation=include_correlation_val,
            include_qc_metrics=include_qc_metrics_val,
            **kwargs,
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw DataFrame by applying normalization and schema compliance."""
        if df.empty:
            return df
        # Convert DataFrame to records for processing
        records = cast(Sequence[Mapping[str, Any]], df.to_dict("records"))
        normalized = self._normalize(records)
        transformed_df = build_dataframe(normalized)

        # Ensure standard row metadata columns exist for determinism and hashing
        transformed_df, _ = add_row_metadata(
            transformed_df,
            subtype=self.pipeline_code,
            copy=False,
        )

        # Coerce confidence_score to nullable Int64 to match schema expectations
        if "confidence_score" in transformed_df.columns:
            numeric = pd.to_numeric(
                transformed_df["confidence_score"], errors="coerce"
            )
            transformed_df["confidence_score"] = numeric.astype("Int64")

        # Ensure all required schema columns are present
        schema_columns = (
            list(self.get_schema().keys()) if self.get_schema() else []
        )
        transformed_df = self._ensure_schema_columns(
            transformed_df, schema_columns
        )

        # Ensure assay_type_description column exists
        if "assay_type_description" not in transformed_df.columns:
            transformed_df["assay_type_description"] = pd.NA

        return transformed_df


__all__ = ["ChemblAssayPipeline"]
