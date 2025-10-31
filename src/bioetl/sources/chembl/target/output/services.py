from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.materialization import MaterializationManager
from bioetl.core.output_writer import UnifiedOutputWriter
from bioetl.utils.output import finalize_output_dataset
from bioetl.utils.qc import prepare_enrichment_metrics

__all__ = ["TargetOutputService"]


@dataclass(slots=True)
class TargetOutputService:
    """Coordinate output materialization and QC helpers for the target pipeline."""

    pipeline_config: PipelineConfig
    run_id: str
    stage_context: dict[str, Any]
    determinism: Any
    runtime_config: Any | None = None
    writer: UnifiedOutputWriter | None = field(init=False, default=None)
    materialization_manager: MaterializationManager | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        determinism_copy = self.determinism.model_copy()
        determinism_copy.column_order = []
        self.writer = UnifiedOutputWriter(
            f"{self.run_id}-materialization",
            determinism=determinism_copy,
            pipeline_config=self.pipeline_config,
        )
        self.materialization_manager = MaterializationManager(
            self.pipeline_config.materialization,
            runtime=self.runtime_config,
            stage_context=self.stage_context,
            output_writer_factory=lambda: self.writer,
            run_id=self.run_id,
            determinism=self.determinism,
        )

    def finalize_targets(
        self,
        df: pd.DataFrame,
        *,
        business_key: str,
        sort_by: list[str],
        ascending: list[bool] | None,
        schema,
        metadata: dict[str, Any],
    ) -> pd.DataFrame:
        """Apply deterministic ordering and metadata to the targets dataset."""

        return finalize_output_dataset(
            df,
            business_key=business_key,
            sort_by=sort_by,
            ascending=ascending,
            schema=schema,
            metadata=metadata,
        )

    def prepare_enrichment_metrics(self, records: list[dict[str, Any]]) -> pd.DataFrame:
        """Transform enrichment metric payloads into a DataFrame."""

        return prepare_enrichment_metrics(records)

    def resolve_materialization_format(self, runtime_options: dict[str, Any]) -> str:
        """Determine the requested output format for materialization artefacts."""

        runtime_override = runtime_options.get("materialization_format") or runtime_options.get("format")
        if runtime_override:
            resolved = str(runtime_override).strip().lower()
            if resolved in {"csv", "parquet"}:
                return resolved

        materialization_paths = getattr(self.pipeline_config, "materialization", None)
        if materialization_paths:
            inferred = materialization_paths.infer_dataset_format("gold", "targets")
            if inferred:
                normalised = str(inferred).strip().lower()
                if normalised in {"csv", "parquet"}:
                    return normalised

        return "parquet"

    def materialize_gold(
        self,
        targets_df: pd.DataFrame,
        components_df: pd.DataFrame,
        protein_class_df: pd.DataFrame,
        xref_df: pd.DataFrame,
        *,
        format_name: str,
        output_path,
    ) -> None:
        self.materialization_manager.materialize_gold(
            targets_df,
            components_df,
            protein_class_df,
            xref_df,
            format=format_name,
            output_path=output_path,
        )

    def materialize_silver(
        self,
        uniprot_df: pd.DataFrame,
        component_df: pd.DataFrame,
        *,
        format_name: str,
    ) -> None:
        self.materialization_manager.materialize_silver(
            uniprot_df,
            component_df,
            format=format_name,
        )

    def materialize_iuphar(
        self,
        classification_df: pd.DataFrame,
        gold_df: pd.DataFrame,
        *,
        format_name: str,
    ) -> None:
        self.materialization_manager.materialize_iuphar(
            classification_df,
            gold_df,
            format=format_name,
        )

