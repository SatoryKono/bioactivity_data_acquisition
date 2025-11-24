"""Упрощённый Document-пайплайн."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.core.pipeline import RunResult
from bioetl.pipelines.chembl._constants import (
    API_DOCUMENT_FIELDS,
    DOCUMENT_MUST_HAVE_FIELDS,
)
from bioetl.pipelines.chembl.common import BaseChemblPipeline
from bioetl.pipelines.chembl.mixins import FieldMappingRule


class ChemblDocumentPipeline(BaseChemblPipeline):
    entity_name = "document"
    id_column = "document_chembl_id"
    actor = "document_pipeline_actor"
    descriptor_must_have_fields: tuple[str, ...] = DOCUMENT_MUST_HAVE_FIELDS
    descriptor_default_select_fields = API_DOCUMENT_FIELDS

    def __init__(
        self,
        config: Any,
        run_id: str,
        source: Iterable[dict[str, Any]] | None = None,
        *,
        writer: Any = None,
    ) -> None:
        super().__init__(config, run_id, source, writer=writer)

    def get_normalization_rules(self) -> Mapping[str, Any]:
        spec: dict[str, FieldMappingRule]
        spec = {
            "document_chembl_id": FieldMappingRule(
                source="document_chembl_id",
            ),
            "title": FieldMappingRule(source="title"),
        }

        return self.build_normalization_rules_from_spec(spec)

    def get_schema(self) -> Mapping[str, Any]:
        return {"document_chembl_id": lambda series: series.notna()}

    def _build_document_client(self, bundle: Any) -> Any:
        return bundle.entity_client

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw DataFrame by applying normalization."""
        if df.empty:
            return df

        return super().transform(df)

    def save_results(
        self,
        df: pd.DataFrame,
        output_dir: Path,
        *,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
    ) -> RunResult:
        return super().save_results(
            df,
            output_dir,
            extended=extended,
            include_correlation=(
                include_correlation
                if include_correlation is not None
                else False
            ),
            include_qc_metrics=(
                include_qc_metrics if include_qc_metrics is not None else False
            ),
        )


__all__ = ["ChemblDocumentPipeline"]
