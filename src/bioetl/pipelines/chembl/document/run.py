"""Упрощённый Document-пайплайн."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.core.pipeline import RunResult
from bioetl.pipelines.chembl.common import BaseChemblPipeline
from bioetl.pipelines.chembl.document.normalize import enrich_with_document_terms


class ChemblDocumentPipeline(BaseChemblPipeline):
    entity_name = "document"
    id_column = "document_chembl_id"
    actor = "document_pipeline_actor"

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
        return {
            "field_mappings": {
                "document_chembl_id": "document_chembl_id",
                "title": "title",
            }
        }

    def get_schema(self) -> Mapping[str, Any]:
        return {"document_chembl_id": lambda series: series.notna()}

    def _build_document_client(self, bundle: Any) -> Any:
        return bundle.entity_client

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw DataFrame by applying normalization and enrichment."""
        if df.empty:
            return df

        # Start with base transformation (normalization)
        working_df = super().transform(df)

        # Check if enrichment is enabled
        chembl_config = getattr(self.config, "chembl", None)
        if chembl_config is None:
            domain = getattr(self.config, "domain", None)
            if domain is not None:
                chembl_config = getattr(domain, "chembl", None)

        if chembl_config is not None:
            # Convert to dict if needed
            if hasattr(chembl_config, "model_dump"):
                chembl_config = chembl_config.model_dump()
            elif hasattr(chembl_config, "dict"):
                chembl_config = chembl_config.dict()

            # Check if document_term enrichment is enabled
            doc_enrich = chembl_config.get("document", {}).get("enrich", {})
            doc_term_enrich = doc_enrich.get("document_term", {})
            if doc_term_enrich.get("enabled", False):
                # Get client from bundle
                bundle = self.build_chembl_entity_bundle(
                    entity_name="document",
                    source_name="chembl",
                    source_config=self._resolve_source_config("chembl"),
                    options={},
                    chembl_client_kwargs={},
                    fresh_http_client=False,
                )
                client = bundle.chembl_client

                # Apply enrichment
                working_df = enrich_with_document_terms(
                    working_df,
                    client,
                    doc_term_enrich,
                )

        return working_df

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
            include_correlation=include_correlation if include_correlation is not None else False,
            include_qc_metrics=include_qc_metrics if include_qc_metrics is not None else False,
        )


__all__ = ["ChemblDocumentPipeline"]
