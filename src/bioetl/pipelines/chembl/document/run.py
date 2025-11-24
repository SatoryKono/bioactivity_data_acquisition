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
from bioetl.pipelines.chembl.document.normalize import (
    enrich_with_document_terms,
)
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

            # Normalise to mapping for safe key access
            config_mapping: Mapping[str, Any]
            if isinstance(chembl_config, Mapping):
                config_mapping = chembl_config
            else:
                config_mapping = {}

            # Check if document_term enrichment is enabled
            document_cfg = config_mapping.get("document", {})
            if not isinstance(document_cfg, Mapping):
                document_cfg = {}

            enrich_cfg = document_cfg.get("enrich", {})
            if not isinstance(enrich_cfg, Mapping):
                enrich_cfg = {}

            doc_term_enrich = enrich_cfg.get("document_term", {})
            if not isinstance(doc_term_enrich, Mapping):
                doc_term_enrich = {}

            if bool(doc_term_enrich.get("enabled", False)):
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
