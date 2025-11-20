"""Упрощённый Activity-пайплайн ChEMBL на базе общего каркаса."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd

from bioetl.clients.chembl_entity_factory import (
    ChemblClientBundle,
    ChemblEntityClientFactory,
)
from collections.abc import Sequence

from bioetl.chembl.common.enrich import ChemblEnrichmentScenario
from bioetl.clients.client_chembl import ChemblClient
from bioetl.core.io import ensure_columns
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.pipelines.chembl.common import BaseChemblPipeline
from bioetl.pipelines.chembl.activity.normalize import enrich_with_compound_record
from bioetl.pipelines.mixins.enrichment_engine import EnrichmentScenarioEngine


class ChemblActivityPipeline(BaseChemblPipeline):
    """Adapter-класс, реализующий правила нормализации/обогащения для активности."""

    def __init__(
        self,
        config: Any,
        run_id: str,
        source: Iterable[dict[str, Any]] | None = None,
        *,
        writer=None,
    ) -> None:
        super().__init__(config, run_id, source, writer=writer)
        self.writer = writer

    def logger_for(
        self,
        *,
        stage: str | None = None,
        component: str | None = None,
        **extra: Any,
    ) -> Any:
        """Return a logger bound to the pipeline and stage context."""
        return self._make_pipeline_logger(stage=stage, component=component, **extra)

    def _build_activity_enrichment_bundle(
        self,
        entity_name: str,
        *,
        client_name: str,
    ) -> ChemblClientBundle:
        """Return Chembl bundle for enrichment scenarios and register the client."""

        if not entity_name:
            msg = "entity_name must be provided for enrichment bundle"
            raise ValueError(msg)

        factory = ChemblEntityClientFactory(self.config)
        source_config = getattr(self.config.domain, "sources", {}).get("chembl")
        bundle = factory.build(
            entity_name,
            source_name="chembl",
            source_config=source_config,
        )

        registered = getattr(self, "_registered_clients", {})
        if client_name and client_name not in registered:
            self.register_client(client_name, bundle.api_client)

        return bundle

    def _extract_assay_fields(
        self,
        df: pd.DataFrame,
        client: ChemblClient,
        log: Any | None = None,
    ) -> pd.DataFrame:
        """Hydrate assay metadata columns deterministically for extract tests."""

        base_log = log or UnifiedLogger.get(__name__).bind(stage="extract_assay_fields")
        required_columns: tuple[tuple[str, str], ...] = (
            ("assay_organism", "string"),
            ("assay_tax_id", "Int64"),
        )
        working_df = ensure_columns(df.copy(), required_columns)
        if working_df.empty:
            base_log.debug(LogEvents.EXTRACT_ASSAY_FIELDS_COMPLETE, rows=0, matched=0)
            return working_df

        if "assay_chembl_id" not in working_df.columns:
            base_log.warning(LogEvents.ENRICHMENT_SKIPPED_MISSING_COLUMNS, missing_columns=["assay_chembl_id"])
            return working_df

        assay_ids = (
            working_df["assay_chembl_id"].dropna().astype("string").str.strip().str.upper()
        )
        valid_ids = assay_ids[assay_ids.ne("")].drop_duplicates().sort_values(kind="mergesort")
        if valid_ids.empty:
            base_log.debug(LogEvents.ENRICHMENT_SKIPPED_NO_VALID_IDS)
            return working_df

        try:
            base_log.info(
                LogEvents.EXTRACT_ASSAY_FIELDS_FETCHING,
                ids=len(valid_ids),
            )
            records_df = client.fetch_assays_by_ids(
                ids=list(valid_ids),
                fields=["assay_chembl_id", "assay_organism", "assay_tax_id"],
            )
        except Exception as exc:  # noqa: BLE001
            base_log.warning(
                LogEvents.EXTRACT_ASSAY_FIELDS_FETCH_ERROR,
                error=str(exc),
            )
            working_df["assay_organism"] = working_df["assay_organism"].astype("string")
            working_df["assay_tax_id"] = working_df["assay_tax_id"].astype("Int64")
            return working_df

        if not isinstance(records_df, pd.DataFrame) or records_df.empty:
            base_log.debug(LogEvents.ENRICHMENT_NO_RECORDS_FOUND)
            working_df["assay_organism"] = working_df["assay_organism"].astype("string")
            working_df["assay_tax_id"] = working_df["assay_tax_id"].astype("Int64")
            return working_df

        payload = (
            records_df.loc[:, ["assay_chembl_id", "assay_organism", "assay_tax_id"]]
            .dropna(subset=["assay_chembl_id"])
            .copy()
        )
        if payload.empty:
            base_log.debug(LogEvents.ENRICHMENT_NO_RECORDS_FOUND)
            working_df["assay_organism"] = working_df["assay_organism"].astype("string")
            working_df["assay_tax_id"] = working_df["assay_tax_id"].astype("Int64")
            return working_df

        payload["assay_chembl_id"] = payload["assay_chembl_id"].astype("string").str.strip().str.upper()
        payload = (
            payload[payload["assay_chembl_id"].ne("")]
            .drop_duplicates(subset=["assay_chembl_id"], keep="first")
            .sort_values(by=["assay_chembl_id"], kind="mergesort")
            .reset_index(drop=True)
        )

        payload["assay_tax_id"] = pd.to_numeric(payload["assay_tax_id"], errors="coerce")
        payload.loc[payload["assay_tax_id"] < 1, "assay_tax_id"] = pd.NA
        payload["assay_tax_id"] = payload["assay_tax_id"].astype("Int64")
        payload["assay_organism"] = payload["assay_organism"].astype("string")

        merged = working_df.merge(
            payload,
            on="assay_chembl_id",
            how="left",
            suffixes=("", "_enrich"),
            sort=False,
        )
        for column in ("assay_organism", "assay_tax_id"):
            enrich_column = f"{column}_enrich"
            if enrich_column in merged.columns:
                merged[column] = merged[enrich_column].combine_first(merged[column])
                merged = merged.drop(columns=[enrich_column])

        merged["assay_organism"] = merged["assay_organism"].astype("string")
        merged["assay_tax_id"] = merged["assay_tax_id"].astype("Int64")

        base_log.info(
            LogEvents.EXTRACT_ASSAY_FIELDS_COMPLETE,
            rows=int(len(merged)),
            matched=int(payload.shape[0]),
        )
        return merged

    def get_normalization_rules(self) -> Mapping[str, Any]:
        return {
            "field_mappings": {
                "activity_id": "activity_id",
                "assay_id": "assay_id",
                "value": "value",
            },
            "value_normalizers": {
                "activity_id": lambda v: int(v) if v is not None else None,
                "value": lambda v: float(v) if v is not None else None,
            },
        }

    def get_enrichment_rules(self):
        def add_flags(record: Mapping[str, Any]) -> Mapping[str, Any]:
            enriched = dict(record)
            enriched["is_active"] = bool(record.get("value"))
            return enriched

        return [add_flags]

    def get_schema(self):
        return {
            "activity_id": lambda series: series.notna(),
            "assay_id": lambda series: series.notna(),
        }

    def execute_enrichment_stages(
        self,
        df: pd.DataFrame,
        *,
        stages: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Execute registered enrichment scenarios using EnrichmentScenarioEngine."""
        scenarios = getattr(self.__class__, "_ENRICHMENT_SCENARIOS", {})
        if not scenarios:
            return super().execute_enrichment_stages(df, stages=stages)

        engine = EnrichmentScenarioEngine()
        for scenario in scenarios.values():
            engine.register(scenario)

        default_order = getattr(self.__class__, "_DEFAULT_ENRICHMENT_ORDER", None)
        selected = stages if stages is not None else default_order
        return engine.execute(self, df, selected=selected)

    # CLI совместимость
    def save_results(self, df: pd.DataFrame, output_path, **_: Any):
        df.to_csv(output_path, index=False)
        return {"output_path": str(output_path), "rows": len(df)}


__all__ = ["ChemblActivityPipeline"]
