"""Pipeline for loading Guide to Pharmacology (IUPHAR) targets."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.client_factory import APIClientFactory, ensure_target_source_config
from bioetl.core.logger import UnifiedLogger
from bioetl.core.materialization import MaterializationManager
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas.registry import schema_registry
from bioetl.sources.iuphar.client import IupharClient
from bioetl.sources.iuphar.pagination import PageNumberPaginator
from bioetl.sources.iuphar.parser import parse_api_response
from bioetl.sources.iuphar.request import (
    DEFAULT_PAGE_SIZE,
    build_families_request,
    build_targets_request,
)
from bioetl.sources.iuphar.schema import (
    IupharClassificationSchema,
    IupharGoldSchema,
    IupharTargetSchema,
)
from bioetl.sources.iuphar.service import IupharService, IupharServiceConfig
from bioetl.utils.output import finalize_output_dataset
from bioetl.utils.qc import (
    prepare_enrichment_metrics,
    update_summary_metrics,
    update_summary_section,
)

logger = UnifiedLogger.get(__name__)

schema_registry.register("gtp_iuphar_targets", "1.0.0", IupharTargetSchema)  # type: ignore[arg-type]
schema_registry.register("gtp_iuphar_classification", "1.0.0", IupharClassificationSchema)  # type: ignore[arg-type]
schema_registry.register("gtp_iuphar_gold", "1.0.0", IupharGoldSchema)  # type: ignore[arg-type]


class GtpIupharPipeline(PipelineBase):
    """ETL pipeline dedicated to Guide to Pharmacology target data."""

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        self.primary_schema = IupharTargetSchema

        factory = APIClientFactory.from_pipeline_config(config)
        source_config = ensure_target_source_config(
            config.sources.get("iuphar"),
            defaults={"enabled": True},
        )
        self.source_config = source_config

        self.iuphar_client: IupharClient | None = None
        self._raw_client: UnifiedAPIClient | None = None
        self._paginator: PageNumberPaginator | None = None

        if source_config.enabled:
            api_client_config = factory.create("iuphar", source_config)
            raw_client = UnifiedAPIClient(api_client_config)
            self._raw_client = raw_client
            self.iuphar_client = IupharClient(raw_client)
            self.register_client(raw_client)

        page_size = int(source_config.batch_size) if source_config.batch_size else DEFAULT_PAGE_SIZE
        if self._raw_client is not None:
            self._paginator = PageNumberPaginator(self._raw_client, page_size)

        self.iuphar_service = IupharService(
            config=IupharServiceConfig(
                identifier_column="targetId",
                output_identifier_column="iuphar_target_id",
                candidate_columns=("iuphar_name", "name", "abbreviation", "synonyms"),
                gene_symbol_columns=("geneSymbol",),
                fallback_source="iuphar",
            ),
        )

        runtime_config = getattr(self.config, "runtime", None)
        self.materialization_manager = MaterializationManager(
            self.config.materialization,
            runtime=runtime_config,
            stage_context=self.stage_context,
            output_writer_factory=lambda: self.output_writer,
            run_id=self.run_id,
            determinism=self.determinism,
        )

        self.classification_df: pd.DataFrame = pd.DataFrame()
        self.gold_df: pd.DataFrame = pd.DataFrame()

    # ------------------------------------------------------------------
    # Lifecycle methods
    # ------------------------------------------------------------------
    def extract(self, input_file: Path | None = None) -> pd.DataFrame:  # noqa: D401 - part of PipelineBase contract
        if self.iuphar_client is None or self._paginator is None or not self.source_config.enabled:
            logger.warning(
                "iuphar_extraction_disabled",
                enabled=self.source_config.enabled,
                has_client=self.iuphar_client is not None,
            )
            return pd.DataFrame(
                columns=[
                    "targetId",
                    "iuphar_target_id",
                    "iuphar_name",
                    "name",
                    "abbreviation",
                    "synonyms",
                    "annotationStatus",
                    "geneSymbol",
                ]
            )

        targets = self._paginator.fetch_all(
            "/targets",
            unique_key="targetId",
            params=build_targets_request(annotation_status="CURATED"),
            parser=lambda payload: parse_api_response(payload, unique_key="targetId"),
        )
        families = self._paginator.fetch_all(
            "/targets/families",
            unique_key="familyId",
            params=build_families_request(),
            parser=lambda payload: parse_api_response(payload, unique_key="familyId"),
        )

        self.stage_context["raw_targets"] = targets
        self.stage_context["raw_families"] = families

        if not targets:
            logger.info("iuphar_extraction_empty", families=len(families))
            return pd.DataFrame(
                columns=[
                    "targetId",
                    "iuphar_target_id",
                    "iuphar_name",
                    "name",
                    "abbreviation",
                    "synonyms",
                    "annotationStatus",
                    "geneSymbol",
                ]
            )

        frame = pd.DataFrame(targets).convert_dtypes()
        if "name" in frame.columns:
            frame["iuphar_name"] = frame["name"]
        if "targetId" in frame.columns:
            frame["iuphar_target_id"] = pd.to_numeric(frame["targetId"], errors="coerce").astype("Int64")
        else:
            frame["iuphar_target_id"] = pd.Series(dtype="Int64")

        return frame

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - part of PipelineBase contract
        if df.empty:
            self.classification_df = pd.DataFrame()
            self.gold_df = pd.DataFrame()
            self.reset_additional_tables()
            self.qc_enrichment_metrics = pd.DataFrame()
            return df

        targets_payload = self.stage_context.get("raw_targets")
        families_payload = self.stage_context.get("raw_families")
        if not isinstance(targets_payload, Sequence):
            targets_payload = None
        if not isinstance(families_payload, Sequence):
            families_payload = None

        enriched_df, classification_df, gold_df, metrics = self.iuphar_service.enrich_targets(
            df,
            targets=targets_payload,
            families=families_payload,
        )

        if "targetId" in enriched_df.columns:
            fallback_ids = pd.to_numeric(enriched_df["targetId"], errors="coerce").astype("Int64")
            if "iuphar_target_id" in enriched_df.columns:
                enriched_df["iuphar_target_id"] = (
                    enriched_df["iuphar_target_id"].fillna(fallback_ids).astype("Int64")
                )
            else:
                enriched_df["iuphar_target_id"] = fallback_ids
        else:
            enriched_df["iuphar_target_id"] = enriched_df["iuphar_target_id"].astype("Int64")

        best_classifications = classification_df[classification_df["classification_source"] == "iuphar"]
        if not best_classifications.empty and "iuphar_target_id" in best_classifications.columns:
            best_lookup = (
                best_classifications.sort_values(
                    ["iuphar_target_id", "classification_depth"],
                    ascending=[True, False],
                    kind="stable",
                )
                .drop_duplicates("iuphar_target_id")
                .set_index("iuphar_target_id")
            )
            for column in ("classification_path", "classification_depth", "iuphar_type", "iuphar_class", "iuphar_subclass"):
                if column in best_lookup.columns:
                    enriched_df[column] = enriched_df["iuphar_target_id"].map(best_lookup[column])

        timestamp = pd.Timestamp.now(tz="UTC").isoformat()
        metadata = {
            "pipeline_version": self.config.pipeline.version,
            "run_id": self.run_id,
            "source_system": "iuphar",
            "extracted_at": timestamp,
        }

        sort_by = list(self.determinism.sort.by or ["iuphar_target_id"])
        ascending = list(self.determinism.sort.ascending or []) or None

        final_df = finalize_output_dataset(
            enriched_df,
            business_key="iuphar_target_id",
            sort_by=sort_by,
            ascending=ascending,
            schema=IupharTargetSchema,
            metadata=metadata,
        )

        self.set_export_metadata_from_dataframe(
            final_df,
            pipeline_version=self.config.pipeline.version,
            source_system="iuphar",
            column_order=list(final_df.columns),
        )

        self.classification_df = classification_df.convert_dtypes()
        self.gold_df = gold_df.convert_dtypes()

        self.reset_additional_tables()
        if not self.classification_df.empty:
            self.add_additional_table(
                "gtp_iuphar_classification",
                self.classification_df,
                formats=("csv", "parquet"),
            )
        if not self.gold_df.empty:
            self.add_additional_table(
                "gtp_iuphar_gold",
                self.gold_df,
                formats=("csv", "parquet"),
            )

        self.materialization_manager.materialize_iuphar(
            self.classification_df,
            self.gold_df,
            format=self.config.materialization.default_format,
        )

        self.qc_metrics.update(metrics)
        if metrics:
            update_summary_metrics(self.qc_summary_data, metrics)
            enrichment_records = [
                {
                    "metric": "iuphar",
                    "value": metrics.get("iuphar_coverage"),
                    "stage": "iuphar",
                    "passed": True,
                    "severity": "info",
                }
            ]
            self.qc_enrichment_metrics = prepare_enrichment_metrics(enrichment_records)
        else:
            self.qc_enrichment_metrics = pd.DataFrame()

        update_summary_section(
            self.qc_summary_data,
            "datasets",
            {
                "targets": int(len(final_df)),
                "classification": int(len(self.classification_df)),
                "gold": int(len(self.gold_df)),
            },
        )

        return final_df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - part of PipelineBase contract
        if df.empty:
            logger.info("iuphar_validation_skipped", rows=0)
            update_summary_section(
                self.qc_summary_data,
                "validation",
                {"targets": {"status": "skipped", "rows": 0, "severity": "info"}},
            )
            return df

        validated_targets = self.run_schema_validation(
            df,
            IupharTargetSchema,
            dataset_name="gtp_iuphar_targets",
            severity="critical",
        )

        self.classification_df = self.run_schema_validation(
            self.classification_df,
            IupharClassificationSchema,
            dataset_name="gtp_iuphar_classification",
        )

        self.gold_df = self.run_schema_validation(
            self.gold_df,
            IupharGoldSchema,
            dataset_name="gtp_iuphar_gold",
        )

        update_summary_section(
            self.qc_summary_data,
            "datasets",
            {
                "targets": int(len(validated_targets)),
                "classification": int(len(self.classification_df)),
                "gold": int(len(self.gold_df)),
            },
        )

        logger.info(
            "iuphar_validation_completed",
            rows=len(validated_targets),
            classification_rows=len(self.classification_df),
            gold_rows=len(self.gold_df),
        )

        return validated_targets


__all__ = ["GtpIupharPipeline"]
