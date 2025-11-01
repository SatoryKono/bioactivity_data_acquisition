"""Reusable pipeline base for single external enrichment sources."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas.base import BaseSchema
from bioetl.schemas.registry import schema_registry
from bioetl.utils.output import finalize_output_dataset
from bioetl.utils.qc import update_summary_metrics, update_summary_section

logger = UnifiedLogger.get(__name__)

_SYSTEM_COLUMNS: tuple[str, ...] = (
    "index",
    "hash_row",
    "hash_business_key",
    "pipeline_version",
    "run_id",
    "source_system",
    "chembl_release",
    "extracted_at",
)


@dataclass
class IdentifierPayload:
    """Container aggregating identifier collections used during enrichment."""

    pmids: list[str]
    dois: list[str]
    titles: list[str]
    records: list[Mapping[str, Any]]


if TYPE_CHECKING:  # pragma: no cover - used for typing only
    from bioetl.sources.document.pipeline import AdapterDefinition


class ExternalSourcePipeline(PipelineBase):
    """Base pipeline orchestrating enrichment via a single external adapter."""

    source_name: ClassVar[str]
    adapter_definition: ClassVar["AdapterDefinition"]
    normalized_schema: ClassVar[type[BaseSchema]]
    business_key: ClassVar[str]
    metadata_source_system: ClassVar[str]

    default_input_filename: ClassVar[Path] = Path("document.csv")
    expected_input_columns: ClassVar[tuple[str, ...]] = ("doi", "pmid", "title")
    identifier_columns: ClassVar[dict[str, tuple[str, ...]]] = {
        "doi": ("doi", "doi_clean"),
        "pmid": ("pmid",),
        "title": ("title",),
    }
    match_columns: ClassVar[tuple[str, ...]] = ()
    sort_by: ClassVar[tuple[str, ...]] = ()

    def __init__(self, config: "PipelineConfig", run_id: str):  # type: ignore[name-defined]
        super().__init__(config, run_id)
        self.primary_schema = self.normalized_schema
        self._ensure_schema_registered()
        self.adapter = self._initialise_adapter()
        self._input_rows = 0
        self._requested_counts: dict[str, int] = {}
        self.runtime_options.setdefault("source", self.source_name)

    # Lazy import guard for type checking
    if True:  # pragma: no cover - executed at import time only
        from bioetl.config import PipelineConfig  # noqa: PLC0415

    def _initialise_adapter(self) -> Any | None:
        """Construct the external adapter using structured configuration."""

        sources = getattr(self.config, "sources", None)
        if not isinstance(sources, Mapping):
            logger.warning("adapter_sources_unavailable", source=self.source_name)
            return None

        source_cfg = sources.get(self.source_name)
        if source_cfg is None:
            logger.warning("adapter_config_missing", source=self.source_name)
            return None

        enabled = getattr(source_cfg, "enabled", True)
        if not enabled:
            logger.info("adapter_disabled", source=self.source_name)
            return None

        from bioetl.sources.chembl.document.request.external import (  # noqa: PLC0415
            build_adapter_configs,
        )

        api_config, adapter_config = build_adapter_configs(
            self.config,
            self.source_name,
            source_cfg,
            self.adapter_definition,
        )

        adapter = self.adapter_definition.adapter_cls(api_config, adapter_config)
        api_client = getattr(adapter, "api_client", None)
        if api_client is not None:
            self.register_client(api_client)
        logger.info("adapter_initialised", source=self.source_name)
        return adapter

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Load identifiers from the configured lookup table."""

        df, _ = self.read_input_table(
            default_filename=self.default_input_filename,
            expected_columns=list(self.expected_input_columns),
            dtype="string",
            input_file=input_file,
        )
        if not df.empty:
            df = df.convert_dtypes()
        self._input_rows = int(len(df))
        update_summary_section(
            self.qc_summary_data,
            "inputs",
            {self.source_name: {"rows": self._input_rows}},
        )
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Request enrichment data from the configured adapter."""

        if df.empty:
            logger.info("no_identifiers_provided", source=self.source_name)
            payload = IdentifierPayload([], [], [], [])
        else:
            payload = self._build_identifier_payload(df)

        requested_total = sum(len(values) for key, values in vars(payload).items() if key != "records")
        self._requested_counts = {
            key: len(values)
            for key, values in vars(payload).items()
            if key != "records" and values
        }

        if requested_total == 0:
            logger.warning("no_valid_identifiers", source=self.source_name)
            enriched = self._empty_dataframe()
        elif self.adapter is None:
            logger.warning("adapter_unavailable", source=self.source_name)
            enriched = self._empty_dataframe()
        else:
            enriched = self.adapter.process_identifiers(
                pmids=payload.pmids or None,
                dois=payload.dois or None,
                titles=payload.titles or None,
                records=payload.records or None,
            )
            if enriched is None or enriched.empty:
                logger.warning(
                    "adapter_returned_empty",
                    source=self.source_name,
                    requested=requested_total,
                )
                enriched = self._empty_dataframe()
            else:
                enriched = enriched.convert_dtypes()

        enriched = self._ensure_data_columns(enriched)
        matched_count = self._resolve_match_count(enriched)
        self._update_metrics(requested_total, matched_count)

        finalized = self._finalize_dataset(enriched)
        self.set_export_metadata_from_dataframe(
            finalized,
            pipeline_version=self.config.pipeline.version,
            source_system=self.metadata_source_system,
            chembl_release=None,
            column_order=list(finalized.columns),
        )
        return finalized

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate the enriched dataset using the registered schema."""

        validated = self.run_schema_validation(
            df,
            self.normalized_schema,
            dataset_name=self.source_name,
            severity="error",
        )
        row_count = int(len(validated))
        update_summary_section(
            self.qc_summary_data,
            "row_counts",
            {self.source_name: row_count},
        )
        update_summary_section(
            self.qc_summary_data,
            "datasets",
            {self.source_name: {"rows": row_count}},
        )
        return validated

    def close_resources(self) -> None:  # noqa: D401
        """Close the adapter API client."""

        adapter = getattr(self, "adapter", None)
        api_client = getattr(adapter, "api_client", None)
        self._close_resource(api_client, resource_name=f"api_client.{self.source_name}")
        super().close_resources()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _build_identifier_payload(self, df: pd.DataFrame) -> IdentifierPayload:
        records = df.replace({pd.NA: None}).to_dict("records")
        dois = self._collect_identifiers(df, "doi")
        pmids = self._collect_identifiers(df, "pmid")
        titles = self._collect_identifiers(df, "title")
        return IdentifierPayload(pmids=pmids, dois=dois, titles=titles, records=records)

    def _collect_identifiers(self, df: pd.DataFrame, identifier: str) -> list[str]:
        columns = self.identifier_columns.get(identifier, ())
        if not columns:
            return []

        seen: set[str] = set()
        collected: list[str] = []
        for column in columns:
            if column not in df.columns:
                continue
            series = df[column].dropna()
            if series.empty:
                continue
            values = series.astype("string").str.strip()
            for value in values:
                if not value:
                    continue
                normalised = str(value)
                if normalised in seen:
                    continue
                seen.add(normalised)
                collected.append(normalised)
        return collected

    def _ensure_data_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        required = self._schema_data_columns()
        if not required:
            return df
        working = df.copy()
        for column in required:
            if column not in working.columns:
                working[column] = pd.NA
        extra = [column for column in working.columns if column not in required]
        return working[required + extra]

    def _schema_data_columns(self) -> list[str]:
        schema_columns = self._schema_columns()
        return [column for column in schema_columns if column not in _SYSTEM_COLUMNS]

    def _schema_columns(self) -> list[str]:
        schema = self.normalized_schema
        columns: list[str] = []
        resolver = getattr(schema, "get_column_order", None)
        if callable(resolver):
            try:
                columns = list(resolver())
            except Exception:  # pragma: no cover - defensive
                columns = []
        if not columns:
            order = getattr(schema, "_column_order", None)
            if isinstance(order, Sequence):
                columns = [str(column) for column in order]
        return columns

    def _resolve_match_count(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        for column in self.match_columns or (self.business_key,):
            if column in df.columns:
                series = df[column]
                if pd.api.types.is_bool_dtype(series):
                    return int(series.fillna(False).sum())
                return int(series.notna().sum())
        return int(len(df))

    def _update_metrics(self, requested: int, matched: int) -> None:
        coverage_threshold = self._resolve_coverage_threshold()
        coverage = float(matched / requested) if requested else 0.0
        metrics: dict[str, dict[str, Any]] = {
            f"{self.source_name}.requested": {
                "count": requested,
                "value": float(requested),
                "passed": True,
                "severity": "info",
            },
            f"{self.source_name}.coverage": {
                "count": matched,
                "value": coverage,
                "threshold": coverage_threshold,
                "passed": coverage >= coverage_threshold if requested else True,
                "severity": "warning"
                if requested and coverage < coverage_threshold
                else "info",
            },
        }
        update_summary_metrics(self.qc_summary_data, metrics)
        self.qc_metrics.update(metrics)

    def _resolve_coverage_threshold(self) -> float:
        qc_config = getattr(self.config, "qc", None)
        thresholds = getattr(qc_config, "thresholds", {}) if qc_config else {}
        key = f"{self.source_name}.min_coverage"
        try:
            value = float(thresholds.get(key, 0.0)) if isinstance(thresholds, Mapping) else 0.0
        except (TypeError, ValueError):  # pragma: no cover - configuration guard
            logger.warning("invalid_coverage_threshold", source=self.source_name, value=thresholds.get(key))
            return 0.0
        return value

    def _finalize_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        sort_columns = list(self.sort_by or ())
        if not sort_columns:
            sort_columns = [self.business_key]

        extracted_at = pd.Timestamp.now(tz="UTC").isoformat()
        finalized = finalize_output_dataset(
            df,
            business_key=self.business_key,
            sort_by=sort_columns,
            ascending=True,
            schema=self.normalized_schema,
            metadata={
                "pipeline_version": self.config.pipeline.version,
                "run_id": self.run_id,
                "source_system": self.metadata_source_system,
                "chembl_release": None,
                "extracted_at": extracted_at,
            },
        )

        if finalized.empty:
            finalized = self._empty_dataframe()

        finalized = self._ensure_schema_columns(finalized)
        return finalized

    def _ensure_schema_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = self._schema_columns()
        if not columns:
            return df
        working = df.copy()
        for column in columns:
            if column not in working.columns:
                working[column] = pd.Series(dtype="object")
        return working.reindex(columns=columns)

    def _empty_dataframe(self) -> pd.DataFrame:
        columns = self._schema_data_columns()
        if not columns:
            return pd.DataFrame()
        return pd.DataFrame(columns=columns)

    def _ensure_schema_registered(self) -> None:
        try:
            schema_registry.register(
                self.source_name,
                "1.0.0",
                self.normalized_schema,
            )  # type: ignore[arg-type]
        except ValueError:
            # Already registered – keep the initial registration.
            pass

__all__ = ["ExternalSourcePipeline", "IdentifierPayload"]
