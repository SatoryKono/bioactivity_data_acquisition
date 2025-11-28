"""Этапы пайплайна activity ChEMBL."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from bioetl.clients.chembl.entities import ChemblActivityClient
from bioetl.clients.chembl.factories import default_activity_client_factory
from bioetl.core.io.artifacts import (
    RunArtifacts,
    SchemaRegistry,
    WriteArtifacts,
)
from bioetl.core.io.output import UnifiedOutputWriter, emit_qc_artifact
from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.types import PipelineConfig, WriteResult
from bioetl.pipelines.chembl.activity.normalizers import (
    ActivityNormalizer,
)
from bioetl.pipelines.chembl.activity.parsers import (
    ActivityParser,
)
from bioetl.pipelines.chembl.batch_executor import execute_chembl_batches
from bioetl.pipelines.chembl.common import (
    ChemblExtractionDescriptor,
    ConfigValidationError,
)


@dataclass(slots=True)
class ActivityExtractor:
    """Извлечение активностей через клиент ChEMBL."""

    client_factory: Callable[
        [Any], ChemblActivityClient
    ] = default_activity_client_factory
    parser: ActivityParser = field(default_factory=ActivityParser)
    release: str | None = None

    def extract(
        self,
        config: PipelineConfig,
        descriptor: ChemblExtractionDescriptor,
        *,
        batch_size: int | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Execute the extraction stage for activity data.

        Args:
            config: Pipeline configuration.
            descriptor: ChEMBL extraction descriptor.
            batch_size: Optional batch size override.

        Returns:
            Tuple of (extracted dataframe, metadata dictionary).
        """
        effective_batch_size = batch_size or (
            descriptor.batch_plan.batch_size
            if descriptor.batch_plan
            else 20
        )
        if effective_batch_size is None:
            effective_batch_size = 20
        if effective_batch_size > 25:
            raise ConfigValidationError(
                "batch_size must not exceed 25 for ChEMBL API"
            )

        client = self.client_factory(config)
        status = client.status()
        if isinstance(status, Mapping):
            self.release = (
                str(status.get("chembl_release"))
                if status.get("chembl_release")
                else None
            )
        meta: dict[str, Any] = {
            "chembl_release": self.release,
            "status": status,
        }

        ids = descriptor.ids or []

        def fetch_batch(
            batch: Sequence[str] | None,
        ) -> tuple[pd.DataFrame, dict[str, Any]]:
            if not batch:
                return pd.DataFrame(), {"api_calls": 0}
            try:
                payload = client.fetch_by_ids(batch)
                # Handle both dict (legacy assumption?) and iterator
                iterator: Any = payload
                if isinstance(iterator, Mapping):
                    iterator = iterator.values()
                frames = []
                for raw in iterator:
                    if isinstance(raw, Mapping) and "results" not in raw:
                        raw = {"results": [raw]}
                    frames.append(self.parser.parse(raw))
                df = (
                    pd.concat(frames, ignore_index=True)
                    if frames
                    else pd.DataFrame()
                )
                return df, {"api_calls": 1}
            except RuntimeError as exc:  # pragma: no cover; noqa: BLE001
                fallback = self._fallback_rows(list(batch), exc)
                return fallback, {"fallback": len(fallback), "api_calls": 1}

        df, stats = execute_chembl_batches(
            fetch_batch, ids, batch_size=effective_batch_size
        )

        if self.release:
            df = df.assign(chembl_release=self.release)
        meta["failures"] = stats.fallback_count
        meta["api_calls"] = stats.api_calls
        return df, meta

    def _fallback_rows(
        self, ids: list[str], exc: Exception
    ) -> pd.DataFrame:
        timestamp = datetime.now(timezone.utc).isoformat()
        records = [
            {
                "activity_id": chembl_id,
                "assay_id": None,
                "target_id": None,
                "value": None,
                "unit": None,
                "error_code": "extract_failed",
                "http_status": getattr(exc, "status", None),
                "error_message": str(exc),
                "attempt": 1,
                "timestamp": timestamp,
            }
            for chembl_id in ids
        ]
        return pd.DataFrame.from_records(records)


@dataclass(slots=True)
class ActivityTransformer:
    """Нормализация и обогащение данных об активностях."""

    release: str | None = None
    normalizer: ActivityNormalizer = field(default_factory=ActivityNormalizer)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the transformation stage.

        Enriches data with domain-specific fields and normalizes columns.

        Args:
            df: Input dataframe from extraction.

        Returns:
            Transformed and normalized dataframe.
        """
        enriched = self._domain_enrich(df)
        return self.normalizer.normalize(enriched)

    def _domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.release and "chembl_release" not in df.columns:
            return df.assign(chembl_release=self.release)
        return df


@dataclass(slots=True)
class ActivityWriter:
    """Запись результатов activity-пайплайна с QC артефактами."""

    schema_registry: SchemaRegistry
    config: PipelineConfig
    pipeline_code: str
    run_id: str
    output_root: Path
    logs_directory: Path

    def write(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        *,
        run_stem: str,
        output_dir: Path,
    ) -> WriteResult:
        """Execute the write stage.

        Writes the dataframe to storage and emits QC artifacts.

        Args:
            df: Dataframe to write.
            artifacts: Artifacts container to populate.
            run_stem: Stem for run-specific filenames.
            output_dir: Target directory for output.

        Returns:
            Result of the write operation.
        """
        logger = UnifiedLogger.get(self.__class__.__name__).bind(
            run_id=self.run_id
        )
        writer = UnifiedOutputWriter(
            output_dir=output_dir,
            pipeline_code=self.pipeline_code,
            run_stem=run_stem,
            schema_registry=self.schema_registry,
            config=self.config,
            logger=logger,
        )
        run_artifacts = RunArtifacts(
            output_dir=output_dir,
            logs_directory=self.logs_directory,
            write_artifacts=artifacts,
        )
        write_result = writer.write_dataset_atomic(df, run_artifacts)
        qc_paths = emit_qc_artifact(df, run_artifacts)
        if qc_paths:
            artifacts.quality_report_path = qc_paths.get("quality_report")
        return write_result


__all__ = ["ActivityExtractor", "ActivityTransformer", "ActivityWriter"]
