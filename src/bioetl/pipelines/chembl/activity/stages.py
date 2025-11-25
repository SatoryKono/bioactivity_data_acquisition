"""Этапы пайплайна activity ChEMBL."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

import pandas as pd

from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.clients.factories.default_chembl_factory import default_activity_client_factory
from bioetl.core.io.artifacts import RunArtifacts, SchemaRegistry, WriteArtifacts
from bioetl.core.io.output import UnifiedOutputWriter, emit_qc_artifact
from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.types import PipelineConfig, WriteResult
from bioetl.pipelines.chembl.activity.normalizers.activity_normalizer import ActivityNormalizer
from bioetl.pipelines.chembl.activity.parsers.activity_parser import ActivityParser
from bioetl.pipelines.chembl.common import ChemblExtractionDescriptor, ConfigValidationError


@dataclass(slots=True)
class ActivityExtractor:
    """Извлечение активностей через клиент ChEMBL."""

    client_factory: Callable[[Any], ChemblActivityClient] = default_activity_client_factory
    parser: ActivityParser = field(default_factory=ActivityParser)
    release: str | None = None

    def extract(
        self, config: PipelineConfig, descriptor: ChemblExtractionDescriptor, *, batch_size: int | None = None
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        effective_batch_size = batch_size or (descriptor.batch_plan.batch_size if descriptor.batch_plan else 20)
        if effective_batch_size is None:
            effective_batch_size = 20
        if effective_batch_size > 25:
            raise ConfigValidationError("batch_size must not exceed 25 for ChEMBL API")

        client = self.client_factory(config)
        status = client.status()
        if isinstance(status, Mapping):
            self.release = str(status.get("chembl_release")) if status.get("chembl_release") else None
        meta: dict[str, Any] = {"chembl_release": self.release, "status": status}

        ids = descriptor.ids or []
        batches = [ids[i : i + effective_batch_size] for i in range(0, len(ids), effective_batch_size)]
        if not batches:
            batches = [None]

        frames: list[pd.DataFrame] = []
        failures = 0
        for batch in batches:
            if batch is None:
                continue
            try:
                payload = client.fetch_by_ids(batch)
                batch_frames = [self.parser.parse(raw) for raw in payload]
                frame = pd.concat(batch_frames, ignore_index=True) if batch_frames else pd.DataFrame()
                frames.append(frame)
            except Exception as exc:  # pragma: no cover - defensive
                failures += 1
                frames.append(self._fallback_rows(batch, exc))

        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if self.release:
            df = df.assign(chembl_release=self.release)
        meta["failures"] = failures
        meta["api_calls"] = len(batches)
        return df, meta

    def _fallback_rows(self, ids: list[str], exc: Exception) -> pd.DataFrame:
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

    def write(self, df: pd.DataFrame, artifacts: WriteArtifacts, *, run_stem: str, output_dir: Path) -> WriteResult:
        logger = UnifiedLogger.get(self.__class__.__name__).bind(run_id=self.run_id)
        writer = UnifiedOutputWriter(
            output_dir=output_dir,
            pipeline_code=self.pipeline_code,
            run_stem=run_stem,
            schema_registry=self.schema_registry,
            config=self.config,
            logger=logger,
        )
        run_artifacts = RunArtifacts(output_dir=output_dir, logs_directory=self.logs_directory, write_artifacts=artifacts)
        write_result = writer.write_dataset_atomic(df, run_artifacts)
        qc_paths = emit_qc_artifact(df, run_artifacts)
        if qc_paths:
            artifacts.quality_report_path = qc_paths.get("quality_report")
        return write_result


__all__ = ["ActivityExtractor", "ActivityTransformer", "ActivityWriter"]
