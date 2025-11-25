from __future__ import annotations

"""ChEMBL activity pipeline scaffold with descriptor-driven extraction."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping

import pandas as pd

from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.clients.factories.default_chembl_factory import default_activity_client_factory
from bioetl.core.io.artifacts import RunArtifacts, SchemaRegistry, SchemaRegistryEntry, WriteArtifacts
from bioetl.core.io.output import UnifiedOutputWriter, emit_qc_artifact
from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.orchestration import PipelineBaseCommon
from bioetl.core.pipeline.types import (
    MaterializationConfig,
    PipelineConfig,
    PipelineInfo,
    RunResult,
    StageExecutionOptions,
    WriteResult,
)
from bioetl.pipelines.chembl.common import (
    BatchPlan,
    ChemblExtractionDescriptor,
    ChemblPipelineContract,
    ConfigValidationError,
    descriptor_from_options,
)
from bioetl.pipelines.chembl.stage_runner import register_pipeline
from bioetl.pipelines.chembl.activity.parsers.activity_parser import ActivityParser
from bioetl.pipelines.chembl.activity.normalizers.activity_normalizer import ActivityNormalizer
from bioetl.schemas.activity_schema import ActivityColumns, ActivitySchema


class ChemblActivityPipeline(PipelineBaseCommon, ChemblPipelineContract):
    """Implements the activity_chembl pipeline contract."""

    id_column: str | None = "activity_id"
    pipeline_code: str = "activity_chembl"

    def __init__(
        self,
        config,
        run_id: str,
        *,
        client_factory: Callable[[Any], ChemblActivityClient] | None = None,
    ) -> None:
        super().__init__(config, run_id)
        self.client_factory = client_factory or default_activity_client_factory
        self._descriptor: ChemblExtractionDescriptor | None = None
        self._schema_registry = self._build_schema_registry()
        self._release: str | None = None
        self.extract_metadata: MutableMapping[str, Any] = {}

    # Descriptor lifecycle -------------------------------------------------
    def build_descriptor(self) -> ChemblExtractionDescriptor:
        metadata = getattr(self.config, "metadata", {}) or {}
        ids = metadata.get("ids") if isinstance(metadata, Mapping) else None
        pagination = metadata.get("pagination") if isinstance(metadata, Mapping) else None
        batch_plan = BatchPlan(
            batch_size=(metadata.get("batch_size") if isinstance(metadata, Mapping) else None),
            chunk_size=(metadata.get("chunk_size") if isinstance(metadata, Mapping) else None),
        )
        release = metadata.get("chembl_release") if isinstance(metadata, Mapping) else None
        descriptor = descriptor_from_options(
            ids=ids,
            pagination=pagination if isinstance(pagination, dict) else None,
            batch_size=batch_plan.batch_size,
            chunk_size=batch_plan.chunk_size,
            release=release,
        )
        self._descriptor = descriptor
        return descriptor

    def resolve_chembl_release(self, config) -> tuple[str | None, dict]:  # type: ignore[override]
        metadata = getattr(config, "metadata", {}) or {}
        if isinstance(metadata, Mapping) and metadata.get("chembl_release"):
            return metadata.get("chembl_release"), {"source": "config"}
        if self._release:
            return self._release, {"source": "cached"}
        return None, {"source": "unknown"}

    # Orchestration hooks --------------------------------------------------
    def prepare_run(self, options: StageExecutionOptions) -> None:
        self._descriptor = self._descriptor or self.build_descriptor()
        determinism = {"sort": {"by": list(ActivityColumns)}}
        metadata = getattr(self.config, "metadata", {}) if hasattr(self.config, "metadata") else {}
        if isinstance(metadata, dict):
            metadata.setdefault("determinism", determinism)
            metadata.setdefault("fail_on_schema_drift", options.fail_on_schema_drift)
            self.config.metadata = metadata  # type: ignore[attr-defined]

    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._release and "chembl_release" not in df.columns:
            df = df.assign(chembl_release=self._release)
        return df

    # Stage implementations -----------------------------------------------
    def extract(self, descriptor: Any, options: StageExecutionOptions) -> pd.DataFrame:
        descriptor = self._descriptor or self.build_descriptor()
        df, meta = self.run_descriptor_extraction(descriptor)
        self.extract_metadata.update(meta)
        return df

    def transform(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        parsed = self.pre_transform(df)
        enriched = self.domain_enrich(parsed)
        normalizer = ActivityNormalizer()
        return normalizer.normalize(enriched)

    def validate(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        return df

    def save_results(
        self, df: pd.DataFrame, artifacts: WriteArtifacts, options: StageExecutionOptions
    ) -> WriteResult:
        output_dir = artifacts.data_path.parent if artifacts.data_path else self.output_root
        run_stem = output_dir.name if artifacts.data_path else self.build_run_stem(options.run_tag, options.mode)
        logger = UnifiedLogger.get(self.__class__.__name__).bind(run_id=self.run_id)
        writer = UnifiedOutputWriter(
            output_dir=output_dir,
            pipeline_code=self.pipeline_code,
            run_stem=run_stem,
            schema_registry=self._schema_registry,
            config=self.config,
            logger=logger,
        )
        run_artifacts = RunArtifacts(output_dir=output_dir, logs_directory=self.logs_directory, write_artifacts=artifacts)
        write_result = writer.write_dataset_atomic(df, run_artifacts)
        qc_paths = emit_qc_artifact(df, run_artifacts)
        if qc_paths:
            artifacts.quality_report_path = qc_paths.get("quality_report")
        return write_result

    def finalize_run(self, run_result: RunResult) -> None:
        run_result.metadata.update({
            "chembl_release": self._release,
            "extract_metadata": dict(self.extract_metadata),
        })

    # Extraction helpers ---------------------------------------------------
    def run_descriptor_extraction(
        self, descriptor: ChemblExtractionDescriptor, *, batch_size: int | None = None
    ) -> tuple[pd.DataFrame, dict]:
        effective_batch_size = batch_size or (descriptor.batch_plan.batch_size if descriptor.batch_plan else 20)
        if effective_batch_size is None:
            effective_batch_size = 20
        if effective_batch_size > 25:
            raise ConfigValidationError("batch_size must not exceed 25 for ChEMBL API")

        client = self.client_factory(self.config)
        status = client.status()
        if isinstance(status, Mapping):
            self._release = str(status.get("chembl_release")) if status.get("chembl_release") else None
        meta: dict[str, Any] = {"chembl_release": self._release, "status": status}

        ids = descriptor.ids or []
        batches = [ids[i : i + effective_batch_size] for i in range(0, len(ids), effective_batch_size)]
        if not batches:
            batches = [None]

        parser = ActivityParser()
        frames: list[pd.DataFrame] = []
        failures = 0
        for batch in batches:
            if batch is None:
                continue
            try:
                payload = client.fetch_by_ids(batch)
                batch_frames: list[pd.DataFrame] = []
                for raw in payload.values():
                    batch_frames.append(parser.parse(raw))
                frame = pd.concat(batch_frames, ignore_index=True) if batch_frames else pd.DataFrame()
                frames.append(frame)
            except Exception as exc:  # pragma: no cover - defensive
                failures += 1
                frames.append(self._fallback_rows(batch, exc))

        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if self._release:
            df = df.assign(chembl_release=self._release)
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

    # Schema registry ------------------------------------------------------
    def _build_schema_registry(self) -> SchemaRegistry:
        registry = SchemaRegistry()
        registry.register(
            SchemaRegistryEntry(
                identifier=self.pipeline_code,
                schema=ActivitySchema,
                version="1.0.0",
                column_order=ActivityColumns,
                determinism=None,
                business_key_fields=("activity_id",),
                row_hash_fields=ActivityColumns,
                required_fields=ActivityColumns,
            )
        )
        return registry

    # Deterministic outputs -----------------------------------------------
    def plan_run_artifacts(self, output_dir: Path, run_tag: str | None, mode: str | None):
        run_stem = self.build_run_stem(run_tag, mode)
        target_dir = output_dir / run_stem
        target_dir.mkdir(parents=True, exist_ok=True)
        dataset_name = f"activity_{run_stem}.csv"
        artifacts = WriteArtifacts(data_path=target_dir / dataset_name)
        return target_dir, artifacts



def _registered_pipeline_factory() -> ChemblActivityPipeline:
    materialization = MaterializationConfig(root=Path("/tmp/chembl_activity"))
    config = PipelineConfig(pipeline=PipelineInfo(name="activity_chembl"), materialization=materialization)
    return ChemblActivityPipeline(config, run_id="dev")


register_pipeline("activity", _registered_pipeline_factory)

__all__ = ["ChemblActivityPipeline"]
