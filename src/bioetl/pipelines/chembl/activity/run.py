from __future__ import annotations

"""ChEMBL activity pipeline scaffold with descriptor-driven extraction."""

from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping

import pandas as pd

from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.clients.factories.default_chembl_factory import default_activity_client_factory
from bioetl.core.io.artifacts import SchemaRegistry, SchemaRegistryEntry, WriteArtifacts
from bioetl.core.pipeline.services import WriteService
from bioetl.core.pipeline.unified import UnifiedPipelineBase
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
    descriptor_from_options,
)
from bioetl.application.pipelines.chembl.stage_runner import register_pipeline
from bioetl.pipelines.chembl.activity.stages import ActivityExtractor, ActivityTransformer, ActivityWriter
from bioetl.schemas.activity_schema import ActivityColumns, ActivitySchema


class ActivityWriteService(WriteService):
    def __init__(self, writer: ActivityWriter) -> None:
        self.writer = writer

    def save(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
        *,
        context,
    ) -> WriteResult:
        output_dir = artifacts.data_path.parent if artifacts.data_path else context.output_dir
        run_stem = (
            output_dir.name if artifacts.data_path else context.pipeline.build_run_stem(options.run_tag, options.mode)
        )
        return self.writer.write(df, artifacts, run_stem=run_stem, output_dir=output_dir)


class ChemblActivityPipeline(UnifiedPipelineBase, ChemblPipelineContract):
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
        super().__init__(config, run_id=run_id)
        self.client_factory = client_factory or default_activity_client_factory
        self._descriptor: ChemblExtractionDescriptor | None = None
        self._schema_registry = self._build_schema_registry()
        self._release: str | None = None
        self.extract_metadata: MutableMapping[str, Any] = {}
        self.extractor = ActivityExtractor(client_factory=self.client_factory)
        self.transformer = ActivityTransformer()
        self.writer = ActivityWriter(
            schema_registry=self._schema_registry,
            config=config,
            pipeline_code=self.pipeline_code,
            run_id=self.run_id,
            output_root=self.output_root,
            logs_directory=self.logs_directory,
        )
        self.write_service = ActivityWriteService(self.writer)

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
        return df

    # Stage implementations -----------------------------------------------
    def extract(self, descriptor: Any, options: StageExecutionOptions) -> pd.DataFrame:
        descriptor = self._descriptor or self.build_descriptor()
        df, meta = self.run_descriptor_extraction(descriptor)
        self.extract_metadata.update(meta)
        return df

    def transform(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        parsed = self.pre_transform(df)
        self.transformer.release = self._release
        return self.transformer.transform(parsed)

    def validate(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        return df

    def save_results(
        self, df: pd.DataFrame, artifacts: WriteArtifacts, options: StageExecutionOptions
    ) -> WriteResult:
        output_dir = artifacts.data_path.parent if artifacts.data_path else self.output_root
        run_stem = output_dir.name if artifacts.data_path else self.build_run_stem(options.run_tag, options.mode)
        return self.writer.write(df, artifacts, run_stem=run_stem, output_dir=output_dir)

    def finalize_run(self, run_result: RunResult) -> None:
        run_result.metadata.update({
            "chembl_release": self._release,
            "extract_metadata": dict(self.extract_metadata),
        })

    # Extraction helpers ---------------------------------------------------
    def run_descriptor_extraction(
        self, descriptor: ChemblExtractionDescriptor, *, batch_size: int | None = None
    ) -> tuple[pd.DataFrame, dict]:
        df, meta = self.extractor.extract(self.config, descriptor, batch_size=batch_size)
        self._release = self.extractor.release or self._release
        return df, meta

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
