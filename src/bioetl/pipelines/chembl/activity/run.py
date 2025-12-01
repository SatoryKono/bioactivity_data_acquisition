"""ChEMBL activity pipeline scaffold with descriptor-driven extraction.

LEGACY CODE: This module contains the original ChemblActivityPipeline
implementation maintained for backward compatibility with existing tests.

TODO: Migrate test_run_smoke.py to use ChemblCommonPipeline instead of this
legacy code. Method signature mismatches and general exception handling are
intentional for legacy compatibility and should not be modified without
understanding the full inheritance hierarchy.

New code should use ChemblCommonPipeline from
bioetl.pipelines.chembl.common.base.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping, cast

import pandas as pd

from bioetl.core.config.models import ChemblPipelineMetadata
from bioetl.clients.chembl.entities import ChemblActivityClient
from bioetl.clients.chembl.factories import (
    default_activity_client_factory,
)
from bioetl.core.io import UnifiedOutputWriter
from bioetl.core.io.artifacts import (
    RunArtifacts,
    SchemaRegistry,
    SchemaRegistryEntry,
    WriteArtifacts,
)
from bioetl.core.pipeline.runtime import PipelineRuntimeBase
from bioetl.core.pipeline.services import (
    ArtifactPlanner,
    ArtifactRuntimeService,
    DefaultValidationService,
    WriteService,
    default_artifact_service_factory,
)
from bioetl.core.pipeline.stage_plan import (
    StagePlanMetadata,
    build_default_stage_plan,
)
from bioetl.core.pipeline.types import (
    MaterializationConfig,
    PipelineConfig,
    PipelineInfo,
    StageContextProtocol,
    StageDescriptor,
    StageExecutionOptions,
    StageRuntimeContext,
    WriteResult,
)
from bioetl.pipelines.chembl.common.descriptor import (
    BatchPlan,
    ChemblExtractionDescriptor,
    ChemblPipelineContract,
    descriptor_from_options,
)
from bioetl.pipelines.chembl.common.base import ChemblCommonPipeline
from bioetl.pipelines.chembl.runner import register_pipeline
from bioetl.pipelines.chembl.activity.stages import (
    ActivityExtractor,
    ActivityTransformer,
    ActivityWriter,
)
from bioetl.schemas.chembl_activity_schema import (
    ChEMBLActivitySchema,
    ChEMBLActivityColumns,
)


class ActivityWriteService(WriteService):
    """Service for writing activity pipeline results."""

    def __init__(self, writer: ActivityWriter) -> None:
        self.writer = writer

    def save(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
        *,
        context: StageContextProtocol,
        runtime: StageRuntimeContext,
    ) -> WriteResult:
        runtime_context = runtime.context or context
        pipeline = getattr(runtime_context, "pipeline", None)
        output_dir = (
            artifacts.data_path.parent
            if artifacts.data_path
            else runtime_context.output_dir
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        run_stem = (
            output_dir.name
            if artifacts.data_path
            else pipeline.build_run_stem(
                options.run_tag,
                options.mode,
            )
            if pipeline
            else output_dir.name
        )
        if artifacts.data_path is None:
            artifacts.data_path = output_dir / f"activity_{run_stem}.csv"
        return self.writer.write(
            df,
            artifacts,
            run_stem=run_stem,
            output_dir=output_dir,
        )


class ActivityArtifactPlanner(ArtifactPlanner):
    """Deterministic artifact planner for the activity pipeline."""

    def __init__(self, pipeline: PipelineRuntimeBase) -> None:
        self.pipeline = pipeline

    def plan(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        _ = pipeline_code
        run_stem = self.pipeline.build_run_stem(run_tag, mode)
        target_dir = output_dir / run_stem
        target_dir.mkdir(parents=True, exist_ok=True)
        dataset_name = f"activity_{run_stem}.csv"
        artifacts = WriteArtifacts(data_path=target_dir / dataset_name)
        return target_dir, artifacts


def activity_artifact_runtime_service_factory(
    pipeline: PipelineRuntimeBase,
) -> ArtifactRuntimeService:
    """Create an artifact runtime service with activity-specific planning."""

    planner = ActivityArtifactPlanner(pipeline)
    artifact_service = default_artifact_service_factory(planner)
    return ArtifactRuntimeService(
        artifact_planner=planner,
        artifact_service=artifact_service,
    )


class ChemblActivityPipeline(ChemblCommonPipeline, ChemblPipelineContract):
    """Implements the activity_chembl pipeline contract."""

    id_column: str | None = "activity_id"
    pipeline_code: str = "activity_chembl"

    def __init__(
        self,
        config: Mapping[str, Any] | PipelineConfig,
        run_id: str,
        *,
        client_factory: Callable[[Any], ChemblActivityClient] | None = None,  # type: ignore[name-defined]
    ) -> None:
        print(
            "DEBUG: ChemblActivityPipeline.__init__ called "
            "with updated schema"
        )
        super().__init__(  # type: ignore[arg-type]
            config,
            run_id=run_id,
            artifact_runtime_service_factory=(
                activity_artifact_runtime_service_factory
            ),
            custom_artifact_planner_factory=(
                lambda: ActivityArtifactPlanner(self)
            ),
            schema_registry_factory=self._build_schema_registry,
            descriptor_type="dataclass",
        )
        self.client_factory = client_factory or default_activity_client_factory
        self.validator = ChEMBLActivitySchema
        self.validation_service = DefaultValidationService(self.validator)
        self._descriptor: ChemblExtractionDescriptor | None = None
        self._release: str | None = None
        self.extract_metadata: MutableMapping[str, Any] = {}
        self.extractor = ActivityExtractor(client_factory=self.client_factory)
        self.transformer = ActivityTransformer()
        self.writer = ActivityWriter(
            schema_registry=(
                self._schema_registry_factory()
                if self._schema_registry_factory
                else self._build_schema_registry()
            ),
            config=config,
            pipeline_code=self.pipeline_code,
            run_id=self.run_id,
            output_root=self.output_root,
            logs_directory=self.logs_directory,
        )
        self.write_service = ActivityWriteService(self.writer)  # type: ignore[arg-type]

    # Descriptor lifecycle -------------------------------------------------
    def _get_config_metadata(
        self, config: Mapping[str, Any] | None = None
    ) -> ChemblPipelineMetadata:
        """Safely retrieve metadata from config (dict or object)."""
        cfg = config if config is not None else self.config
        if isinstance(cfg, Mapping):
            metadata = cfg.get("metadata") or {}
            if isinstance(metadata, Mapping):
                return cast(ChemblPipelineMetadata, dict(metadata))
        metadata = getattr(cfg, "metadata", {}) if cfg is not None else {}
        return cast(ChemblPipelineMetadata, dict(metadata))

    def build_descriptor(self) -> ChemblExtractionDescriptor:
        metadata = self._get_config_metadata()
        if isinstance(metadata, Mapping):
            ids = metadata.get("ids")
            pagination = metadata.get("pagination")
            batch_size = metadata.get("batch_size")
            chunk_size = metadata.get("chunk_size")
            release = metadata.get("chembl_release")
        else:
            ids = None
            pagination = None
            batch_size = None
            chunk_size = None
            release = None
        batch_plan = BatchPlan(
            batch_size=batch_size,
            chunk_size=chunk_size,
        )
        descriptor = descriptor_from_options(
            ids=ids,
            pagination=pagination if isinstance(pagination, dict) else None,
            batch_size=batch_plan.batch_size,
            chunk_size=batch_plan.chunk_size,
            release=release,
        )
        self._descriptor = descriptor
        return descriptor

    def resolve_chembl_release(self, config: Any) -> str:  # type: ignore[override]
        metadata = self._get_config_metadata(config)
        if isinstance(metadata, Mapping) and metadata.get("chembl_release"):
            return metadata.get("chembl_release")  # type: ignore[return-value]
        if self._release:
            return self._release
        return None  # type: ignore[return-value]

    # Orchestration hooks --------------------------------------------------
    def prepare_run(self, options: StageExecutionOptions) -> None:
        self._descriptor = self._descriptor or self.build_descriptor()
        determinism = {"sort": {"by": list(ChEMBLActivityColumns)}}
        metadata = self._get_config_metadata()

        if isinstance(metadata, dict):
            metadata.setdefault("determinism", determinism)
            metadata.setdefault(
                "fail_on_schema_drift",
                options.fail_on_schema_drift,
            )
            cfg = self.config
            if isinstance(cfg, MutableMapping):
                cfg["metadata"] = metadata
            elif hasattr(cfg, "metadata"):
                try:
                    cfg.metadata = metadata  # type: ignore[attr-defined]
                except (TypeError, ValueError):
                    pass  # Config is frozen/immutable

    # Stage implementations -----------------------------------------------
    def _extract_with_dataclass_descriptor(
        self,
        descriptor: ChemblExtractionDescriptor,
        options: StageExecutionOptions
    ) -> pd.DataFrame:
        """Extract data using dataclass descriptor pattern."""
        if options.dry_run:
            return pd.DataFrame(columns=list(ChEMBLActivityColumns))

        df, meta = self.run_descriptor_extraction(descriptor)
        self.extract_metadata.update(meta)
        return df

    def extract(
        self,
        descriptor: ChemblExtractionDescriptor | StageDescriptor | None,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        """Extract data based on descriptor."""
        extraction_descriptor = self._resolve_descriptor(descriptor)
        df = self._extract_with_dataclass_descriptor(
            extraction_descriptor,
            options,
        )
        # Apply CLI limit if specified
        if options.limit is not None and not df.empty:
            df = df.head(options.limit)
        return df

    def transform(
        self,
        df: pd.DataFrame,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        """Transform extracted data."""
        parsed = self.pre_transform(df)
        self.transformer.release = self._release
        return self.transformer.transform(parsed)

    def validate(
        self,
        df: pd.DataFrame,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        """Validate transformed data."""
        return df

    def save_results(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
    ) -> WriteResult:
        """Save pipeline results."""
        if artifacts.data_path is None:
            _, planned_artifacts = self.plan_run_artifacts(
                self.output_root,
                options.run_tag,
                options.mode,
            )
            artifacts.data_path = planned_artifacts.data_path

        # Create UnifiedOutputWriter directly with required parameters
        logger = UnifiedLogger.get(self.__class__.__name__).bind(
            run_id=self.run_id
        )
        writer = UnifiedOutputWriter(
            output_dir=self.output_root,
            pipeline_code=self.pipeline_code,
            run_stem=options.run_tag,
            schema_registry=self.writer.schema_registry,
            config=self.config,
            logger=logger,
        )

        run_artifacts = RunArtifacts(
            output_dir=self.output_root,
            logs_directory=self.logs_directory,
            write_artifacts=artifacts,
        )

        try:
            print(
                "DEBUG: Attempting UnifiedOutputWriter.write_dataset_atomic "
                f"with df shape {df.shape}"
            )
            result = writer.write_dataset_atomic(df, run_artifacts)
            print(
                "DEBUG: UnifiedOutputWriter.write_dataset_atomic "
                "succeeded"
            )
            return result
        except Exception as e:
            print(
                "DEBUG: UnifiedOutputWriter.write_dataset_atomic "
                f"failed: {e}"
            )
            # Fall through to legacy write_service
            return self.write_service.save(df, artifacts, options)

    def build_pipeline_metadata(
        self, context: StageContextProtocol | None = None
    ) -> Mapping[str, Any]:
        """Construct metadata for the executed pipeline run."""
        del context
        metadata: dict[str, Any] = {}
        metadata["extract_metadata"] = dict(self.extract_metadata)
        metadata["chembl_release"] = self._release
        return metadata

    # Extraction helpers ---------------------------------------------------
    def run_descriptor_extraction(  # type: ignore[override]
        self,
        descriptor: ChemblExtractionDescriptor,
        *,
        batch_size: int | None = None,
    ) -> tuple[pd.DataFrame, dict]:  # type: ignore[type-arg]
        df, meta = self.extractor.extract(  # type: ignore[arg-type]
            descriptor,
            self.config,
            batch_size=batch_size,
        )
        self._release = self.extractor.release or self._release
        return df, meta

    # Schema registry ------------------------------------------------------
    def _build_schema_registry(self) -> SchemaRegistry:
        registry = SchemaRegistry()
        # Debug: Check actual schema dtype before registration
        print(
            f"DEBUG: row_index dtype in schema: "
            f"{ChEMBLActivitySchema.columns['row_index'].dtype}"
        )
        registry.register(
            SchemaRegistryEntry(
                identifier=self.pipeline_code,
                schema=ChEMBLActivitySchema,
                version="1.0.0",
                column_order=ChEMBLActivityColumns,
                determinism=None,
                business_key_fields=("activity_id",),
                row_hash_fields=ChEMBLActivityColumns,
                required_fields=ChEMBLActivityColumns,
            )
        )
        return registry

    def build_stage_plan(
        self, context: StageContextProtocol, options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        """Inject prebuilt descriptor into the extract stage."""

        descriptor = self._descriptor or self.build_descriptor()
        metadata = StagePlanMetadata(
            dry_run=options.dry_run,
            has_validator=self.validator is not None,
            extended=options.extended,
        )
        plan = build_default_stage_plan(descriptor, metadata)
        patched_plan: list[StageDescriptor] = []
        for stage in plan:
            if stage.kind == "extract":
                patched_stage = StageDescriptor(
                    id=stage.id,
                    kind=stage.kind,
                    params={"descriptor": descriptor},
                    next=stage.next,
                )
                patched_plan.append(patched_stage)
            else:
                patched_plan.append(stage)
        return tuple(patched_plan)

    def _resolve_descriptor(
        self, descriptor: ChemblExtractionDescriptor | StageDescriptor | None
    ) -> ChemblExtractionDescriptor:
        if isinstance(descriptor, StageDescriptor):
            descriptor = cast(
                ChemblExtractionDescriptor,
                descriptor.params.get("descriptor"),
            )

        if descriptor is None:
            descriptor = self.build_descriptor()

        return descriptor


def _registered_pipeline_factory() -> ChemblActivityPipeline:
    print("DEBUG: _registered_pipeline_factory called")
    materialization = MaterializationConfig(
        root=Path("/tmp/chembl_activity"),
    )
    config = PipelineConfig(
        pipeline=PipelineInfo(name="activity_chembl"),
        materialization=materialization,
    )
    pipeline = ChemblActivityPipeline(config, run_id="dev")
    print(f"DEBUG: Created pipeline of type: {type(pipeline)}")
    return pipeline


register_pipeline("activity", _registered_pipeline_factory)

__all__ = ["ChemblActivityPipeline"]
