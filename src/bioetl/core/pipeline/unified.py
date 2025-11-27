"""Unified pipeline implementations."""
from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Mapping,
    Sequence,
    TypeVar,
)

import pandas as pd
import pandera as pa
from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.definition import PipelineDefinition
from bioetl.core.pipeline.runtime import PipelineRuntimeBase
from bioetl.core.pipeline.services import (
    ArtifactRuntimeService,
    ValidationService,
    WriteService,
    default_validation_service_factory,
    default_write_service_factory,
)
from bioetl.core.pipeline.stage_plan import (
    StagePlanMetadata,
    build_default_stage_plan,
)
from bioetl.core.pipeline.types import (
    ArtifactStore,
    DataBucket,
    RunResult,
    StageContext,
    StageDescriptor,
    StageExecutionOptions,
    StageRuntimeContext,
    WriteArtifacts,
    WriteResult,
)

if TYPE_CHECKING:
    from bioetl.pipelines.chembl.common.chembl_extraction_service import (
        ChemblExtractionService,
    )


@dataclass(slots=True)
class BatchExtractionStats:
    """Сводные статистики по батчевой выборке."""

    rows: int
    api_calls: int
    cache_hits: int
    success_count: int
    fallback_count: int
    error_count: int
    duration_seconds: float


class PipelineBase(PipelineRuntimeBase):
    """Интерфейс стадий пайплайна."""

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str | None = None,
        validator: pa.DataFrameSchema | None = None,
        validation_service_factory: (
            Callable[[PipelineRuntimeBase], ValidationService] | None
        ) = None,
        write_service_factory: (
            Callable[[PipelineRuntimeBase], WriteService] | None
        ) = None,
        pipeline_definition: PipelineDefinition | None = None,
        artifact_runtime_service_factory: (
            Callable[[PipelineRuntimeBase], ArtifactRuntimeService] | None
        ) = None,
    ) -> None:
        super().__init__(
            config,
            pipeline_definition,
            run_id=run_id,
            validator=validator,
            validation_service_factory=(
                validation_service_factory
                or default_validation_service_factory
            ),
            write_service_factory=(
                write_service_factory
                or default_write_service_factory
            ),
            artifact_runtime_service_factory=artifact_runtime_service_factory,
        )

    @abstractmethod
    def extract(
        self,
        descriptor: "ChemblExtractionDescriptor | None",
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def transform(
        self,
        df: pd.DataFrame,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        ...

    def validate(
        self,
        df: pd.DataFrame,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        if not options.enable_validation:
            return df

        if (
            self.validator is not None
            and self.validation_service is not None
        ):
            return self.validation_service.validate(
                df, pipeline=self, options=options
            )

        return df

    def save_results(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
    ) -> WriteResult:
        if self.write_service is None:
            msg = (
                "write_service is not configured; "
                "provide write_service_factory or "
                "override save_results"
            )
            raise NotImplementedError(msg)

        output_dir = (
            artifacts.data_path.parent
            if artifacts.data_path
            else self.output_root
        )
        stage_context = self.context_builder.build(
            logger=UnifiedLogger.get(self.__class__.__name__).bind(
                run_id=self.run_id,
                pipeline=self.pipeline_code,
            ),
            output_dir=output_dir,
            data_bucket=DataBucket(),
            artifact_store=ArtifactStore(artifacts),
            metadata_service=self.metadata_service,
            qc_orchestrator=self.qc_orchestrator,
        )
        runtime_context = StageRuntimeContext(
            context=stage_context,
            options=options,
        )

        return self.write_service.save(
            df,
            artifacts,
            options,
            context=stage_context,
            runtime=runtime_context,
        )

    @property
    def pipeline_name(self) -> str:
        """Return the pipeline name (code)."""
        return self.pipeline_code

    # Hooks ---------------------------------------------------------------
    # pragma: no cover - optional hook
    def prepare_run(
        self,
        options: StageExecutionOptions,
    ) -> None:
        """Вызывается перед началом extract."""

    def finalize_run(  # pragma: no cover
        self,
        run_result: RunResult,
    ) -> None:
        """Вызывается после завершения write."""


class UnifiedPipelineBase(PipelineBase):
    """Базовая реализация общего жизненного цикла ETL."""

    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        metadata = StagePlanMetadata(
            dry_run=options.dry_run,
            has_validator=self.validator is not None,
            extended=options.extended,
        )
        return tuple(build_default_stage_plan(context.descriptor, metadata))


ChemblPipelineT = TypeVar("ChemblPipelineT", bound="ChemblPipelineBase")


class ChemblExtractionDescriptor(Generic[ChemblPipelineT]):
    """Описание извлечения сущности ChEMBL."""

    def __init__(
        self,
        *,
        build_context: Callable[[ChemblPipelineT], Mapping[str, Any]],
        fetcher_factory: Callable[
            [Mapping[str, Any]],
            Callable[[Sequence[str] | None], Any],
        ],
        finalizer_factory: Callable[
            [Mapping[str, Any]],
            Callable[[pd.DataFrame], pd.DataFrame],
        ],
    ) -> None:
        self.build_context = build_context
        self.fetcher_factory = fetcher_factory
        self.finalizer_factory = finalizer_factory


class ChemblPipelineBase(UnifiedPipelineBase):
    """Базовый пайплайн для ChEMBL с делегированием доменной логики сервису."""

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str | None = None,
        extraction_service: "ChemblExtractionService" | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(config, run_id=run_id, **kwargs)
        if extraction_service:
            self.extraction_service = extraction_service
        else:
            self._init_extraction_service()

    def _init_extraction_service(self) -> None:
        """Initialize extraction service based on config."""
        # pylint: disable=import-outside-toplevel
        from bioetl.pipelines.chembl.common import chembl_extraction_service

        extraction_service = (
            chembl_extraction_service.ChemblExtractionService()
        )
        self.extraction_service = extraction_service

    @property
    def chembl_release(self) -> str | None:
        """Return the ChEMBL release version."""
        return self.extraction_service.chembl_release

    def resolve_chembl_release(
        self, chembl_client: Any  # noqa: ARG002
    ) -> str:
        """Resolve the ChEMBL release version using the client."""
        return self.extraction_service.resolve_chembl_release(chembl_client)

    def run_descriptor_extraction(
        self,
        descriptor: ChemblExtractionDescriptor[ChemblPipelineT],
        ids: Sequence[str] | None,
        *,
        summary_event: str,
        metadata_filters: Mapping[str, Any] | None = None,
        fetch_mode: str = "default",
        **batch_kwargs: Any,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        """Run descriptor extraction with proper logging and error handling."""
        return self.extraction_service.run_descriptor_extraction(
            self,
            descriptor,
            ids,
            summary_event=summary_event,
            metadata_filters=metadata_filters,
            fetch_mode=fetch_mode,
            **batch_kwargs,
        )
