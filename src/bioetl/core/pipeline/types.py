from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence, TYPE_CHECKING, Protocol, runtime_checkable
from contextlib import AbstractContextManager

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.config.runtime import QCReportRuntimeOptions
from bioetl.core.io import WriteResult
from bioetl.core.logging import pipeline_stage

if TYPE_CHECKING:
    from bioetl.core.pipeline.common import PipelineBaseCommon


class PipelineExtractionMode(str, Enum):
    """Enumerate high level extraction flows supported by pipelines."""

    AUTO = "auto"
    BATCH = "batch"
    FULL = "full"


@runtime_checkable
class PipelineStagesProtocol(Protocol):
    """Minimal protocol describing the public lifecycle hooks."""

    def prepare_run(self) -> None: ...

    def extract(
        self,
        *,
        mode: PipelineExtractionMode = PipelineExtractionMode.AUTO,
        ids: Sequence[str] | None = None,
    ) -> pd.DataFrame: ...

    def transform(self, df: pd.DataFrame) -> pd.DataFrame: ...

    def validate(self, df: pd.DataFrame) -> pd.DataFrame: ...

    def save_results(
        self,
        df: pd.DataFrame,
        output_path: Path,
        *,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
    ) -> "RunResult": ...

    def finalize_run(self, result: "RunResult" | None) -> None: ...


@dataclass(frozen=True)
class RunResult:
    """Final result of a pipeline execution."""

    write_result: WriteResult
    run_directory: Path
    manifest: Path | None = None
    additional_datasets: dict[str, Path] = field(default_factory=dict)
    qc_summary: Path | None = None
    debug_dataset: Path | None = None
    # Additional fields kept for backward compatibility
    run_id: str | None = None
    log_file: Path | None = None
    stage_durations_ms: dict[str, float] = field(default_factory=dict)
    _dataset_path: Path | None = None
    _records: int | None = None
    _dataframe: pd.DataFrame | None = None

    @property
    def dataset_path(self) -> Path:
        """Return the primary dataset path for backward compatibility."""

        return self._dataset_path or self.write_result.dataset

    @property
    def records(self) -> int:
        """Return the number of records materialised by the run."""

        if self._records is not None:
            return self._records
        if self._dataframe is not None:
            return int(self._dataframe.shape[0])
        return 0

    @property
    def dataframe(self) -> pd.DataFrame:
        """Return a copy of the dataframe produced by the run when available."""

        if self._dataframe is not None:
            return self._dataframe
        return pd.DataFrame()


@dataclass(frozen=True)
class StageExecutionOptions:
    """Options shared across pipeline stage commands."""

    extended: bool = False
    include_correlation: bool = False
    include_qc_metrics: bool = False
    qc_reports: QCReportRuntimeOptions | None = None
    qc_thresholds: Mapping[str, float] | None = None
    fail_on_qc_violation: bool = False
    extract_mode: PipelineExtractionMode = PipelineExtractionMode.AUTO
    extract_ids: Sequence[str] | None = None


@dataclass
class StageContext:
    """Mutable runtime context shared between stage commands."""

    pipeline: "PipelineBaseCommon"
    output_dir: Path
    options: StageExecutionOptions
    stage_durations_ms: dict[str, float]
    data: dict[str, Any] = field(default_factory=dict)
    result: RunResult | None = None

    def pipeline_stage(self, stage: str) -> AbstractContextManager[BoundLogger]:
        """Return structured logging context for ``stage``."""

        return pipeline_stage(
            stage,
            pipeline=self.pipeline.pipeline_code,
            run_id=self.pipeline.run_id,
            dataset=self.pipeline.pipeline_code,
            component=self.pipeline._component_for_stage(  # pylint: disable=protected-access
                stage
            ),
            logger_name=__name__,
        )

    def set_payload(self, key: str, value: Any) -> None:
        self.data[key] = value

    def require_payload(self, key: str) -> Any:
        if key not in self.data:
            msg = f"Stage context missing payload '{key}'"
            raise KeyError(msg)
        return self.data[key]

    def set_result(self, result: RunResult) -> None:
        self.result = result


class PipelineStageCommand(Protocol):
    """Interface implemented by all pipeline stage commands."""

    name: str

    def should_run(self, options: StageExecutionOptions) -> bool: ...

    def execute(self, context: StageContext) -> None: ...


__all__ = [
    "PipelineExtractionMode",
    "PipelineStagesProtocol",
    "PipelineStageCommand",
    "RunResult",
    "StageContext",
    "StageExecutionOptions",
]
