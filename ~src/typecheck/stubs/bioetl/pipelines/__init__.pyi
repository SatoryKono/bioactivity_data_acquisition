from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from bioetl.config.models.models import CLIConfig
from bioetl.config.runtime import QCReportRuntimeOptions

class WriteResult:
    dataset: Path
    quality_report: Path | None
    metadata: Path | None
    correlation_report: Path | None
    qc_metrics: Path | None
    extras: dict[str, Path]

class WriteArtifacts:
    dataset: Path
    metadata: Path | None
    quality_report: Path | None
    correlation_report: Path | None
    qc_metrics: Path | None

class RunArtifacts:
    write: WriteArtifacts
    run_directory: Path
    manifest: Path | None
    log_file: Path
    extras: dict[str, Path]

class RunResult:
    write_result: WriteResult
    run_directory: Path
    manifest: Path | None
    additional_datasets: dict[str, Path]
    qc_summary: Path | None
    debug_dataset: Path | None
    run_id: str | None
    log_file: Path | None
    stage_durations_ms: dict[str, float]
    _dataset_path: Path | None
    _records: int | None
    _dataframe: pd.DataFrame | None

    @property
    def dataset_path(self) -> Path: ...
    @property
    def records(self) -> int: ...
    @property
    def dataframe(self) -> pd.DataFrame: ...

class PipelineExtractionMode(Enum):
    AUTO: PipelineExtractionMode
    BATCH: PipelineExtractionMode
    FULL: PipelineExtractionMode


class PipelineStagesProtocol(Protocol):
    def prepare_run(self) -> None: ...

    def extract(
        self,
        *,
        mode: PipelineExtractionMode = ...,
        ids: Sequence[str] | None = ...,
    ) -> pd.DataFrame: ...

    def transform(self, df: pd.DataFrame) -> pd.DataFrame: ...

    def validate(self, df: pd.DataFrame) -> pd.DataFrame: ...

    def save_results(
        self,
        df: pd.DataFrame,
        output_path: Path,
        *,
        extended: bool = ...,
        include_correlation: bool | None = ...,
        include_qc_metrics: bool | None = ...,
    ) -> RunResult: ...

    def finalize_run(self, result: RunResult | None) -> None: ...


class PipelineBase(PipelineStagesProtocol):
    config: Any
    run_id: str
    pipeline_code: str

    def extract(
        self,
        *,
        mode: PipelineExtractionMode = ...,
        ids: Sequence[str] | None = ...,
    ) -> pd.DataFrame: ...
    def transform(self, df: pd.DataFrame) -> pd.DataFrame: ...
    def validate(self, df: pd.DataFrame) -> pd.DataFrame: ...
    def save_results(
        self,
        df: pd.DataFrame,
        output_path: Path,
        *,
        extended: bool = ...,
        include_correlation: bool | None = ...,
        include_qc_metrics: bool | None = ...,
    ) -> RunResult: ...
    def write(
        self,
        df: pd.DataFrame,
        output_path: Path,
        *,
        extended: bool = ...,
        include_correlation: bool | None = ...,
        include_qc_metrics: bool | None = ...,
    ) -> RunResult: ...
    def run(
        self,
        output_path: Path,
        *args: Any,
        extended: bool = ...,
        include_correlation: bool | None = ...,
        include_qc_metrics: bool | None = ...,
        qc_reports: QCReportRuntimeOptions | None = ...,
        qc_thresholds: Mapping[str, float] | None = ...,
        fail_on_qc_violation: bool = ..., 
        **kwargs: Any,
    ) -> RunResult: ...

class ChemblActivityPipeline(PipelineBase): ...
class ChemblAssayPipeline(PipelineBase): ...
class ChemblDocumentPipeline(PipelineBase): ...
class ChemblTargetPipeline(PipelineBase): ...
class TestItemChemblPipeline(PipelineBase): ...

ActivityPipeline = ChemblActivityPipeline
AssayPipeline = ChemblAssayPipeline
DocumentPipeline = ChemblDocumentPipeline
TargetPipeline = ChemblTargetPipeline
TestItemPipeline = TestItemChemblPipeline
PipelineRunOptions = CLIConfig

__all__ = [
    "ActivityPipeline",
    "AssayPipeline",
    "ChemblActivityPipeline",
    "ChemblAssayPipeline",
    "ChemblDocumentPipeline",
    "ChemblTargetPipeline",
    "DocumentPipeline",
    "PipelineBase",
    "PipelineRunOptions",
    "RunArtifacts",
    "RunResult",
    "TargetPipeline",
    "TestItemChemblPipeline",
    "TestItemPipeline",
    "WriteArtifacts",
    "WriteResult",
]
