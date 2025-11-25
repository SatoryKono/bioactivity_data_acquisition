from __future__ import annotations

"""Runtime services used by pipeline stage plans."""

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pandas as pd
import pandera as pa

from bioetl.core.io import ArtifactWriter
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    StageContextProtocol,
    StageExecutionOptions,
    StageRuntimeContext,
    WriteArtifacts,
    WriteResult,
)


class ValidationService(Protocol):
    """Protocol for validating DataFrame results."""

    def empty_frame(self) -> pd.DataFrame:
        ...

    def validate(
        self,
        df: pd.DataFrame,
        *,
        pipeline: PipelineBaseProtocol,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        ...


class WriteService(Protocol):
    """Protocol for persisting transformed data."""

    def save(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
        *,
        context: StageContextProtocol,
        runtime: StageRuntimeContext,
    ) -> WriteResult:
        ...

    def write_metadata(
        self, output_dir: Path, artifacts: WriteArtifacts, df: pd.DataFrame | None, *, dry_run: bool
    ) -> None:
        ...


def _sort_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    columns = sorted(df.columns)
    if not columns:
        return df.reset_index(drop=True)
    return df.loc[:, columns].sort_values(by=columns).reset_index(drop=True)


@dataclass(slots=True)
class DefaultValidationService:
    """Validates dataframes using Pandera schemas and pipeline hooks."""

    validator: pa.DataFrameSchema | None = None

    def empty_frame(self) -> pd.DataFrame:
        if self.validator is None:
            return pd.DataFrame()
        columns = {name: pd.Series(dtype=str(schema.dtype)) for name, schema in self.validator.columns.items()}
        return pd.DataFrame(columns)

    def validate(
        self,
        df: pd.DataFrame,
        *,
        pipeline: PipelineBaseProtocol,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        frame = df if self.validator is None else self.validator.validate(df)
        validate_hook = getattr(pipeline, "validate", None)
        if callable(validate_hook):
            frame = validate_hook(frame, options)
        return _sort_dataframe(frame)


@dataclass(slots=True)
class DefaultWriteService:
    """Deterministic writer backed by :class:`ArtifactWriter`."""

    artifact_writer: ArtifactWriter

    def save(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
        *,
        context: StageContextProtocol,
        runtime: StageRuntimeContext,
    ) -> WriteResult:
        output_dir = artifacts.data_path.parent if artifacts.data_path else runtime.attributes.get("output_dir")
        return self.artifact_writer.write(
            df,
            artifacts,
            output_dir=output_dir,
            dry_run=options.dry_run,
            extended=options.extended,
        )

    def write_metadata(
        self, output_dir: Path, artifacts: WriteArtifacts, df: pd.DataFrame | None, *, dry_run: bool
    ) -> None:
        self.artifact_writer._write_metadata(output_dir, artifacts, df, dry_run=dry_run)


def default_validation_service_factory(pipeline: PipelineBaseProtocol) -> ValidationService:
    return DefaultValidationService(getattr(pipeline, "validator", None))


def default_write_service_factory(pipeline: PipelineBaseProtocol) -> WriteService:
    artifact_writer = ArtifactWriter(
        pipeline_code=pipeline.pipeline_code,
        run_id=pipeline.run_id,
        git_commit=getattr(pipeline, "_git_commit", None),
        config_hash=getattr(pipeline, "_config_hash", None),
    )
    return DefaultWriteService(artifact_writer)


__all__ = [
    "DefaultValidationService",
    "DefaultWriteService",
    "ValidationService",
    "WriteService",
    "default_validation_service_factory",
    "default_write_service_factory",
]
