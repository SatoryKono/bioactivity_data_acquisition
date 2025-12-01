"""Default service implementations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pandera.pandas as pa

from bioetl.core.io.writer import ArtifactWriter
from bioetl.core.pipeline.services.base import ValidationService, WriteService
from bioetl.core.io.artifacts import WriteArtifacts
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    StageContextProtocol,
    StageExecutionOptions,
    StageRuntimeContext,
    WriteResult,
)


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
        """Create an empty DataFrame based on the schema."""
        if self.validator is None:
            return pd.DataFrame()
        columns = {
            name: pd.Series(dtype=str(schema.dtype))
            for name, schema in self.validator.columns.items()
        }
        return pd.DataFrame(columns)

    def validate(
        self,
        df: pd.DataFrame,
        *,
        pipeline: PipelineBaseProtocol,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        """
        Validate DataFrame against the internal Pandera validator.

        Sorts columns and rows for determinism after validation.
        """
        _ = (pipeline, options)
        frame = df if self.validator is None else self.validator.validate(df)
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
        """Save DataFrame using ArtifactWriter."""
        _ = context
        any_artifacts = cast(Any, artifacts)
        runtime_context = cast(Any, runtime)
        output_dir = (
            any_artifacts.data_path.parent
            if any_artifacts.data_path
            else runtime_context.context.output_dir
        )
        return cast(
            WriteResult,
            self.artifact_writer.write(
                df,
                artifacts,
                output_dir=output_dir,
                dry_run=options.dry_run,
                extended=options.extended,
            ),
        )

    def write_metadata(
        self,
        output_dir: Path,
        artifacts: WriteArtifacts,
        df: pd.DataFrame | None,
        *,
        dry_run: bool,
    ) -> None:
        """Write metadata sidecars using ArtifactWriter."""
        self.artifact_writer.write_metadata(
            output_dir,
            artifacts,
            df,
            dry_run=dry_run,
        )


def default_validation_service_factory(
    pipeline: PipelineBaseProtocol,
) -> ValidationService:
    """Create a default validation service for the pipeline."""
    return DefaultValidationService(getattr(pipeline, "validator", None))


def default_write_service_factory(
    pipeline: PipelineBaseProtocol,
) -> WriteService:
    """Create a default write service using ArtifactWriter."""
    artifact_writer = ArtifactWriter(
        pipeline_code=pipeline.pipeline_code,
        run_id=getattr(pipeline, "run_id", ""),
        git_commit=getattr(pipeline, "_git_commit", None),
        config_hash=getattr(pipeline, "_config_hash", None),
    )
    return DefaultWriteService(artifact_writer)
