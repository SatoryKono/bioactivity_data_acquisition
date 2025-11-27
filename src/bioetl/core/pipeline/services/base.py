from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pandas as pd

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
        """Return an empty DataFrame with the correct schema."""

    def validate(
        self,
        df: pd.DataFrame,
        *,
        pipeline: PipelineBaseProtocol,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        """
        Validate the DataFrame against the schema.

        Args:
            df: The DataFrame to validate.
            pipeline: The pipeline instance (for context).
            options: Execution options.

        Returns:
            The validated (and potentially sorted/coerced) DataFrame.
        """


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
        """
        Save the DataFrame to artifacts.

        Args:
            df: The DataFrame to save.
            artifacts: The write artifacts definition.
            options: Execution options.
            context: Stage context.
            runtime: Runtime context.

        Returns:
            The result of the write operation.
        """

    def write_metadata(
        self,
        output_dir: Path,
        artifacts: WriteArtifacts,
        df: pd.DataFrame | None,
        *,
        dry_run: bool,
    ) -> None:
        """
        Write metadata sidecars (meta.yaml).

        Args:
            output_dir: Directory to write to.
            artifacts: Artifacts definition.
            df: The DataFrame associated with the artifacts (optional).
            dry_run: Whether to perform a dry run.
        """
