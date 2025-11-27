from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Protocol, runtime_checkable

import pandas as pd

from bioetl.core.io.artifacts import RunArtifacts, WriteArtifacts
from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.types import WriteResult


@runtime_checkable
class UnifiedOutputWriter(Protocol):
    """Protocol for unified output writer."""

    output_dir: Path

    def write_dataset_atomic(
        self, df: pd.DataFrame, artifacts: RunArtifacts, format: str = "csv"
    ) -> WriteResult: ...

    def write(
        self,
        df: pd.DataFrame,
        artifacts: RunArtifacts,
        run_stem: str,
        output_dir: Path,
    ) -> WriteResult: ...


class PipelineOutputService:
    """Service for handling pipeline output writing operations."""

    def __init__(
        self,
        config: Mapping[str, Any] | None = None,
        logger: UnifiedLogger | None = None,
    ) -> None:
        self.config = config or {}
        self.logger = logger or UnifiedLogger.get(__name__)

    def resolve_writer(self, output_dir: Path) -> UnifiedOutputWriter | None:
        """Resolve and configure the unified writer from config."""
        io_cfg = (
            self.config.get("io") if isinstance(self.config, Mapping) else None
        )
        if isinstance(io_cfg, Mapping):
            writer = io_cfg.get("writer")
            if writer is not None:
                if hasattr(writer, "output_dir"):
                    writer.output_dir = output_dir
                return writer  # type: ignore
        return None

    def save(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        output_dir: Path,
        format: str = "csv",
    ) -> WriteResult:
        """
        Save dataset using configured writer and emit QC artifacts.

        Raises:
            RuntimeError: If no unified writer is configured.
        """
        writer = self.resolve_writer(output_dir)
        if not writer:
            raise RuntimeError("No unified writer configured")

        run_artifacts = RunArtifacts(
            output_dir=output_dir,
            logs_directory=output_dir / "logs",
            write_artifacts=artifacts,
        )

        try:
            if hasattr(writer, "write_dataset_atomic"):
                result = writer.write_dataset_atomic(
                    df, run_artifacts, format=format
                )
            else:
                result = writer.write(
                    df,
                    run_artifacts,
                    run_stem=output_dir.name,
                    output_dir=output_dir,
                )
        except Exception:
            self.logger.error("write_failed", exc_info=True)
            raise

        try:
            from bioetl.core.io.output import emit_qc_artifact

            emit_qc_artifact(df, run_artifacts)
        except Exception:
            self.logger.debug("emit_qc_failed", exc_info=True)

        return result
