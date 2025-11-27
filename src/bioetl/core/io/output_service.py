from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Protocol, runtime_checkable

import pandas as pd

from bioetl.core.io.artifacts import RunArtifacts
from bioetl.core.io.output import emit_qc_artifact
from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.types import (
    StageExecutionOptions,
    WriteArtifacts,
    WriteResult,
)


@runtime_checkable
class Writer(Protocol):
    output_dir: Path | None

    def write_dataset_atomic(
        self, df: pd.DataFrame, artifacts: RunArtifacts, *, format: str = "csv"
    ) -> WriteResult:
        ...

    def write(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        *,
        run_stem: str,
        output_dir: Path,
    ) -> WriteResult:
        ...


class PipelineOutputService:
    """Унифицированное сохранение результатов пайплайна."""

    def __init__(self, config: Mapping[str, Any] | Any) -> None:
        self.config = config
        self.logger = UnifiedLogger.get(__name__)

    def resolve_writer(self, output_dir: Path) -> Writer:
        """Получить настроенный writer из конфигурации."""

        if isinstance(self.config, Mapping):
            io_cfg = self.config.get("io")
        else:
            io_cfg = getattr(self.config, "io", None)
        if not isinstance(io_cfg, Mapping):
            msg = "io.writer is not configured"
            raise ValueError(msg)

        writer = io_cfg.get("writer")
        if writer is None:
            msg = "io.writer is not configured"
            raise ValueError(msg)

        if hasattr(writer, "output_dir"):
            try:
                writer.output_dir = output_dir
            except Exception as exc:  # pragma: no cover - защитный механизм
                self.logger.warning(
                    "WRITER_OUTPUT_DIR_SET_FAILED",
                    output_dir=str(output_dir),
                    error=str(exc),
                )

        return writer

    def save(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
    ) -> WriteResult:
        output_dir = artifacts.data_path.parent if artifacts.data_path else Path.cwd()
        writer = self.resolve_writer(output_dir)

        run_artifacts = RunArtifacts(
            output_dir=output_dir,
            logs_directory=output_dir / "logs",
            write_artifacts=artifacts,
        )

        if hasattr(writer, "write_dataset_atomic"):
            result = writer.write_dataset_atomic(df, run_artifacts, format="csv")
        elif hasattr(writer, "write"):
            run_stem = (
                artifacts.data_path.stem
                if artifacts.data_path
                else options.run_tag
                or output_dir.name
            )
            result = writer.write(
                df,
                artifacts,
                run_stem=run_stem,
                output_dir=output_dir,
            )
        else:  # pragma: no cover - defensive branch
            msg = "Writer must implement write_dataset_atomic or write"
            raise AttributeError(msg)

        try:  # pragma: no cover - QC может быть выключен
            emit_qc_artifact(df, run_artifacts)
        except Exception as exc:
            self.logger.warning("QC_EMIT_SKIPPED", error=str(exc))

        return result


__all__ = ["PipelineOutputService", "Writer"]
