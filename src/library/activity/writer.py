"""Output writer for activity ETL artefacts (assay-aligned)."""

from __future__ import annotations

from pathlib import Path

from library.common.pipeline_base import ETLResult
from library.common.writer_base import ActivityETLWriter

from .config import ActivityConfig


def write_activity_outputs(
    *,
    result: ETLResult,
    output_dir: Path,
    date_tag: str,
    config: ActivityConfig,
    logger=None,
) -> dict[str, Path]:
    """Persist ETL artefacts and return generated file paths."""
    
    # Use the new unified ETL writer
    writer = ActivityETLWriter(config, "activities")
    return writer.write_outputs(result, output_dir, date_tag)


__all__ = ["write_activity_outputs"]


