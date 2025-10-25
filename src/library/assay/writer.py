"""Output writer for assay ETL artefacts."""

from __future__ import annotations

from pathlib import Path

from library.common.writer_base import AssayETLWriter, ETLResult

from .config import AssayConfig


def write_assay_outputs(
    result: ETLResult,
    output_dir: Path,
    date_tag: str,
    config: AssayConfig,
) -> dict[str, Path]:
    """Persist ETL artefacts to disk and return the generated paths."""

    # Use the new unified ETL writer
    writer = AssayETLWriter(config, "assays")
    return writer.write_outputs(result, output_dir, date_tag)


__all__ = [
    "write_assay_outputs",
]
