"""Deterministic output helpers used by :class:`PipelineBase`."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from .artifacts import RunArtifacts, WriteArtifacts, WriteResult
from .determinism import (
    CSVQuotingLiteral,
    DeterministicWriteArtifacts,
    build_write_artifacts,
    ensure_hash_columns,
    prepare_dataframe,
    serialise_metadata,
)
from .finalize_output import finalize_output
from .writer import (
    build_run_manifest_payload,
    emit_qc_artifact,
    write_dataset_atomic,
    write_frame_like,
    write_json_atomic,
    write_yaml_atomic,
)

if TYPE_CHECKING:
    from infrastructure.config.models.models import PipelineConfig

__all__ = [
    "CSVQuotingLiteral",
    "DeterministicWriteArtifacts",
    "RunArtifacts",
    "WriteArtifacts",
    "WriteResult",
    "build_run_manifest_payload",
    "build_write_artifacts",
    "emit_qc_artifact",
    "ensure_hash_columns",
    "finalize_output",
    "plan_run_artifacts",
    "prepare_dataframe",
    "serialise_metadata",
    "write_dataset_atomic",
    "write_frame_like",
    "write_json_atomic",
    "write_yaml_atomic",
]


def plan_run_artifacts(
    stem: str,
    *,
    run_directory: Path,
    logs_directory: Path,
    dataset_extension: str,
    qc_extension: str,
    manifest_extension: str,
    log_extension: str,
    include_correlation: bool = False,
    include_qc_metrics: bool = False,
    include_metadata: bool = False,
    include_manifest: bool = False,
    extras: dict[str, Path] | None = None,
) -> RunArtifacts:
    """Return the artifact map for a deterministic run."""

    dataset = run_directory / f"{stem}.{dataset_extension}"
    quality = run_directory / f"{stem}_quality_report.{qc_extension}"
    correlation = (
        run_directory / f"{stem}_correlation_report.{qc_extension}"
        if include_correlation
        else None
    )
    qc_metrics = (
        run_directory / f"{stem}_qc.{qc_extension}"
        if include_qc_metrics
        else None
    )
    metadata = run_directory / f"{stem}_meta.yaml" if include_metadata else None
    manifest = (
        run_directory / f"{stem}_run_manifest.{manifest_extension}"
        if include_manifest
        else None
    )
    log_file = logs_directory / f"{stem}.{log_extension}"

    write_artifacts = WriteArtifacts(
        dataset=dataset,
        metadata=metadata,
        quality_report=quality,
        correlation_report=correlation,
        qc_metrics=qc_metrics,
    )
    extras_map: dict[str, Path] = dict(extras) if extras is not None else {}
    return RunArtifacts(
        write=write_artifacts,
        run_directory=run_directory,
        manifest=manifest,
        log_file=log_file,
        extras=extras_map,
    )
