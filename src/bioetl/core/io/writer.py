"""Infrastructure helpers for deterministic artifact emission."""

from __future__ import annotations

import csv
import json
import os
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

import pandas as pd
import yaml

from bioetl.config.models.models import PipelineConfig
from bioetl.core.logging import LogEvents
from bioetl.tools import hash_file

from .determinism import CSVQuotingLiteral

__all__ = [
    "CSVQuotingLiteral",
    "build_run_manifest_payload",
    "emit_qc_artifact",
    "write_dataset_atomic",
    "write_frame_like",
    "write_json_atomic",
    "write_yaml_atomic",
]


def _csv_quoting(config: PipelineConfig) -> CSVQuotingLiteral:
    quoting_name = config.determinism.serialization.csv.quoting.upper()
    try:
        quote_value = getattr(csv, f"QUOTE_{quoting_name}")
        if not isinstance(quote_value, int):
            msg = f"Invalid CSV quoting constant: {quoting_name}"
            raise ValueError(msg)
        # Ensure it's one of the valid CSV quoting constants
        if quote_value not in (
            csv.QUOTE_ALL,
            csv.QUOTE_MINIMAL,
            csv.QUOTE_NONNUMERIC,
            csv.QUOTE_NONE,
        ):
            msg = f"Invalid CSV quoting constant value: {quote_value}"
            raise ValueError(msg)
        # Return validated quote value (guaranteed to be one of 0,1,2,3)
        return quote_value  # type: ignore[return-value]  # Literal type narrowing
    except AttributeError as exc:  # pragma: no cover - configuration error
        msg = f"Unsupported CSV quoting option: {quoting_name}"
        raise ValueError(msg) from exc


def write_dataset_atomic(
    df: pd.DataFrame, path: Path, *, config: PipelineConfig
) -> None:
    """Write ``df`` deterministically to ``path`` using an atomic replace."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    csv_config = config.determinism.serialization.csv
    float_format = f"%.{config.determinism.float_precision}f"
    quoting_value = _csv_quoting(config)
    with tmp_path.open("w", encoding="utf-8", newline="") as handle:
        df.to_csv(
            path_or_buf=handle,
            index=False,
            sep=csv_config.separator,
            na_rep=csv_config.na_rep,
            quoting=quoting_value,
            lineterminator="\n",
            float_format=float_format,
        )
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


def _write_mapping_atomic(
    payload: Mapping[str, Any],
    path: Path,
    serializer: Callable[[Mapping[str, Any], TextIO], None],
) -> None:
    """Persist ``payload`` using ``serializer`` via an atomic replace."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        serializer(payload, handle)
    os.replace(tmp_path, path)


def write_json_atomic(payload: Mapping[str, Any], path: Path) -> None:
    """Write ``payload`` as canonical JSON via an atomic replace."""

    _write_mapping_atomic(
        payload,
        path,
        lambda data, handle: json.dump(
            data,
            handle,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def write_yaml_atomic(payload: Mapping[str, Any], path: Path) -> None:
    """Persist ``payload`` as YAML using an atomic ``os.replace``."""

    _write_mapping_atomic(
        payload,
        path,
        lambda data, handle: yaml.safe_dump(
            data, handle, sort_keys=True, allow_unicode=True
        ),
    )


def _write_qc_metrics_csv(payload: Mapping[str, Any], path: Path, *, config: PipelineConfig) -> None:
    """Write QC metrics as single-row CSV with fixed column order per PROJECT_RULES."""
    import pandas as pd

    # Fixed column order per PROJECT_RULES QC contract
    columns = [
        "row_count",
        "duplicate_count",
        "duplicate_ratio",
        "deduplicated_count",
        "total_missing_values",
        "columns_with_missing",
        "units_distribution",
        "relation_distribution",
        "iqr_outliers",
        "business_key_fields"
    ]

    # Extract values in fixed order, convert to strings for CSV
    row_data = {}
    for col in columns:
        value = payload.get(col, "")
        # Convert complex types to string representation
        if isinstance(value, (dict, list)):
            value = str(value)
        elif value is None:
            value = ""
        elif isinstance(value, float):
            # Format floats to match golden file precision (6 decimal places)
            value = f"{value:.6f}"
        else:
            value = str(value)
        row_data[col] = value

    # Create single-row DataFrame and write as CSV
    df = pd.DataFrame([row_data])
    write_dataset_atomic(df, path, config=config)


def write_frame_like(
    frame_like: pd.DataFrame | Mapping[str, Any],
    path: Path,
    *,
    config: PipelineConfig,
) -> None:
    """Persist a DataFrame or mapping to ``path`` deterministically."""

    if isinstance(frame_like, pd.DataFrame):
        write_dataset_atomic(frame_like, path, config=config)
        return
    if isinstance(frame_like, Mapping):
        # Special case for qc_metrics - must be CSV per PROJECT_RULES
        if path.name.endswith("_qc.csv"):
            _write_qc_metrics_csv(frame_like, path, config=config)
            return
        write_yaml_atomic(frame_like, path)
        return
    msg = f"Unsupported frame-like type: {type(frame_like)}"
    raise TypeError(msg)


def emit_qc_artifact(
    frame: pd.DataFrame | Mapping[str, Any] | None,
    target_path: Path | None,
    *,
    config: PipelineConfig,
    log: Any,
    artifact_name: str,
) -> Path | None:
    """Persist an optional QC artefact and return the resolved path."""

    if frame is None or target_path is None:
        return None
    log.debug(
        LogEvents.WRITING_QC_ARTIFACT,
        artifact=artifact_name,
        path=str(target_path),
    )
    write_frame_like(frame, target_path, config=config)
    return target_path


def _relative_artifact_path(path: Path, base: Path) -> str:
    try:
        return path.relative_to(base).as_posix()
    except ValueError:
        return path.as_posix()


def _file_sha256(path: Path) -> str:
    return hash_file(path)


def build_run_manifest_payload(
    *,
    pipeline_code: str,
    run_id: str,
    run_directory: Path,
    dataset: Path,
    metadata: Path | None,
    quality_report: Path | None,
    correlation_report: Path | None,
    qc_metrics: Path | None,
    extras: Mapping[str, Path],
) -> dict[str, Any]:
    """Return manifest payload capturing deterministic artifact metadata."""

    def _entry(name: str, path: Path) -> dict[str, Any]:
        return {
            "name": name,
            "path": _relative_artifact_path(path, run_directory),
            "size_bytes": path.stat().st_size,
            "sha256": _file_sha256(path),
        }

    entries: list[dict[str, Any]] = []
    if dataset.exists():
        entries.append(_entry("dataset", dataset))
    if metadata and metadata.exists():
        entries.append(_entry("meta", metadata))
    if quality_report and quality_report.exists():
        entries.append(_entry("quality_report", quality_report))
    if correlation_report and correlation_report.exists():
        entries.append(_entry("correlation_report", correlation_report))
    if qc_metrics and qc_metrics.exists():
        entries.append(_entry("qc_metrics", qc_metrics))
    for extra_name, extra_path in sorted(extras.items()):
        if extra_path.exists():
            entries.append(_entry(f"extra:{extra_name}", extra_path))

    entries.sort(key=lambda item: (item["name"], item["path"]))

    return {
        "manifest_version": 1,
        "pipeline": pipeline_code,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "run_directory": run_directory.as_posix(),
        "artifacts": entries,
        "total_artifacts": len(entries),
    }
