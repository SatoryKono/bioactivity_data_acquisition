"""Deterministic output helpers used by :class:`PipelineBase`."""

from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pandas as pd
import yaml

from bioetl.config import PipelineConfig

from .logger import UnifiedLogger

__all__ = [
    "DeterministicWriteArtifacts",
    "prepare_dataframe",
    "ensure_hash_columns",
    "write_dataset_atomic",
    "write_yaml_atomic",
    "serialise_metadata",
]


@dataclass(frozen=True, slots=True)
class DeterministicWriteArtifacts:
    """In-memory view of a dataset prepared for writing."""

    dataframe: pd.DataFrame
    metadata: Mapping[str, Any]


def _boolean_representation(value: bool, *, config: PipelineConfig) -> str:
    truthy, falsy = config.determinism.serialization.booleans
    return truthy if value else falsy


def _normalise_scalar(value: Any, *, config: PipelineConfig) -> str:
    serialization = config.determinism.serialization
    if pd.isna(value):
        return serialization.nan_rep
    if isinstance(value, bool):
        return _boolean_representation(value, config=config)
    if isinstance(value, (int, str)):
        return str(value)
    if isinstance(value, float):
        precision = config.determinism.float_precision
        return f"{value:.{precision}f}"
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, pd.Timestamp):
        ts = value.tz_convert("UTC") if value.tzinfo is not None else value.tz_localize("UTC")
        return ts.isoformat().replace("+00:00", "Z")
    if isinstance(value, (list, tuple)):
        return json.dumps([_normalise_scalar(item, config=config) for item in value], ensure_ascii=False, sort_keys=True)
    if isinstance(value, Mapping):
        normalised = {str(k): _normalise_scalar(v, config=config) for k, v in value.items()}
        return json.dumps(normalised, ensure_ascii=False, sort_keys=True)
    return str(value)


def _hash_fields(row: pd.Series, fields: Sequence[str], *, config: PipelineConfig) -> str:
    if not fields:
        return ""
    import hashlib

    algorithm = config.determinism.hashing.algorithm
    try:
        digest = hashlib.new(algorithm)
    except ValueError as exc:  # pragma: no cover - defensive, relies on hashlib API
        msg = f"Unsupported hash algorithm: {algorithm}"
        raise ValueError(msg) from exc

    serialised_values = []
    for field in fields:
        if field not in row:
            msg = f"Field '{field}' required for hashing is missing from dataframe"
            raise KeyError(msg)
        serialised_values.append(_normalise_scalar(row[field], config=config))

    joined = "\u001f".join(serialised_values)
    digest.update(joined.encode("utf-8"))
    return digest.hexdigest()


def ensure_hash_columns(df: pd.DataFrame, *, config: PipelineConfig) -> pd.DataFrame:
    """Return ``df`` with integrity hash columns populated."""

    hashing_config = config.determinism.hashing
    exclude = set(hashing_config.exclude_fields)
    if hashing_config.row_fields:
        row_fields = list(hashing_config.row_fields)
    else:
        row_fields = [col for col in df.columns if col not in exclude]
    business_fields = list(hashing_config.business_key_fields)

    if "hash_row" not in df.columns:
        df = df.assign(hash_row=df.apply(_hash_fields, axis=1, fields=row_fields, config=config))
    if business_fields and "hash_business_key" not in df.columns:
        df = df.assign(
            hash_business_key=df.apply(_hash_fields, axis=1, fields=business_fields, config=config)
        )
    return df


def _stable_sort(df: pd.DataFrame, *, config: PipelineConfig) -> pd.DataFrame:
    sort_config = config.determinism.sort
    if not sort_config.by:
        return df
    
    # Check if all sort columns exist in the DataFrame
    missing_columns = [col for col in sort_config.by if col not in df.columns]
    
    # If DataFrame is empty and columns are missing, skip sorting (empty DataFrame)
    if df.empty and missing_columns:
        return df
    
    # If DataFrame is not empty but columns are missing, raise error
    if missing_columns:
        msg = f"Sort columns missing from dataframe: {missing_columns}"
        raise KeyError(msg)
    
    ascending = sort_config.ascending or [True] * len(sort_config.by)
    return df.sort_values(
        by=sort_config.by,
        ascending=ascending,
        na_position=sort_config.na_position,
        kind="stable",
    ).reset_index(drop=True)


def _enforce_column_order(df: pd.DataFrame, *, config: PipelineConfig) -> pd.DataFrame:
    order = list(config.determinism.column_order)
    if not order:
        return df
    missing = [column for column in order if column not in df.columns]
    if missing:
        msg = f"Column order references missing columns: {missing}"
        raise ValueError(msg)
    extra = [column for column in df.columns if column not in order]
    return df[[*order, *extra]]


def prepare_dataframe(df: pd.DataFrame, *, config: PipelineConfig) -> pd.DataFrame:
    """Apply determinism rules (sort + column order) to ``df``."""

    prepared = _stable_sort(df, config=config)
    prepared = _enforce_column_order(prepared, config=config)
    return prepared


def _csv_quoting(config: PipelineConfig) -> int:
    quoting_name = config.determinism.serialization.csv.quoting.upper()
    try:
        return getattr(csv, f"QUOTE_{quoting_name}")
    except AttributeError as exc:  # pragma: no cover - configuration error
        msg = f"Unsupported CSV quoting option: {quoting_name}"
        raise ValueError(msg) from exc


def write_dataset_atomic(df: pd.DataFrame, path: Path, *, config: PipelineConfig) -> None:
    """Write ``df`` deterministically to ``path`` using an atomic replace."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    csv_config = config.determinism.serialization.csv
    float_format = f"%.{config.determinism.float_precision}f"
    df.to_csv(
        tmp_path,
        index=False,
        sep=csv_config.separator,
        na_rep=csv_config.na_rep,
        encoding="utf-8",
        quoting=_csv_quoting(config),
        lineterminator="\n",
        float_format=float_format,
    )
    os.replace(tmp_path, path)


def write_yaml_atomic(payload: Mapping[str, Any], path: Path) -> None:
    """Persist ``payload`` as YAML using an atomic ``os.replace``."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=True, allow_unicode=True)
    os.replace(tmp_path, path)


def write_frame_like(frame: pd.DataFrame | Mapping[str, Any], path: Path, *, config: PipelineConfig) -> None:
    """Write optional QC artefacts respecting deterministic CSV settings."""

    if isinstance(frame, pd.DataFrame):
        dataset = frame
    else:
        dataset = pd.DataFrame([frame])
    write_dataset_atomic(dataset, path, config=config)


def serialise_metadata(
    df: pd.DataFrame,
    *,
    config: PipelineConfig,
    run_id: str,
    pipeline_code: str,
    dataset_path: Path,
    stage_durations_ms: Mapping[str, float],
) -> Mapping[str, Any]:
    """Construct the base metadata payload for ``meta.yaml``."""

    hashing = config.determinism.hashing
    base_metadata: dict[str, Any] = {
        "pipeline": pipeline_code,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "dataset_path": str(dataset_path),
        "hash_policy_version": config.determinism.hash_policy_version,
        "stage_durations_ms": dict(stage_durations_ms),
        "sorting": {
            "by": list(config.determinism.sort.by),
            "ascending": list(config.determinism.sort.ascending or [True] * len(config.determinism.sort.by)),
            "na_position": config.determinism.sort.na_position,
        },
        "hashing": {
            "algorithm": hashing.algorithm,
            "row_fields": list(hashing.row_fields) if hashing.row_fields else [],
            "business_key_fields": list(hashing.business_key_fields),
        },
    }
    if "hash_row" in df.columns and not df.empty:
        base_metadata["hashing"]["sample_hash_row"] = str(df.iloc[0]["hash_row"])
    if "hash_business_key" in df.columns and not df.empty:
        base_metadata["hashing"]["sample_hash_business_key"] = str(df.iloc[0]["hash_business_key"])
    return base_metadata


def build_write_artifacts(
    df: pd.DataFrame,
    *,
    config: PipelineConfig,
    run_id: str,
    pipeline_code: str,
    dataset_path: Path,
    stage_durations_ms: Mapping[str, float],
) -> DeterministicWriteArtifacts:
    """Prepare dataframe and metadata for persistent storage."""

    prepared = prepare_dataframe(df, config=config)
    prepared = ensure_hash_columns(prepared, config=config)
    metadata = serialise_metadata(
        prepared,
        config=config,
        run_id=run_id,
        pipeline_code=pipeline_code,
        dataset_path=dataset_path,
        stage_durations_ms=stage_durations_ms,
    )
    return DeterministicWriteArtifacts(dataframe=prepared, metadata=metadata)


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
    log.debug("writing_qc_artifact", artifact=artifact_name, path=str(target_path))
    write_frame_like(frame, target_path, config=config)
    return target_path


def finalise_output(
    df: pd.DataFrame,
    *,
    config: PipelineConfig,
    run_id: str,
    pipeline_code: str,
    dataset_path: Path,
    metadata_path: Path,
    stage_durations_ms: Mapping[str, float],
    metadata_hook: Callable[[Mapping[str, Any], pd.DataFrame], Mapping[str, Any]] | None = None,
    quality_report: pd.DataFrame | Mapping[str, Any] | None = None,
    quality_path: Path | None = None,
    correlation_report: pd.DataFrame | Mapping[str, Any] | None = None,
    correlation_path: Path | None = None,
    qc_metrics: pd.DataFrame | Mapping[str, Any] | None = None,
    qc_metrics_path: Path | None = None,
) -> DeterministicWriteArtifacts:
    """Persist the dataset and optional QC artefacts."""

    log = UnifiedLogger.get(__name__)
    log.debug("building_write_artifacts", dataset=str(dataset_path))
    prepared = build_write_artifacts(
        df,
        config=config,
        run_id=run_id,
        pipeline_code=pipeline_code,
        dataset_path=dataset_path,
        stage_durations_ms=stage_durations_ms,
    )
    metadata = dict(prepared.metadata)
    if metadata_hook is not None:
        metadata = dict(metadata_hook(metadata, prepared.dataframe))

    log.debug("writing_dataset", path=str(dataset_path), rows=len(prepared.dataframe))
    write_dataset_atomic(prepared.dataframe, dataset_path, config=config)

    log.debug("writing_metadata", path=str(metadata_path))
    write_yaml_atomic(metadata, metadata_path)

    emit_qc_artifact(quality_report, quality_path, config=config, log=log, artifact_name="quality_report")
    emit_qc_artifact(correlation_report, correlation_path, config=config, log=log, artifact_name="correlation_report")
    emit_qc_artifact(qc_metrics, qc_metrics_path, config=config, log=log, artifact_name="qc_metrics")

    return DeterministicWriteArtifacts(dataframe=prepared.dataframe, metadata=metadata)
