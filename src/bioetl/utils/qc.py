"""Helpers for assembling QC summary structures and related artefacts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Literal

import pandas as pd

from bioetl.utils.fallback import normalise_retry_after_column


_SequenceMode = Literal["auto", "set", "update", "extend"]


def _normalise_path(path: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(path, str):
        normalised = path.strip()
        if not normalised:
            raise ValueError("Summary path must contain at least one key")
        return (normalised,)

    components = tuple(str(part) for part in path if str(part))
    if not components:
        raise ValueError("Summary path must contain at least one key")
    return components


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def accumulate_summary(
    summary: dict[str, Any],
    path: Sequence[str] | str,
    payload: Any,
    *,
    mode: _SequenceMode = "auto",
) -> dict[str, Any]:
    """Merge ``payload`` into ``summary`` following the provided ``path``.

    Args:
        summary: Target summary dictionary mutated in-place.
        path: Location expressed either as a single key or a sequence of keys.
        payload: Value to merge at the destination node.
        mode: Merge strategy.  ``"auto"`` infers behaviour from ``payload`` type,
            ``"set"`` overwrites the destination, ``"update"`` merges mapping
            values and ``"extend"`` appends to list-like destinations.

    Returns:
        The original ``summary`` dictionary for convenience.
    """

    keys = _normalise_path(path)
    node: dict[str, Any] = summary
    for key in keys[:-1]:
        node = node.setdefault(key, {})

    leaf = keys[-1]

    merge_mode = mode
    if merge_mode == "auto":
        if isinstance(payload, Mapping):
            merge_mode = "update"
        elif _is_sequence(payload):
            merge_mode = "extend"
        else:
            merge_mode = "set"

    if merge_mode == "set":
        node[leaf] = payload
        return summary

    if merge_mode == "update":
        existing = node.get(leaf)
        if not isinstance(existing, dict):
            existing = {}
        else:
            existing = dict(existing)
        existing.update(dict(payload))
        node[leaf] = existing
        return summary

    if merge_mode == "extend":
        existing_list: list[Any]
        existing = node.get(leaf)
        if isinstance(existing, list):
            existing_list = existing
        elif existing is None:
            existing_list = []
        else:
            existing_list = list(existing if _is_sequence(existing) else [existing])
        existing_list.extend(list(payload))
        node[leaf] = existing_list
        return summary

    raise ValueError(f"Unsupported accumulate mode: {merge_mode}")


def register_fallback_statistics(
    summary: dict[str, Any],
    *,
    df: pd.DataFrame | None,
    additional_tables: dict[str, pd.DataFrame] | None = None,
    table_name: str | None = None,
    id_column: str | None = None,
    fallback_columns: Sequence[str] | None = None,
    source_column: str = "source_system",
    fallback_label: str = "CHEMBL_FALLBACK",
) -> dict[str, Any]:
    """Populate fallback diagnostics for QC reporting.

    The helper extracts rows that originated from fallback flows and populates
    ``summary`` with row counts alongside per-reason aggregates.  When
    ``additional_tables`` is provided the fallback rows are materialised for
    downstream export.
    """

    total_rows = int(len(df)) if df is not None else 0

    if df is None or source_column not in df.columns:
        if additional_tables is not None and table_name:
            additional_tables.pop(table_name, None)
        summary.pop("fallbacks", None)
        if total_rows:
            accumulate_summary(
                summary,
                "row_counts",
                {"total": total_rows},
            )
        return {}

    working = df.copy()
    source_series = working[source_column].astype("string")
    fallback_mask = source_series.str.upper() == fallback_label.upper()

    fallback_count = int(fallback_mask.sum())
    success_count = int(total_rows - fallback_count)
    fallback_rate = float(fallback_count / total_rows) if total_rows else 0.0

    selected_columns: list[str] = []
    if fallback_columns:
        selected_columns.extend([column for column in fallback_columns if column in working.columns])
    else:
        selected_columns = [
            column
            for column in (
                id_column,
                source_column,
                "fallback_reason",
                "fallback_error_type",
                "fallback_error_message",
                "fallback_http_status",
                "fallback_error_code",
                "fallback_retry_after_sec",
                "fallback_attempt",
                "fallback_timestamp",
                "chembl_release",
                "run_id",
                "extracted_at",
            )
            if column and column in working.columns
        ]

    fallback_records = (
        working.loc[fallback_mask, selected_columns].copy()
        if fallback_count and selected_columns
        else pd.DataFrame(columns=selected_columns)
    )

    if not fallback_records.empty and "fallback_retry_after_sec" in fallback_records.columns:
        normalise_retry_after_column(fallback_records)

    reason_counts: dict[str, int] = {}
    if fallback_count and "fallback_reason" in fallback_records.columns:
        counts = (
            fallback_records["fallback_reason"].fillna("<missing>")
            .astype("string")
            .value_counts(dropna=False)
            .to_dict()
        )
        reason_counts = {str(reason): int(count) for reason, count in counts.items()}

    entity_ids: list[int] | list[str] = []
    if fallback_count and id_column and id_column in fallback_records.columns:
        id_series = fallback_records[id_column]
        try:
            numeric_ids = pd.to_numeric(id_series, errors="coerce")
            entity_ids = sorted({int(value) for value in numeric_ids.dropna().astype(int).tolist()})
        except (TypeError, ValueError):
            entity_ids = sorted({str(value) for value in id_series.dropna().astype(str).tolist()})

    summary_payload: dict[str, Any] = {
        "total_rows": total_rows,
        "success_count": success_count,
        "fallback_count": fallback_count,
        "fallback_rate": fallback_rate,
        "reason_counts": reason_counts,
    }
    summary_payload["entity_ids"] = entity_ids
    if id_column:
        base = id_column.rstrip("_id") if id_column.endswith("_id") else id_column
        plural_key = f"{base}_ids" if not base.endswith("s") else base
        summary_payload[plural_key] = entity_ids

    accumulate_summary(
        summary,
        "row_counts",
        {"total": total_rows, "success": success_count, "fallback": fallback_count},
    )
    accumulate_summary(summary, "fallbacks", summary_payload, mode="set")

    if additional_tables is not None and table_name:
        if not fallback_records.empty:
            fallback_records = fallback_records.reset_index(drop=True).convert_dtypes()
            additional_tables[table_name] = fallback_records
        else:
            additional_tables.pop(table_name, None)

    return summary_payload


def prepare_missing_mappings_table(
    records: Iterable[Mapping[str, Any]] | pd.DataFrame | None,
) -> pd.DataFrame:
    """Convert missing mapping records into a deterministic dataframe."""

    if records is None:
        return pd.DataFrame()

    if isinstance(records, pd.DataFrame):
        df = records.copy()
    else:
        materialised = list(records)
        if not materialised:
            return pd.DataFrame()
        df = pd.DataFrame(materialised)

    if df.empty:
        return df

    expected_order = [
        "stage",
        "target_chembl_id",
        "input_accession",
        "resolved_accession",
        "resolution",
        "status",
        "details",
    ]

    for column in expected_order:
        if column not in df.columns:
            df[column] = pd.NA

    remaining = [column for column in df.columns if column not in expected_order]
    ordered_columns = expected_order + remaining
    df = df.loc[:, ordered_columns]
    df = df.convert_dtypes()

    sort_columns = [column for column in ("stage", "target_chembl_id", "input_accession") if column in df.columns]
    if sort_columns:
        df = df.sort_values(by=sort_columns, kind="mergesort").reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    return df


def prepare_enrichment_metrics_table(
    metrics: Iterable[Mapping[str, Any]] | pd.DataFrame | None,
) -> pd.DataFrame:
    """Normalise enrichment metric records for CSV export."""

    if metrics is None:
        return pd.DataFrame()

    if isinstance(metrics, pd.DataFrame):
        df = metrics.copy()
    else:
        materialised = list(metrics)
        if not materialised:
            return pd.DataFrame()
        df = pd.DataFrame(materialised)

    if df.empty:
        return df

    expected_order = ["metric", "value", "threshold_min", "passed", "severity"]
    for column in expected_order:
        if column not in df.columns:
            df[column] = pd.NA

    remaining = [column for column in df.columns if column not in expected_order]
    df = df.loc[:, expected_order + remaining]
    df = df.convert_dtypes()

    if "metric" in df.columns:
        df = df.sort_values(by=["metric"], kind="mergesort").reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    return df

