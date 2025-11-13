"""Transform utilities for ChEMBL assay pipeline array serialization."""

from __future__ import annotations

import json
from typing import Any, cast

import pandas as pd

from bioetl.core.logging import UnifiedLogger
from bioetl.core.logging import LogEvents
from bioetl.core.io import header_rows_serialize, serialize_array_fields

__all__ = [
    "header_rows_serialize",
    "serialize_array_fields",
    "validate_assay_parameters_truv",
]

AssayParam = dict[str, Any]


def _is_null_like(value: Any) -> bool:
    """Return True when a value should be treated as missing."""

    if value is None:
        return True

    if isinstance(value, str):
        return value.strip() == ""

    if isinstance(value, float):
        return bool(pd.isna(value))

    try:
        is_na_raw = cast(Any, pd.isna(value))
    except TypeError:
        return False

    return bool(is_na_raw) if isinstance(is_na_raw, bool) else False


def validate_assay_parameters_truv(
    df: pd.DataFrame,
    column: str = "assay_parameters",
    fail_fast: bool = True,
) -> pd.DataFrame:
    """Validate TRUV invariants for assay parameter payloads.

    Verifies the following invariants:
    - value IS NOT NULL XOR text_value IS NOT NULL
    - standard_value IS NOT NULL XOR standard_text_value IS NOT NULL
    - active ∈ {0, 1, NULL}
    - relation ∈ {'=', '<', '≤', '>', '≥', '~', NULL} (non-standard values produce warnings)

    Parameters
    ----------
    df:
        DataFrame containing the ``assay_parameters`` column (JSON array payload).
    column:
        Name of the column with parameters (defaults to ``"assay_parameters"``).
    fail_fast:
        When True, raises ``ValueError`` on invariant violation.
        When False, logs warnings and continues.

    Returns
    -------
    pd.DataFrame:
        Original DataFrame (the validation step does not mutate data).

    Raises
    ------
    ValueError:
        If ``fail_fast`` is True and an invariant violation is detected.

    Notes
    -----
    - Validation runs during the transform stage to support fail-fast behavior.
    - Standard relation operators: '=', '<', '≤', '>', '≥', '~'.
    - Non-standard operators produce warnings but do not block execution.
    """
    log = UnifiedLogger.get(__name__).bind(component="assay_transform")

    if column not in df.columns:
        log.debug(LogEvents.TRUV_VALIDATION_SKIPPED_MISSING_COLUMN, column=column)
        return df

    # Collection of supported relation operators
    STANDARD_RELATIONS = {"=", "<", "≤", ">", "≥", "~"}

    errors: list[str] = []
    warnings: list[str] = []

    for idx, row in df.iterrows():
        params_str = row.get(column)
        if _is_null_like(params_str):
            continue

        try:
            if isinstance(params_str, str):
                params_raw = json.loads(params_str)
            else:
                params_raw = params_str
        except (json.JSONDecodeError, TypeError) as exc:
            errors.append(
                f"Row {idx}: Invalid JSON in {column}: {exc}",
            )
            continue

        if not isinstance(params_raw, list):
            errors.append(
                f"Row {idx}: {column} must be a JSON array, got {type(params_raw).__name__}",
            )
            continue

        # Validate each parameter entry
        params_candidates = cast(list[object], params_raw)

        for param_idx, param_raw in enumerate(params_candidates):
            if not isinstance(param_raw, dict):
                errors.append(
                    f"Row {idx}, param {param_idx}: Parameter must be a dict, got {type(param_raw).__name__}",
                )
                continue

            param_dict: AssayParam = cast(AssayParam, param_raw)

            # Enforce TRUV invariant: value XOR text_value
            value: Any = param_dict.get("value")
            text_value: Any = param_dict.get("text_value")
            # Treat value as NULL when None, NaN, or an empty string
            value_is_null = (
                value is None
                or (isinstance(value, float) and pd.isna(value))
                or (isinstance(value, str) and value.strip() == "")
            )
            # Treat text_value as NULL when None, NaN, or an empty string
            text_value_is_null = (
                text_value is None
                or (isinstance(text_value, float) and pd.isna(text_value))
                or (isinstance(text_value, str) and text_value.strip() == "")
            )

            if not value_is_null and not text_value_is_null:
                errors.append(
                    f"Row {idx}, param {param_idx}: Both 'value' and 'text_value' are not NULL "
                    f"(value={value}, text_value={text_value}). TRUV invariant violation: "
                    "value and text_value must be mutually exclusive.",
                )

            # Enforce standard TRUV invariant: standard_value XOR standard_text_value
            standard_value: Any = param_dict.get("standard_value")
            standard_text_value: Any = param_dict.get("standard_text_value")
            # Treat standard_value as NULL when None, NaN, or an empty string
            standard_value_is_null = (
                standard_value is None
                or (isinstance(standard_value, float) and pd.isna(standard_value))
                or (isinstance(standard_value, str) and standard_value.strip() == "")
            )
            # Treat standard_text_value as NULL when None, NaN, or an empty string
            standard_text_value_is_null = (
                standard_text_value is None
                or (isinstance(standard_text_value, float) and pd.isna(standard_text_value))
                or (isinstance(standard_text_value, str) and standard_text_value.strip() == "")
            )

            if not standard_value_is_null and not standard_text_value_is_null:
                errors.append(
                    f"Row {idx}, param {param_idx}: Both 'standard_value' and 'standard_text_value' "
                    f"are not NULL (standard_value={standard_value}, "
                    f"standard_text_value={standard_text_value}). TRUV invariant violation: "
                    "standard_value and standard_text_value must be mutually exclusive.",
                )

            # Enforce active ∈ {0, 1, NULL}
            active: Any = param_dict.get("active")
            if active is not None:
                if isinstance(active, bool):
                    # Convert bool into int for validation
                    active_int = 1 if active else 0
                elif isinstance(active, (int, float)):
                    active_int = int(active)
                elif isinstance(active, str):
                    try:
                        active_int = int(active)
                    except ValueError:
                        errors.append(
                            f"Row {idx}, param {param_idx}: Invalid 'active' value: {active!r}. "
                            "Must be 0, 1, or NULL.",
                        )
                        continue
                else:
                    errors.append(
                        f"Row {idx}, param {param_idx}: Invalid 'active' type: {type(active).__name__}. "
                        "Must be 0, 1, or NULL.",
                    )
                    continue

                if active_int not in {0, 1}:
                    errors.append(
                        f"Row {idx}, param {param_idx}: Invalid 'active' value: {active_int}. "
                        "Must be 0, 1, or NULL.",
                    )

            # Enforce relation ∈ {'=', '<', '≤', '>', '≥', '~', NULL}
            relation: Any = param_dict.get("relation")
            if relation is not None and not (isinstance(relation, float) and pd.isna(relation)):
                relation_str = str(relation).strip()
                if relation_str and relation_str not in STANDARD_RELATIONS:
                    warnings.append(
                        f"Row {idx}, param {param_idx}: Non-standard 'relation' value: {relation_str!r}. "
                        f"Standard operators: {', '.join(sorted(STANDARD_RELATIONS))}.",
                    )

    # Emit warnings
    if warnings:
        for warning in warnings:
            log.warning(LogEvents.TRUV_VALIDATION_WARNING, message=warning)

    # Handle validation errors
    if errors:
        error_msg = f"TRUV validation failed for {column}:\n" + "\n".join(errors)
        if fail_fast:
            log.error(LogEvents.TRUV_VALIDATION_FAILED, error_count=len(errors))
            raise ValueError(error_msg)
        else:
            for error in errors:
                log.warning(LogEvents.TRUV_VALIDATION_ERROR, message=error)

    if not errors and not warnings:
        log.debug(LogEvents.TRUV_VALIDATION_PASSED, rows_checked=len(df))

    return df
