"""Utilities for constructing unified fallback payloads."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import pandas as pd


def _extract_http_status(error: Any) -> int | None:
    """Best-effort extraction of HTTP status code from ``error``."""

    if error is None:
        return None

    response = getattr(error, "response", None)
    if response is not None:
        status = getattr(response, "status_code", None)
        if status is not None:
            return int(status)

    status = getattr(error, "status", None)
    if status is None:
        status = getattr(error, "status_code", None)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return None


def _extract_retry_after(error: Any) -> float | None:
    """Retrieve Retry-After seconds from ``error`` when available."""

    if error is None:
        return None

    retry_after = getattr(error, "retry_after", None)
    if retry_after is not None:
        try:
            return float(retry_after)
        except (TypeError, ValueError):
            return None

    response = getattr(error, "response", None)
    if response is None:  # pragma: no cover - defensive
        return None

    headers = getattr(response, "headers", None)
    if not isinstance(headers, Mapping):  # pragma: no cover - defensive
        return None

    retry_after_header = headers.get("Retry-After")
    if retry_after_header is None:
        return None

    try:
        return float(retry_after_header)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return None


def _stringify(value: Any) -> str | None:
    """Convert ``value`` to string preserving ``None``."""

    if value is None:
        return None
    return str(value)


def build_fallback_payload(
    *,
    entity: str,
    reason: str,
    error: Exception | None,
    source: str | None = None,
    attempt: int | None = None,
    message: str | None = None,
    context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Construct a unified fallback payload for pipeline records.

    Args:
        entity: Logical entity name (e.g. ``"activity"`` or ``"document"``).
        reason: High level fallback reason (e.g. ``"exception"``).
        error: Exception instance that triggered the fallback.
        source: Optional explicit source label. When omitted, ``entity`` is
            upper-cased and suffixed with ``"_FALLBACK"``.
        attempt: Retry attempt number, if known.
        message: Custom fallback message. When ``None`` the helper derives a
            message from ``error`` or ``reason``.
        context: Additional context merged into the resulting payload.

    Returns:
        Dictionary containing normalized fallback metadata fields.
    """

    label = source or f"{entity.strip().upper()}_FALLBACK" if entity else "FALLBACK"
    attempt_value = attempt if attempt is not None else getattr(error, "attempt", None)
    http_status = _extract_http_status(error)
    retry_after = _extract_retry_after(error)
    error_code = _stringify(getattr(error, "code", None))
    error_type = type(error).__name__ if error is not None else None

    if message is None:
        if error is not None:
            derived = str(error)
            message = derived if derived else f"Fallback triggered due to {reason}"
        else:
            message = f"Fallback triggered due to {reason}"

    payload: dict[str, Any] = {
        "source_system": label,
        "fallback_reason": reason,
        "fallback_error_type": error_type,
        "fallback_error_code": error_code,
        "fallback_error_message": message,
        "fallback_http_status": http_status,
        "fallback_retry_after_sec": retry_after,
        "fallback_attempt": attempt_value if attempt_value is not None else None,
        "fallback_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if context:
        payload.update(dict(context))

    return payload


def normalise_retry_after_column(
    df: pd.DataFrame, column: str = "fallback_retry_after_sec"
) -> None:
    """Coerce ``column`` to a float dtype compatible with Pandera schemas.

    Pandera attempts to cast ``fallback_retry_after_sec`` to ``float64`` during
    schema validation.  When the dataframe contains ``pd.NA`` values and the
    series dtype is ``object``, the coercion fails with ``TypeError: float()
    argument must be a string or a real number, not 'NAType'``.  Explicitly
    normalising the column with :func:`pandas.to_numeric` replaces these
    ``pd.NA`` placeholders with ``NaN`` and yields a ``float64`` series,
    restoring deterministic validation behaviour across pipelines.
    """

    if column not in df.columns:
        return

    series = df[column]
    if not isinstance(series, pd.Series):  # pragma: no cover - defensive guard
        return

    df[column] = pd.to_numeric(series, errors="coerce")

