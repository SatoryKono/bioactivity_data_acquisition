"""Utilities for constructing unified fallback payloads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any
from warnings import warn

import pandas as pd

from bioetl.utils.dtypes import coerce_retry_after


class FallbackRecordBuilder:
    """Helper for constructing deterministic fallback records."""

    __slots__ = ("_business_columns", "_context")

    def __init__(
        self,
        *,
        business_columns: Sequence[str],
        context: Mapping[str, Any] | None = None,
    ) -> None:
        if not business_columns:
            raise ValueError("business_columns must not be empty")

        self._business_columns = tuple(business_columns)
        self._context = MappingProxyType(dict(context or {}))

    @property
    def business_columns(self) -> tuple[str, ...]:
        """Return the immutable sequence of business columns."""

        return self._business_columns

    @property
    def context(self) -> Mapping[str, Any]:
        """Return immutable base context for fallback payloads."""

        return self._context

    def record(self, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Create a fallback record skeleton with optional overrides."""

        record = dict.fromkeys(self._business_columns)
        record.update(self._context)
        if overrides:
            record.update(dict(overrides))
        return record

    def context_with(self, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Merge base context with ``extra`` for payload construction."""

        if not extra:
            return dict(self._context)
        merged = dict(self._context)
        merged.update(dict(extra))
        return merged


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
) -> None:  # pragma: no cover - compatibility shim
    """Backwards compatible wrapper around :func:`coerce_retry_after`."""

    warn(
        "'normalise_retry_after_column' is deprecated; use 'coerce_retry_after' instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    coerce_retry_after(df, column=column)

