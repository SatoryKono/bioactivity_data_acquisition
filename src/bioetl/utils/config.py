"""Utility helpers for coercing configuration values with consistent logging."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Final

LogHook = Callable[..., Any]


def _emit(
    log: LogHook | None,
    event: str | None,
    log_context: Mapping[str, Any] | None,
    payload: Mapping[str, Any],
) -> None:
    """Emit a structured log message if a hook was provided."""

    if log is None or event is None:
        return

    context: dict[str, Any] = {}
    if log_context is not None:
        context.update(dict(log_context))
    context.update(payload)
    log(event, **context)


def coerce_int_config(
    value: Any,
    default: int,
    *,
    field: str,
    minimum: int | None = None,
    exclusive_minimum: bool = False,
    log: LogHook | None = None,
    log_context: Mapping[str, Any] | None = None,
    invalid_event: str | None = None,
    out_of_range_event: str | None = None,
) -> int:
    """Coerce a configuration value to ``int`` with defensive logging."""

    if value is None:
        return default

    try:
        candidate = int(value)
    except (TypeError, ValueError):
        _emit(
            log,
            invalid_event,
            log_context,
            {"field": field, "value": value, "default": default},
        )
        return default

    if minimum is not None:
        violates_minimum: bool
        if exclusive_minimum:
            violates_minimum = candidate <= minimum
        else:
            violates_minimum = candidate < minimum

        if violates_minimum:
            _emit(
                log,
                out_of_range_event,
                log_context,
                {
                    "field": field,
                    "value": candidate,
                    "minimum": minimum,
                    "exclusive_minimum": exclusive_minimum,
                    "default": default,
                },
            )
            return default

    return candidate


def coerce_float_config(
    value: Any,
    default: float,
    *,
    field: str,
    minimum: float | None = None,
    exclusive_minimum: bool = False,
    log: LogHook | None = None,
    log_context: Mapping[str, Any] | None = None,
    invalid_event: str | None = None,
    out_of_range_event: str | None = None,
) -> float:
    """Coerce a configuration value to ``float`` with defensive logging."""

    if value is None:
        return default

    try:
        candidate = float(value)
    except (TypeError, ValueError):
        _emit(
            log,
            invalid_event,
            log_context,
            {"field": field, "value": value, "default": default},
        )
        return default

    if minimum is not None:
        violates_minimum: bool
        if exclusive_minimum:
            violates_minimum = candidate <= minimum
        else:
            violates_minimum = candidate < minimum

        if violates_minimum:
            _emit(
                log,
                out_of_range_event,
                log_context,
                {
                    "field": field,
                    "value": candidate,
                    "minimum": minimum,
                    "exclusive_minimum": exclusive_minimum,
                    "default": default,
                },
            )
            return default

    return candidate


__all__: Final = ["coerce_int_config", "coerce_float_config"]
