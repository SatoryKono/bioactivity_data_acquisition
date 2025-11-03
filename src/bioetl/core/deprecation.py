"""Utilities for emitting deprecation warnings consistently."""

from __future__ import annotations

import warnings


def warn_legacy_client(module_name: str, *, replacement: str | None = None) -> None:
    """Emit a standardized deprecation warning for legacy client modules."""

    message = (
        "Legacy client module '"
        + module_name
        + "' is deprecated and will be removed in a future release."
    )
    if replacement:
        message += f" Use {replacement} instead."

    warnings.warn(message, DeprecationWarning, stacklevel=3)


__all__ = ["warn_legacy_client"]
