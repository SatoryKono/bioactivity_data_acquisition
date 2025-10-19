"""Legacy logging utilities - DEPRECATED.

This module is deprecated. Use library.logging_setup instead.
"""

from __future__ import annotations

import warnings
from typing import Any

from structlog.stdlib import BoundLogger

from library.logging_setup import bind_stage as _bind_stage

# Import the new unified logging setup
from library.logging_setup import configure_logging as _configure_logging


def configure_logging(level: str = "INFO") -> BoundLogger:
    """Configure structlog for structured logging.

    DEPRECATED: Use library.logging_setup.configure_logging() instead.
    This function is kept for backward compatibility.
    """

    warnings.warn("library.utils.logging.configure_logging is deprecated. Use library.logging_setup.configure_logging instead.", DeprecationWarning, stacklevel=2)

    return _configure_logging(level=level)


def bind_stage(logger: BoundLogger, stage: str, **extra: Any) -> BoundLogger:
    """Attach contextual metadata to the logger.

    DEPRECATED: Use library.logging_setup.bind_stage() instead.
    This function is kept for backward compatibility.
    """

    warnings.warn("library.utils.logging.bind_stage is deprecated. Use library.logging_setup.bind_stage instead.", DeprecationWarning, stacklevel=2)

    return _bind_stage(logger, stage, **extra)


__all__ = ["bind_stage", "configure_logging"]
