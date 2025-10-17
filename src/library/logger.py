"""Legacy logging utilities - DEPRECATED.

This module is deprecated. Use library.logging_setup instead.
"""

from __future__ import annotations

import logging
from typing import Any
import warnings

# structlog import removed - using new logging_setup

# Import the new unified logging setup
from .logging_setup import configure_logging as _configure_logging, get_logger as _get_logger

_LOGGING_CONFIGURED = False


def configure_logging(level: int = logging.INFO) -> None:
    """Configure structlog for structured logging.

    DEPRECATED: Use library.logging_setup.configure_logging() instead.
    This function is kept for backward compatibility.
    """

    warnings.warn(
        "library.logger.configure_logging is deprecated. Use library.logging_setup.configure_logging instead.",
        DeprecationWarning,
        stacklevel=2
    )

    # Convert int level to string for new function
    level_str = logging.getLevelName(level)
    _configure_logging(level=level_str)


def get_logger(name: str, **initial_values: Any) -> Any:
    """Return a configured structlog logger.

    DEPRECATED: Use library.logging_setup.get_logger() instead.
    This function is kept for backward compatibility.
    """

    warnings.warn(
        "library.logger.get_logger is deprecated. Use library.logging_setup.get_logger instead.",
        DeprecationWarning,
        stacklevel=2
    )

    return _get_logger(name, **initial_values)
