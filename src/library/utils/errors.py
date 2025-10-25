"""DEPRECATED: Custom exception hierarchy for the publications ETL pipeline.

This module is deprecated and will be removed in a future version.
Use library.common.exceptions instead.
"""

from __future__ import annotations

import warnings

warnings.warn("library.utils.errors is deprecated. Use library.common.exceptions instead.", DeprecationWarning, stacklevel=2)

# Re-export from the new unified exceptions system for backward compatibility
from library.common.exceptions import ConfigError as _ConfigError
from library.common.exceptions import ExtractionError as _ExtractionError
from library.common.exceptions import ValidationError as _ValidationError


# Legacy compatibility classes
class ConfigError(_ConfigError):
    """Legacy compatibility wrapper for ConfigError."""

    def __init__(self, message: str) -> None:
        warnings.warn("ConfigError from library.utils.errors is deprecated. Use library.common.exceptions.ConfigError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message)


class ExtractionError(_ExtractionError):
    """Legacy compatibility wrapper for ExtractionError."""

    def __init__(self, message: str) -> None:
        warnings.warn("ExtractionError from library.utils.errors is deprecated. Use library.common.exceptions.ExtractionError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message)


class ValidationError(_ValidationError):
    """Legacy compatibility wrapper for ValidationError."""

    def __init__(self, message: str) -> None:
        warnings.warn("ValidationError from library.utils.errors is deprecated. Use library.common.exceptions.ValidationError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message)


__all__ = ["ConfigError", "ExtractionError", "ValidationError"]
