"""Compatibility helpers for legacy ``bioetl.pipelines.common`` imports.

This shim is deprecated. Import helpers from :mod:`bioetl.utils` instead.
"""

from __future__ import annotations

import warnings

from bioetl.utils.fs import ensure_directory

warnings.warn(
    (
        "Importing 'ensure_directory' from 'bioetl.pipelines.common' is "
        "deprecated; use 'bioetl.utils.ensure_directory' instead."
    ),
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["ensure_directory"]
