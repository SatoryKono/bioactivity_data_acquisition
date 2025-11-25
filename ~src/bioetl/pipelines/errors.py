"""Compatibility shim for legacy ``bioetl.pipelines.errors`` imports.

This module is deprecated and will be removed in a future release. Import
pipeline exceptions directly from :mod:`bioetl.core.pipeline.errors`.
"""

from __future__ import annotations

import warnings

from bioetl.core.pipeline.errors import (
    PipelineError,
    PipelineHTTPError,
    PipelineNetworkError,
    PipelineTimeoutError,
    map_client_exc,
)

warnings.warn(
    (
        "Importing from 'bioetl.pipelines.errors' is deprecated; "
        "use 'bioetl.core.pipeline.errors' instead."
    ),
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "PipelineError",
    "PipelineHTTPError",
    "PipelineNetworkError",
    "PipelineTimeoutError",
    "map_client_exc",
]
