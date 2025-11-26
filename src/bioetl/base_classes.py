"""Deprecated shim for common client contracts.

This module now re-exports definitions from :mod:`bioetl.clients.common` to
provide backwards compatibility. Prefer importing directly from
``bioetl.clients.common``.
"""

from __future__ import annotations

import warnings

from bioetl.clients.common import (
    BaseApiClient,
    EntityClientProtocol,
    JSONPage,
    JSONPayload,
    JSONRecord,
    JSONRecordStream,
)

__all__ = [
    "BaseApiClient",
    "EntityClientProtocol",
    "JSONPayload",
    "JSONPage",
    "JSONRecord",
    "JSONRecordStream",
]

warnings.warn(
    "bioetl.base_classes is deprecated; use bioetl.clients.common instead.",
    DeprecationWarning,
    stacklevel=2,
)

