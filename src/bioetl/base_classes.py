"""Deprecated shim for common client contracts.

This module now re-exports definitions from :mod:`bioetl.core.http` to
provide backwards compatibility. Prefer importing directly from
``bioetl.core.http`` and related modules.
"""

from __future__ import annotations

import warnings

from bioetl.core.http.api_entity_client import EntityClientProtocol
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.types import JSONPage, JSONPayload, JSONRecord, JSONRecordStream

__all__ = [
    "BaseApiClient",
    "EntityClientProtocol",
    "JSONPayload",
    "JSONPage",
    "JSONRecord",
    "JSONRecordStream",
]

warnings.warn(
    "bioetl.base_classes is deprecated; use bioetl.core.http instead.",
    DeprecationWarning,
    stacklevel=2,
)

