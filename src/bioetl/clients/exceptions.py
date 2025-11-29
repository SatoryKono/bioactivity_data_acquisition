"""Exception classes for bioetl clients."""

from __future__ import annotations

from typing import Any

from bioetl.clients.base.exceptions import RequestException


class PartialFailureError(Exception):
    """Exception raised when some operations fail but others succeed."""

    def __init__(self, message: str, *, partial_data: dict[str, Any]) -> None:
        super().__init__(message)
        self.partial_data = partial_data


__all__ = ["PartialFailureError", "RequestException"]
