"""Public HTTP client exceptions exposed by BioETL.

The module defines a stable contract for upper layers (for example, the CLI)
by hiding concrete implementation details of the networking clients. External
components must import network exceptions only from here to avoid direct
dependencies on ``requests``.
"""

from __future__ import annotations

from requests.exceptions import ConnectionError as _RequestsConnectionError
from requests.exceptions import HTTPError as _RequestsHTTPError
from requests.exceptions import RequestException as _RequestsRequestException
from requests.exceptions import Timeout as _RequestsTimeout

__all__ = [
    "RequestException",
    "HTTPError",
    "Timeout",
    "ConnectionError",
]

# Re-export requests exceptions while keeping the underlying types.
RequestException = _RequestsRequestException
HTTPError = _RequestsHTTPError
Timeout = _RequestsTimeout
ConnectionError = _RequestsConnectionError


