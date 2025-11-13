"""High-level exception hierarchy for BioETL pipelines."""

from __future__ import annotations

from builtins import ConnectionError as BuiltinConnectionError
from builtins import TimeoutError as BuiltinTimeoutError

from bioetl.clients import client_exceptions
from bioetl.core.http import CircuitBreakerOpenError

__all__ = [
    "PipelineError",
    "PipelineNetworkError",
    "PipelineTimeoutError",
    "PipelineHTTPError",
    "map_client_exc",
]

class PipelineError(Exception):
    """Base exception for pipeline failures."""


class PipelineNetworkError(PipelineError):
    """Network interaction error raised inside a pipeline."""


class PipelineTimeoutError(PipelineNetworkError):
    """Timeout raised during a network call."""


class PipelineHTTPError(PipelineNetworkError):
    """HTTP-layer failure raised during a network call."""


def map_client_exc(exc: Exception) -> PipelineError:
    """Map client-level exceptions to the pipeline hierarchy."""

    if isinstance(exc, PipelineError):
        return exc

    if isinstance(exc, (client_exceptions.Timeout, BuiltinTimeoutError)):
        mapped = PipelineTimeoutError(str(exc))
        mapped.__cause__ = exc
        return mapped

    if isinstance(exc, client_exceptions.HTTPError):
        mapped = PipelineHTTPError(str(exc))
        mapped.__cause__ = exc
        return mapped

    if isinstance(
        exc,
        (
            client_exceptions.ConnectionError,
            client_exceptions.RequestException,
            BuiltinConnectionError,
            CircuitBreakerOpenError,
        ),
    ):
        mapped = PipelineNetworkError(str(exc))
        mapped.__cause__ = exc
        return mapped

    mapped = PipelineError(str(exc))
    mapped.__cause__ = exc
    return mapped

