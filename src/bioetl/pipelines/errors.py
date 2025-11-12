"""Иерархия высокоуровневых исключений пайплайнов BioETL."""

from __future__ import annotations

from builtins import ConnectionError as BuiltinConnectionError
from builtins import TimeoutError as BuiltinTimeoutError

from bioetl.clients import client_exceptions
from bioetl.core.api_client import CircuitBreakerOpenError

__all__ = [
    "PipelineError",
    "PipelineNetworkError",
    "PipelineTimeoutError",
    "PipelineHTTPError",
    "map_client_exc",
]

class PipelineError(Exception):
    """Базовое исключение пайплайна."""


class PipelineNetworkError(PipelineError):
    """Ошибка сетевого взаимодействия в пайплайне."""


class PipelineTimeoutError(PipelineNetworkError):
    """Истек таймаут во время сетевого вызова."""


class PipelineHTTPError(PipelineNetworkError):
    """HTTP-ошибка в процессе сетевого вызова."""


def map_client_exc(exc: Exception) -> PipelineError:
    """Сопоставить исключение клиента с иерархией пайплайна."""

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

