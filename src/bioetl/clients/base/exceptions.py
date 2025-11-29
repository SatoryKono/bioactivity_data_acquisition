"""Публичные исключения HTTP-клиентов BioETL."""

from __future__ import annotations

from requests.exceptions import ConnectionError as _RequestsConnectionError
from requests.exceptions import HTTPError as _RequestsHTTPError
from requests.exceptions import RequestException as _RequestsRequestException
from requests.exceptions import Timeout as _RequestsTimeout


class ProviderError(_RequestsRequestException):
    """Исключение верхнего уровня для клиентских ошибок источника."""


class PaginationError(ProviderError):
    """Ошибка при обходе страниц провайдера."""


class ConfigurationError(ProviderError):
    """Ошибки конфигурации клиента или транспорта."""

__all__ = [
    "RequestException",
    "HTTPError",
    "Timeout",
    "ConnectionError",
    "ProviderError",
    "PaginationError",
    "ConfigurationError",
]

# Re-export requests exceptions while keeping the underlying types.
RequestException = _RequestsRequestException
HTTPError = _RequestsHTTPError
Timeout = _RequestsTimeout
ConnectionError = _RequestsConnectionError
