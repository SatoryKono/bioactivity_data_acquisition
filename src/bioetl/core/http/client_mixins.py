"""Mixins for API client error handling and logging."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, TypeVar

import structlog
from bioetl.core.http.pagination_helpers import normalize_payload

if TYPE_CHECKING:
    from bioetl.core.http.interfaces import (
        ApiTransportProtocol,
        BaseApiClient,
    )
else:
    ApiTransportProtocol = BaseApiClient = Any

_T = TypeVar("_T")


class ApiClientMixin:
    """Mixin for API client error handling and logging.

    Responsible for:
    1. Logging errors during request execution via ``_logger``.
    2. Catching exceptions and converting them to
       ``bioetl.clients.client_exceptions.RequestException``.
    3. Providing wrapper methods ``_wrap_callable`` and ``_wrap_iterator``.

    Client code should not duplicate exception handling logic,
    but should use these methods.
    """
    api_client: BaseApiClient | ApiTransportProtocol
    _logger: (
        structlog.stdlib.BoundLogger
        | structlog.typing.FilteringBoundLogger
    )

    def _transport(self) -> ApiTransportProtocol | BaseApiClient:
        transport = (
            getattr(self, "transport", None)
            or getattr(self, "api_client", None)
        )
        if transport is None:
            raise AttributeError(
                "ApiClientMixin requires 'transport' or 'api_client' attribute"
            )
        return transport

    def _normalize_payload(
        self, payload: Any, *, page_key: str | None = "results"
    ) -> Iterator[dict[str, Any]]:
        effective_page_key = (
            page_key
            if page_key is not None
            else getattr(self, "_page_key_override", "results")
        )
        yield from normalize_payload(payload, page_key=effective_page_key)

    def _wrap_callable(
        self,
        func: Callable[[], _T],
        *,
        log_context: Mapping[str, Any] | None = None,
    ) -> _T:
        """Wrap function call for error handling and logging.

        Args:
            func: Function to execute (usually lambda with client call).
            log_context: Additional context for error logging.

        Returns:
            Result of ``func`` execution.

        Raises:
            client_exceptions.HTTPError: Passed through unchanged.
            client_exceptions.RequestException: Wraps other exceptions.
        """
        from bioetl.clients import exceptions as client_exceptions

        try:
            return func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            context = dict(log_context or {})
            self._logger.error("api_call_failed", error=str(exc), **context)
            raise client_exceptions.RequestException(str(exc)) from exc

    def _wrap_iterator(
        self,
        func: Callable[[], Iterator[dict[str, Any]]],
        *,
        log_context: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Wrap iterator for error handling and logging.

        Similar to ``_wrap_callable``, but for generators/iterators.

        Args:
            func: Function returning an iterator.
            log_context: Additional context for logging.

        Yields:
            Elements from the iterator.

        Raises:
            client_exceptions.HTTPError: Passed through.
            client_exceptions.RequestException: Wraps other errors.
        """
        from bioetl.clients import exceptions as client_exceptions

        try:
            yield from func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            context = dict(log_context or {})
            self._logger.error(
                "api_call_failed",
                error=str(exc),
                **context
            )
            raise client_exceptions.RequestException(str(exc)) from exc


class ClosableMixin:
    """Mixin for proper resource cleanup in API clients."""
    api_client: BaseApiClient | ApiTransportProtocol
    _logger: (
        structlog.stdlib.BoundLogger
        | structlog.typing.FilteringBoundLogger
    )

    def close(self) -> None:
        """Close the underlying transport if available."""
        transport_attr = getattr(self, "_transport", None)
        if callable(transport_attr):
            try:
                result = transport_attr()
                if (
                    result
                    and hasattr(result, "close")
                    and callable(result.close)
                ):
                    result.close()
                return
            except Exception:
                # If call failed (e.g. requires args), ignore and fall through
                pass

        transport = getattr(self, "api_client", None)
        close_fn = getattr(transport, "close", None)
        if callable(close_fn):
            close_fn()


__all__ = ["ApiClientMixin", "ClosableMixin"]
