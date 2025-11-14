"""Lightweight wrapper around :class:`UnifiedAPIClient` with consistent logging."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from requests import Response
from structlog.stdlib import BoundLogger

from bioetl.clients.client_exceptions import RequestException
from bioetl.core.http import CircuitBreakerOpenError, UnifiedAPIClient
from bioetl.core.logging import LogEvents, UnifiedLogger

__all__ = ["RetryingSession"]


class RetryingSession:
    """Provide guarded HTTP execution with normalized exception semantics."""

    def __init__(self, client: UnifiedAPIClient, *, logger: BoundLogger | None = None) -> None:
        self._client = client
        self._log = logger or UnifiedLogger.get(__name__).bind(component="clients.retrying_session")

    @property
    def base_url(self) -> str | None:
        """Return the base URL associated with the wrapped client."""

        base_url = getattr(self._client, "base_url", None)
        if isinstance(base_url, str):
            stripped = base_url.strip()
            return stripped or None
        return None

    def get_payload(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> tuple[Mapping[str, Any], Response]:
        """Execute GET request and return both payload and raw response."""

        response = self._perform_get(endpoint, params=params)
        payload = self._coerce_json(response)
        return payload, response

    def _perform_get(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None,
    ) -> Response:
        try:
            return self._client.get(endpoint, params=params)
        except CircuitBreakerOpenError as exc:  # pragma: no cover - exercised via integration tests
            self._log.warning(LogEvents.CLIENT_CIRCUIT_OPEN,
                endpoint=endpoint,
                http_client=self._client.name,
                error=str(exc),
            )
            raise
        except RequestException as exc:
            self._log.error(LogEvents.HTTP_REQUEST_FAILED,
                endpoint=endpoint,
                http_client=self._client.name,
                error=str(exc),
            )
            raise

    def _coerce_json(self, response: Response) -> Mapping[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise RequestException(f"Unable to decode JSON response from {response.url!s}") from exc
        if isinstance(payload, Mapping):
            return payload
        raise RequestException(
            f"Expected mapping payload from {response.url!s}, received {type(payload).__name__}",
        )

