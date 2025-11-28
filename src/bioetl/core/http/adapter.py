"""Reusable HTTP transport adapters with logging and metadata capture."""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import structlog

from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy


class LoggingTransportAdapter(ApiClientMixin, ClosableMixin, ApiTransportProtocol):
    """Thin wrapper over ``ApiTransportProtocol`` with logging helpers.

    This adapter captures response metadata (if present) and provides a
    consistent logging surface for derived transport adapters. Pagination
    strategy resolution can be delegated via ``pagination_factory`` to keep
    derived classes small and focused on domain specifics.
    """

    def __init__(
        self,
        transport: ApiTransportProtocol,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_factory: Callable[
            [str | None, Mapping[str, Any] | None], PaginationStrategy | None
        ]
        | None = None,
        pagination_factories: Mapping[str, Any] | None = None,
        client_name: str = "transport_adapter",
    ) -> None:
        self._base_transport = transport
        self.transport = transport
        self._pagination_strategy: PaginationStrategy | None = None
        self.pagination_strategy = pagination_strategy or (
            pagination_factory(pagination_strategy_name, pagination_factories)
            if pagination_factory
            else None
        )
        self._metadata: dict[str, Any] = {}
        self._logger = structlog.get_logger(__name__).bind(client=client_name)

    @property
    def pagination_strategy(self) -> PaginationStrategy | None:  # type: ignore[override]
        return self._pagination_strategy

    @pagination_strategy.setter
    def pagination_strategy(self, value: PaginationStrategy | None) -> None:
        self._pagination_strategy = value

    @property
    def base_transport(self) -> ApiTransportProtocol:
        """Access underlying transport without wrapper layers."""

        return self._base_transport

    @property
    def metadata(self) -> Mapping[str, Any]:
        """Expose collected metadata from previous responses."""

        return dict(self._metadata)

    def _capture_metadata(
        self, payload: Mapping[str, Any] | Sequence[Mapping[str, Any]] | Any
    ) -> None:
        if not isinstance(payload, Mapping):
            return

        collected: dict[str, Any] = {}
        for key in ("page_meta", "meta"):
            value = payload.get(key)
            if isinstance(value, Mapping):
                collected.update(value)

        if collected:
            self._metadata.update(collected)

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        response = self._wrap_callable(
            lambda: self._base_transport.request(
                method,
                path,
                headers=headers,
                params=params,
                json=json,
            ),
            log_context={"path": path, "method": method},
        )
        self._capture_metadata(response)
        return response


__all__ = ["LoggingTransportAdapter"]
