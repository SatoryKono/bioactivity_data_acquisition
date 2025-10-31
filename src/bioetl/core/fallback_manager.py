"""Fallback manager for resilient API interactions."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import logging
from typing import Any, TypeVar, cast

from requests import Response
from requests import exceptions as req_exc

PayloadT = TypeVar("PayloadT")


logger = logging.getLogger(__name__)


class FallbackManager:
    """Manage fallback strategies for network operations.

    The implementation follows the design outlined in ``REFACTOR_PLAN.md`` where
    strategies are expressed via human readable labels (``"network"``,
    ``"timeout"``, ``"5xx"``). The manager inspects raised exceptions and decides
    whether a fallback should be executed. When a fallback is triggered the
    manager returns deterministic placeholder data to keep downstream
    transformations stable.
    """

    def __init__(
        self,
        strategies: Sequence[str] | None = None,
        *,
        fallback_factory: Callable[[], Mapping[str, Any]] | None = None,
    ) -> None:
        self.strategies = tuple(strategy.lower() for strategy in (strategies or ()))
        self._fallback_factory = fallback_factory
        self.fallback_data: dict[str, Any] = {}

    def execute_with_fallback(
        self,
        func: Callable[[], PayloadT],
        *,
        fallback_data: Mapping[str, Any]
        | Callable[[], Mapping[str, Any]]
        | None = None,
    ) -> PayloadT:
        """Execute ``func`` and return fallback data on eligible errors."""

        try:
            return func()
        except Exception as exc:  # noqa: BLE001 - we inspect the exception below
            if not self.should_fallback(exc):
                raise

            strategy = self.get_strategy_for_error(exc)
            logger.warning(
                "fallback_manager_fallback_used",
                strategy=strategy,
                error=str(exc),
            )

            resolved = self._resolve_fallback_data(fallback_data)
            return cast(PayloadT, resolved)

    def should_fallback(self, exc: Exception) -> bool:
        """Return ``True`` when the error matches a configured strategy."""

        return self.get_strategy_for_error(exc) is not None

    def get_strategy_for_error(self, exc: Exception) -> str | None:
        """Resolve the strategy name for the provided exception."""

        if isinstance(exc, req_exc.ConnectionError):
            return "network" if "network" in self.strategies else None

        if isinstance(exc, req_exc.Timeout):
            return "timeout" if "timeout" in self.strategies else None

        if isinstance(exc, req_exc.HTTPError):
            response: Response | None = getattr(exc, "response", None)
            if response is not None and 500 <= response.status_code < 600:
                return "5xx" if "5xx" in self.strategies else None

        return None

    def get_fallback_data(self) -> Mapping[str, Any]:
        """Return deterministic fallback data."""

        if self.fallback_data:
            return dict(self.fallback_data)

        if self._fallback_factory is not None:
            try:
                return dict(self._fallback_factory())
            except Exception:  # pragma: no cover - defensive guard
                logger.exception("fallback_factory_failed")

        return {}

    def _resolve_fallback_data(
        self,
        candidate: Mapping[str, Any] | Callable[[], Mapping[str, Any]] | None,
    ) -> Mapping[str, Any]:
        if candidate is None:
            return self.get_fallback_data()

        if callable(candidate):
            return dict(candidate())

        return dict(candidate)

