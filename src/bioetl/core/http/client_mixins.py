from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, TypeVar

import structlog

from bioetl.base_classes import BaseApiClient

_T = TypeVar("_T")


class ApiClientMixin:
    """Общий набор утилит для HTTP-клиентов на базе ``BaseApiClient``."""

    transport: BaseApiClient
    _logger: structlog.stdlib.BoundLogger | structlog.types.BindableLogger

    def _normalize_payload(self, payload: Any) -> Iterator[dict[str, Any]]:
        if isinstance(payload, Mapping):
            results = payload.get("results")
            if isinstance(results, Iterable) and not isinstance(results, (str, bytes, bytearray)):
                for item in results:
                    if isinstance(item, Mapping):
                        yield dict(item)
                return

            yield dict(payload)
            return

        if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
            for item in payload:
                if isinstance(item, Mapping):
                    yield dict(item)
            return

        if payload is not None:
            yield {"result": payload}

    def _wrap_callable(
        self, func: Callable[[], _T], *, log_context: Mapping[str, Any] | None = None
    ) -> _T:
        from bioetl.clients import client_exceptions

        try:
            return func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            context = dict(log_context or {})
            self._logger.error("api_call_failed", error=str(exc), **context)
            raise client_exceptions.RequestException(str(exc)) from exc

    def _wrap_iterator(
        self, func: Callable[[], Iterator[dict[str, Any]]], *, log_context: Mapping[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        from bioetl.clients import client_exceptions

        try:
            yield from func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            context = dict(log_context or {})
            self._logger.error("api_call_failed", error=str(exc), **context)
            raise client_exceptions.RequestException(str(exc)) from exc


class ClosableMixin:
    transport: BaseApiClient
    _logger: structlog.stdlib.BoundLogger | structlog.types.BindableLogger

    def close(self) -> None:
        close = getattr(self.transport, "close", None)
        if callable(close):
            close()

    def iter_ids(
        self, ids: Sequence[str], path_template: str = "/{entity}/{id}"
    ) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            for raw_id in ids:
                entity_id = str(raw_id)
                path = path_template.format(entity=self.entity, id=entity_id)
                payload = self._wrap_callable(
                    lambda: self.transport.request("GET", path),
                    log_context={"path": path},
                )
                self._logger.info("api_call", entity=self.entity, entity_id=entity_id)
                yield from self._normalize_payload(payload)

        return self._wrap_iterator(iterator)


__all__ = ["ApiClientMixin", "ClosableMixin"]
