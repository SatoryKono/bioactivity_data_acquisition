from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Any, Callable, TypeVar

import structlog

from bioetl.clients import client_exceptions

_T = TypeVar("_T")


class ApiClientMixin:
    _logger: structlog.typing.FilteringBoundLogger

    def _wrap_callable(self, func: Callable[[], _T]) -> _T:
        try:
            return func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._logger.error("api_call_failed", error=str(exc))
            raise client_exceptions.RequestException(str(exc)) from exc

    def _wrap_iterator(self, func: Callable[[], Iterator[_T]]) -> Iterator[_T]:
        try:
            yield from func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._logger.error("api_call_failed", error=str(exc))
            raise client_exceptions.RequestException(str(exc)) from exc

    def _normalize_payload(self, payload: Any) -> dict[str, Any] | list[dict[str, Any]]:
        if isinstance(payload, Mapping):
            return dict(payload)
        if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
            return [dict(item) for item in payload if isinstance(item, Mapping)]
        return {"result": payload}

    def _iter_normalized(self, payload: Any) -> Iterator[dict[str, Any]]:
        normalized = self._normalize_payload(payload)
        if isinstance(normalized, Mapping):
            yield dict(normalized)
        else:
            for item in normalized:
                if isinstance(item, Mapping):
                    yield dict(item)


__all__ = ["ApiClientMixin"]
