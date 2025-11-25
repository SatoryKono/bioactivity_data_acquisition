from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any

import structlog

from bioetl.base_classes import BaseApiClient, JSONRecordStream
from bioetl.clients import client_exceptions


class _BaseEnricherClient:
    def __init__(self, api_client: BaseApiClient, source: str) -> None:
        self.api_client = api_client
        self._logger = structlog.get_logger(__name__).bind(source=source)

    def _get(
        self, path: str, *, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        def iterator() -> Iterator[dict[str, Any]]:
            payload = self.api_client.get_json(path, params=params)
            self._logger.info("api_call", path=path)

            yielded = False
            for item in self._iter_payload(payload):
                yielded = True
                yield item

            if not yielded and payload is not None:
                yield {"result": payload}

        return self._wrap_iterator(iterator, path)

    def _iter_payload(self, payload: Any) -> Iterator[dict[str, Any]]:
        if isinstance(payload, Mapping):
            results = payload.get("results")
            if isinstance(results, Iterable) and not isinstance(
                results, (str, bytes, bytearray)
            ):
                for item in results:
                    if isinstance(item, Mapping):
                        yield dict(item)
                return
            if payload:
                yield dict(payload)
            return

        if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
            for item in payload:
                if isinstance(item, Mapping):
                    yield dict(item)

    def _wrap_iterator(
        self, func: Callable[[], Iterator[dict[str, Any]]], path: str
    ) -> Iterator[dict[str, Any]]:
        try:
            yield from func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._logger.error("api_call_failed", path=path, error=str(exc))
            raise client_exceptions.RequestException(str(exc)) from exc

    def close(self) -> None:
        close = getattr(self.api_client, "close", None)
        if callable(close):
            close()


__all__ = ["_BaseEnricherClient"]
