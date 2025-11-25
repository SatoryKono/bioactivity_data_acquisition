from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import structlog

from bioetl.base_classes import BaseApiClient
from bioetl.clients import client_exceptions


class _BaseEnricherClient:
    def __init__(self, api_client: BaseApiClient, source: str) -> None:
        self.api_client = api_client
        self._logger = structlog.get_logger(__name__).bind(source=source)

    def _get(self, path: str, *, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        try:
            payload = self.api_client.get_json(path, params=params)
            self._logger.info("api_call", path=path)
            if isinstance(payload, Mapping):
                return dict(payload)
            if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
                return {"results": [dict(item) for item in payload if isinstance(item, Mapping)]}
            return {"result": payload}
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._logger.error("api_call_failed", path=path, error=str(exc))
            raise client_exceptions.RequestException(str(exc)) from exc


__all__ = ["_BaseEnricherClient"]
