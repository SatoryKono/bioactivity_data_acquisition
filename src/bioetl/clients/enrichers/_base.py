from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import structlog

from bioetl.base_classes import BaseApiClient
from bioetl.clients.common import ApiClientMixin


class _BaseEnricherClient(ApiClientMixin):
    def __init__(self, api_client: BaseApiClient, source: str) -> None:
        self.api_client = api_client
        self._logger = structlog.get_logger(__name__).bind(source=source)

    def _get(self, path: str, *, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        def caller() -> dict[str, Any]:
            payload = self.api_client.get_json(path, params=params)
            self._logger.info("api_call", path=path)
            normalized = self._normalize_payload(payload)
            if isinstance(normalized, Iterable) and not isinstance(normalized, Mapping):
                return {"results": [dict(item) for item in normalized if isinstance(item, Mapping)]}
            return dict(normalized)

        return self._wrap_callable(caller)


__all__ = ["_BaseEnricherClient"]
