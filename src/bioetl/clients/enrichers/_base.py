from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

import structlog

from bioetl.clients.common import BaseApiClient, JSONRecordStream
from bioetl.core.http import ApiClientMixin, ClosableMixin


class _BaseEnricherClient(ClosableMixin, ApiClientMixin):
    def __init__(self, api_client: BaseApiClient, source: str) -> None:
        self.api_client = api_client
        self._logger = structlog.get_logger(__name__).bind(source=source)

    def _get(
        self, path: str, *, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        def iterator() -> Iterator[dict[str, Any]]:
            payload = self._wrap_callable(
                lambda: self.api_client.get_json(path, params=params), log_context={"path": path}
            )
            self._logger.info("api_call", path=path)

            yielded = False
            for item in self._normalize_payload(payload):
                yielded = True
                yield item

            if not yielded and payload is not None:
                yield {"result": payload}

        return self._wrap_iterator(iterator, log_context={"path": path})


__all__ = ["_BaseEnricherClient"]
