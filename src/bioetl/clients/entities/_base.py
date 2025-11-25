from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping, Sequence
from typing import Any

import structlog

from bioetl.base_classes import BaseApiClient
from bioetl.clients.common import ApiClientMixin


class _BaseEntityClient(ApiClientMixin):
    def __init__(self, api_client: BaseApiClient, entity: str) -> None:
        self.api_client = api_client
        self.entity = entity.strip("/")
        self._logger = structlog.get_logger(__name__).bind(entity=self.entity)

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            for entity_id in ids:
                payload = self.api_client.get_json(f"/{self.entity}/{entity_id}")
                self._logger.info("api_call", entity=self.entity, entity_id=str(entity_id))
                yield from self._iter_normalized(payload)

        return self._wrap_iterator(iterator)

    def fetch_all(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            query_params: MutableMapping[str, Any] = {"limit": page_size}
            if params:
                query_params.update(params)

            for payload in self.api_client.paginate_json(
                f"/{self.entity}", params=query_params, page_key="results", next_key="next", page_param=None
            ):
                normalized = self._normalize_payload(payload)

                if isinstance(normalized, Mapping):
                    items = normalized.get("results")
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, Mapping):
                                yield dict(item)
                    elif normalized:
                        yield dict(normalized)
                else:
                    for item in normalized:
                        if isinstance(item, Mapping):
                            yield dict(item)

        return self._wrap_iterator(iterator)
