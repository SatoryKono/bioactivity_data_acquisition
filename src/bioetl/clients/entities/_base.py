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
        return self.iter_ids(ids, "/{entity}/{id}")

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

            page_iterator = self._wrap_callable(
                lambda: self.api_client.paginate_json(
                    f"/{self.entity}",
                    params=query_params,
                    page_key="results",
                    next_key="next",
                    page_param=None,
                ),
                log_context={"entity": self.entity},
            )

            for payload in page_iterator:
                yielded = False
                for item in self._normalize_payload(payload):
                    yielded = True
                    yield item

                if not yielded and payload:
                    yield {"result": payload}

        return self._wrap_iterator(iterator, log_context={"entity": self.entity})
