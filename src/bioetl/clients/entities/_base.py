from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, MutableMapping, Sequence
from typing import Any, Callable

import structlog

from bioetl.base_classes import BaseApiClient
from bioetl.clients import client_exceptions


class _BaseEntityClient:
    def __init__(self, api_client: BaseApiClient, entity: str) -> None:
        self.api_client = api_client
        self.entity = entity.strip("/")
        self._logger = structlog.get_logger(__name__).bind(entity=self.entity)

    def _iter_payload(self, payload: Any) -> Iterator[dict[str, Any]]:
        if isinstance(payload, Mapping):
            yield dict(payload)
        elif isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
            for item in payload:
                if isinstance(item, Mapping):
                    yield dict(item)

    def _wrap_iterator(self, func: Callable[[], Iterator[dict[str, Any]]]) -> Iterator[dict[str, Any]]:
        try:
            yield from func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._logger.error("api_call_failed", entity=self.entity, error=str(exc))
            raise client_exceptions.RequestException(str(exc)) from exc

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            for entity_id in ids:
                payload = self.api_client.get_json(f"/{self.entity}/{entity_id}")
                self._logger.info("api_call", entity=self.entity, entity_id=str(entity_id))
                yield from self._iter_payload(payload)

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
                if not isinstance(payload, Mapping):
                    yield from self._iter_payload(payload)
                    continue

                items = payload.get("results")
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, Mapping):
                            yield dict(item)
                elif payload:
                    yield dict(payload)

        return self._wrap_iterator(iterator)
