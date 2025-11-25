from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping, Sequence
from typing import Any

import structlog

from bioetl.base_classes import BaseApiClient
from bioetl.clients.common import ApiClientMixin
from bioetl.core.pipeline.unified import ChemblExtractionDescriptor


class BaseChemblClient(ApiClientMixin, BaseApiClient):
    def __init__(self, api_client: BaseApiClient, entity: str) -> None:
        self.api_client = api_client
        self.entity = entity.strip("/")
        self._logger = structlog.get_logger(__name__).bind(entity=self.entity)

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        return self.iter_ids(ids, "/{entity}/{id}")

    def fetch_all(self, page_size: int = 1000) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            next_path: str | None = f"/{self.entity}"
            params: MutableMapping[str, Any] = {"limit": page_size}
            while next_path:
                payload = self._wrap_callable(
                    lambda: self.api_client.get_json(next_path, params=params),
                    log_context={"entity": self.entity, "path": next_path},
                )
                self._logger.info("api_call", entity=self.entity, path=next_path)
                next_path = None
                params = {}

                if isinstance(payload, Mapping):
                    next_candidate = payload.get("next")
                    next_path = next_candidate if isinstance(next_candidate, str) else None

                yielded = False
                for item in self._normalize_payload(payload):
                    yielded = True
                    yield item

                if not yielded and payload:
                    yield {"result": payload}

        return self._wrap_iterator(iterator, log_context={"entity": self.entity})

    def close(self) -> None:
        close = getattr(self.api_client, "close", None)
        if callable(close):
            close()

    def iterate_records(self, descriptor: ChemblExtractionDescriptor) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            context: Mapping[str, Any] | None = None
            try:
                context = descriptor.build_context(self)
            except Exception:
                context = None

            ids: Sequence[str] | None = None
            page_size = 1000
            if isinstance(context, Mapping):
                ids_value = context.get("ids")
                if isinstance(ids_value, Sequence) and not isinstance(ids_value, (str, bytes, bytearray)):
                    ids = [str(item) for item in ids_value]
                page_size_value = context.get("page_size")
                if isinstance(page_size_value, int):
                    page_size = page_size_value

            fetcher_factory = getattr(descriptor, "fetcher_factory", None)
            if callable(fetcher_factory):
                fetcher = fetcher_factory(context or {})
                if callable(fetcher):
                    result = fetcher(ids)
                    if isinstance(result, Iterator):
                        yield from result
                        return
                    if isinstance(result, Sequence) and not isinstance(result, (str, bytes, bytearray)):
                        for item in result:
                            if isinstance(item, Mapping):
                                yield dict(item)
                        return
                    if isinstance(result, Mapping):
                        yield dict(result)
                        return

            if ids:
                for item in self.fetch_by_ids(ids):
                    yield item
                return

            yield from self.fetch_all(page_size=page_size)

        return self._wrap_iterator(iterator, log_context={"entity": self.entity})


__all__ = ["BaseChemblClient"]
