from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any
import warnings

import structlog

from bioetl.core.http import ApiClientMixin, ClosableMixin
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy


class BaseApiEntityClient(ApiClientMixin, ClosableMixin):
    """Базовый клиент сущности API с общей логикой обхода записей."""

    def __init__(self, transport: ApiTransportProtocol, pagination: PaginationStrategy, *, entity: str) -> None:
        self.transport = transport
        self.entity = entity.strip("/")
        self.pagination_strategy = pagination
        self._logger = structlog.get_logger(__name__).bind(entity=self.entity)

    def _entity_path(self, suffix: str | None = None) -> str:
        if not suffix:
            return f"/{self.entity}"
        suffix = str(suffix).lstrip("/")
        return f"/{self.entity}/{suffix}"

    def iter_ids(
        self,
        ids: Sequence[str],
        path_template: str = "/{entity}/{id}",
    ) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            for raw_id in ids:
                entity_id = str(raw_id)
                path = path_template.format(entity=self.entity, id=entity_id)
                payload = self._wrap_callable(
                    lambda: self._transport().request("GET", path),
                    log_context={"path": path},
                )
                self._logger.info("api_call", entity=self.entity, entity_id=entity_id)
                yield from self._normalize_payload(payload)

        return self._wrap_iterator(iterator)

    def get(self, entity_id: str, *, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        return self._wrap_callable(
            lambda: self._transport().request("GET", self._entity_path(entity_id), params=params),
            log_context={"path": self._entity_path(entity_id)},
        )

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        return self.iter_ids(ids)

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str] | None], Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            if callable(fetcher):
                result = fetcher(ids)
                if isinstance(result, Iterator):
                    yield from result
                    return
                if result is not None:
                    yield from self._normalize_payload(result)
                    return

            if ids:
                yield from self.fetch_by_ids(ids)
                return

            yield from self.list(page_size=page_size or 1000)

        return self._wrap_iterator(iterator)

    def list(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            query_params: dict[str, Any] = {"limit": page_size}
            if params:
                query_params.update(params)

            first_payload = self._wrap_callable(
                lambda: self._transport().request("GET", self._entity_path(), params=query_params),
                log_context={"path": self._entity_path()},
            )
            self._logger.info("api_call", path=self._entity_path())

            for page in self.pagination_strategy.iter_pages(
                first_payload,
                self._transport(),
                endpoint=self._entity_path(),
                params=query_params,
                logger=self._logger,
                page_key=page_key,
                next_key=next_key,
                page_param=page_param,
                normalize=self._normalize_payload,
            ):
                yield from self._normalize_payload(page, page_key=None)

        def wrapped_iterator() -> Iterator[dict[str, Any]]:
            original_page_key_override = getattr(self, "_page_key_override", None)
            self._page_key_override = page_key
            try:
                yield from self._wrap_iterator(iterator)
            finally:
                self._page_key_override = original_page_key_override

        return wrapped_iterator()

    def fetch_all(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[dict[str, Any]]:
        warnings.warn(
            "fetch_all is deprecated; use list instead to enumerate entities.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.list(
            page_size=page_size,
            params=params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    def search(self, params: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
        return self.list(params=params)


__all__ = ["BaseApiEntityClient"]
