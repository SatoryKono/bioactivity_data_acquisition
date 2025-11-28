"""Высокоуровневый фасад для унифицированного доступа к ChEMBL-клиентам."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass, field
from typing import Any, Callable

from bioetl.clients.chembl.entities import ChemblEntityClientFactoryProtocol


def _normalize_fetch_result(
    result: Any,
    *,
    default_api_calls: int = 1,
) -> tuple[Any, dict[str, Any]]:
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], Mapping):
        payload, meta = result
        normalized_meta = {"api_calls": default_api_calls, **dict(meta)}
        return payload, normalized_meta
    return result, {"api_calls": default_api_calls}


@dataclass(slots=True)
class ChemblClientFacade:
    """Упрощённый фасад для ChEMBL с единообразной пагинацией и статусами."""

    entity_name: str
    client_factory: ChemblEntityClientFactoryProtocol | None = None
    entity_fetcher: Callable[[Sequence[str] | None], Any] | None = None
    default_page_size: int = 1000
    _client_factory_fn: Callable[[str], Any] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self._client_factory_fn is None and self.client_factory is not None:
            self._client_factory_fn = self.client_factory.create

    def _acquire_client(self) -> Any:
        if self._client_factory_fn is None:
            msg = "client_factory is required to build ChemblClientFacade"
            raise RuntimeError(msg)
        return self._client_factory_fn(self.entity_name)

    def _run_fetcher(
        self,
        batch: Sequence[str] | None,
        *,
        page_size: int | None,
        client_settings: Mapping[str, Any] | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        if self.entity_fetcher is not None:
            payload, meta = _normalize_fetch_result(
                self.entity_fetcher(batch), default_api_calls=1
            )
            return payload, meta

        page_size = page_size or self.default_page_size
        with closing(self._acquire_client()) as client:
            settings = dict(client_settings or {})
            if batch is not None:
                iterator = client.fetch_batch(batch, **settings)
            else:
                iterator = client.fetch_many(page_size=page_size, **settings)
            payload = list(iterator) if isinstance(iterator, Iterable) else iterator
            return payload, {"api_calls": 1, "cache_hit": False}

    def status(self) -> Mapping[str, Any]:
        status_callable = None
        if self.entity_fetcher and hasattr(self.entity_fetcher, "status"):
            status_callable = getattr(self.entity_fetcher, "status")
        if callable(status_callable):
            result = status_callable()
            return result if isinstance(result, Mapping) else {}

        with closing(self._acquire_client()) as client:
            status_result = client.status()
            return status_result if isinstance(status_result, Mapping) else {}

    def fetch_by_ids(
        self,
        ids: Sequence[str],
        *,
        page_size: int | None = None,
        client_settings: Mapping[str, Any] | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        return self._run_fetcher(ids, page_size=page_size, client_settings=client_settings)

    def fetch_batch(
        self,
        ids: Sequence[str],
        *,
        page_size: int | None = None,
        client_settings: Mapping[str, Any] | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        return self.fetch_by_ids(ids, page_size=page_size, client_settings=client_settings)

    def list(
        self,
        *,
        page_size: int | None = None,
        client_settings: Mapping[str, Any] | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        payload, meta = self._run_fetcher(
            None, page_size=page_size, client_settings=client_settings
        )
        meta.setdefault("api_calls", 1)
        return payload, meta

    def fetch_many(
        self,
        *,
        page_size: int | None = None,
        client_settings: Mapping[str, Any] | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        return self.list(page_size=page_size, client_settings=client_settings)


__all__ = ["ChemblClientFacade"]
