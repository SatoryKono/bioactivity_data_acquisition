from __future__ import annotations

"""Клиент ChEMBL на новой архитектуре."""

from dataclasses import dataclass
from typing import Mapping, Sequence

from bioetl.clients.base import ClientRequest, PaginationParams, RequestContext
from bioetl.clients.config.loader import load_source_config
from bioetl.clients.config.models import SourceConfig
from bioetl.clients.factory import ConfiguredHttpClient
from bioetl.clients.base.http_backend import HttpBackend


@dataclass(frozen=True)
class ChemblRequestBuilder:
    source_config: SourceConfig

    def build(
        self,
        *,
        route: str,
        ids: Sequence[str] | None = None,
        filters: Mapping[str, object] | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> ClientRequest:
        return ClientRequest(
            route=route,
            ids=ids,
            filters=filters,
            pagination=pagination,
            context=context,
        )


class ChemblClient(ConfiguredHttpClient):
    source = "chembl"

    def __init__(
        self,
        backend: HttpBackend,
        *,
        config: SourceConfig | None = None,
    ) -> None:
        cfg = config or load_source_config(self.source)
        super().__init__(config=cfg, backend=backend)
        self.name = f"{self.source}.client"
        self.requests = ChemblRequestBuilder(cfg)

    def request_activity(
        self,
        *,
        ids: Sequence[str] | None = None,
        filters: Mapping[str, object] | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> ClientRequest:
        return self.requests.build(
            route="activity",
            ids=ids,
            filters=filters,
            pagination=pagination,
            context=context,
        )


__all__ = ["ChemblClient", "ChemblRequestBuilder"]
