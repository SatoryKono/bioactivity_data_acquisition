from __future__ import annotations

from typing import Mapping, Sequence

from bioetl.clients.base import ClientRequest, PaginationParams, RequestContext
from bioetl.clients.base.http_backend import HttpBackend
from bioetl.clients.config.loader import load_source_config
from bioetl.clients.config.models import SourceConfig
from bioetl.clients.factory import ConfiguredHttpClient


class PubMedClient(ConfiguredHttpClient):
    source = "pubmed"

    def __init__(self, backend: HttpBackend, *, config: SourceConfig | None = None) -> None:
        cfg = config or load_source_config(self.source)
        super().__init__(config=cfg, backend=backend)
        self.name = f"{self.source}.client"

    def request_articles(
        self,
        *,
        ids: Sequence[str] | None = None,
        filters: Mapping[str, object] | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> ClientRequest:
        return ClientRequest(
            route="article",
            ids=ids,
            filters=filters,
            pagination=pagination,
            context=context,
        )


__all__ = ["PubMedClient"]
