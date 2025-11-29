from __future__ import annotations

from typing import Protocol

from bioetl.clients.base.client import (
    ClientRequest,
    PageStream,
    Record,
    RecordStream,
    RequestContext,
)


class HttpTransport(Protocol):
    """
    Абстракция HTTP-транспорта для клиентского слоя.

    Реализация этого протокола:
    - знает base_url и детали API конкретного источника;
    - реализует пагинацию, ретраи, таймауты, логирование, кэш и т.п.;
    - возвращает сырые записи (Record / Page) без доменных преобразований.
      Клиенты не должны знать эти детали.
    """

    def fetch_one(
        self,
        *,
        endpoint: str,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Record | None:
        ...

    def iter_records(
        self,
        *,
        endpoint: str,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> RecordStream:
        ...

    def iter_pages(
        self,
        *,
        endpoint: str,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> PageStream:
        ...
