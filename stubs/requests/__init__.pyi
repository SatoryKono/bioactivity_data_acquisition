from __future__ import annotations

from typing import Any, Iterable, Mapping, MutableMapping, Sequence

from . import exceptions as exceptions

__all__ = [
    "PreparedRequest",
    "Request",
    "Response",
    "Session",
    "exceptions",
    "get",
    "post",
    "request",
]


class PreparedRequest:
    method: str | None
    url: str | None
    headers: MutableMapping[str, str]
    body: bytes | str | None

    def prepare(self, method: str, url: str, **kwargs: Any) -> None: ...


class Response:
    status_code: int
    headers: MutableMapping[str, str]
    text: str
    content: bytes
    url: str
    reason: str
    request: PreparedRequest

    def json(self, **kwargs: Any) -> Any: ...
    def raise_for_status(self) -> None: ...


class Request:
    method: str
    url: str
    headers: MutableMapping[str, str]
    params: Mapping[str, str] | Sequence[tuple[str, str]] | None
    data: Any

    def prepare(self) -> PreparedRequest: ...


class Session:
    headers: MutableMapping[str, str]

    def __init__(self) -> None: ...
    def prepare_request(self, request: Request) -> PreparedRequest: ...
    def request(self, method: str, url: str, **kwargs: Any) -> Response: ...
    def get(self, url: str, **kwargs: Any) -> Response: ...
    def post(self, url: str, **kwargs: Any) -> Response: ...
    def close(self) -> None: ...


def request(method: str, url: str, **kwargs: Any) -> Response: ...
def get(url: str, **kwargs: Any) -> Response: ...
def post(url: str, **kwargs: Any) -> Response: ...

