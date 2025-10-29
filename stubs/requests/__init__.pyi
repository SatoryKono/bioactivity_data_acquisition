from typing import Any, MutableMapping
from types import TracebackType


class Response:
    """Minimal subset of ``requests.Response`` used by the project."""

    status_code: int
    headers: MutableMapping[str, str]
    text: str
    content: bytes
    url: str

    def json(self, **kwargs: Any) -> Any: ...
    def raise_for_status(self) -> None: ...


class PreparedRequest:
    method: str
    url: str
    headers: MutableMapping[str, str]
    body: bytes | None


class Session:
    headers: MutableMapping[str, str]

    def __init__(self) -> None: ...
    def __enter__(self) -> "Session": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    def request(self, method: str, url: str, **kwargs: Any) -> Response: ...
    def get(self, url: str, **kwargs: Any) -> Response: ...
    def post(self, url: str, **kwargs: Any) -> Response: ...
    def close(self) -> None: ...


def request(method: str, url: str, **kwargs: Any) -> Response: ...
def get(url: str, **kwargs: Any) -> Response: ...
def post(url: str, **kwargs: Any) -> Response: ...


def Request(method: str, url: str, **kwargs: Any) -> PreparedRequest: ...


__all__ = [
    "PreparedRequest",
    "Request",
    "Response",
    "Session",
    "request",
    "get",
    "post",
    "exceptions",
]

from . import exceptions as exceptions
