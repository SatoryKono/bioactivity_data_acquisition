from __future__ import annotations

from typing import Any, Mapping, MutableMapping, Sequence

from .exceptions import (
    ConnectionError,
    HTTPError,
    InvalidSchema,
    InvalidURL,
    JSONDecodeError,
    MissingSchema,
    ReadTimeout,
    RequestException,
    RetryError,
    Timeout,
    TooManyRedirects,
    URLRequired,
)
from .models import PreparedRequest, Request, Response

__all__ = [
    "PreparedRequest",
    "Request",
    "Response",
    "Session",
    "exceptions",
    "delete",
    "get",
    "head",
    "options",
    "patch",
    "post",
    "put",
    "request",
    "ConnectionError",
    "HTTPError",
    "InvalidSchema",
    "InvalidURL",
    "JSONDecodeError",
    "MissingSchema",
    "ReadTimeout",
    "RequestException",
    "RetryError",
    "Timeout",
    "TooManyRedirects",
    "URLRequired",
]


class Session:
    headers: MutableMapping[str, str]

    def __init__(self) -> None: ...
    def prepare_request(self, request: Request) -> PreparedRequest: ...
    def request(
        self,
        method: str,
        url: str,
        params: Mapping[str, str] | Sequence[tuple[str, str]] | None = ...,
        data: Any | None = ...,
        headers: Mapping[str, str] | None = ...,
        timeout: float | tuple[float, float] | None = ...,
        **kwargs: Any,
    ) -> Response: ...
    def get(self, url: str, **kwargs: Any) -> Response: ...
    def post(self, url: str, **kwargs: Any) -> Response: ...
    def put(self, url: str, **kwargs: Any) -> Response: ...
    def delete(self, url: str, **kwargs: Any) -> Response: ...
    def head(self, url: str, **kwargs: Any) -> Response: ...
    def options(self, url: str, **kwargs: Any) -> Response: ...
    def patch(self, url: str, **kwargs: Any) -> Response: ...
    def close(self) -> None: ...
    def __enter__(self) -> Session: ...
    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: Any) -> bool | None: ...


class exceptions:
    RequestException = RequestException
    HTTPError = HTTPError
    ConnectionError = ConnectionError
    Timeout = Timeout
    ReadTimeout = ReadTimeout
    TooManyRedirects = TooManyRedirects
    URLRequired = URLRequired
    MissingSchema = MissingSchema
    InvalidSchema = InvalidSchema
    InvalidURL = InvalidURL
    JSONDecodeError = JSONDecodeError
    RetryError = RetryError


def request(
    method: str,
    url: str,
    params: Mapping[str, str] | Sequence[tuple[str, str]] | None = ...,
    data: Any | None = ...,
    headers: Mapping[str, str] | None = ...,
    timeout: float | tuple[float, float] | None = ...,
    **kwargs: Any,
) -> Response: ...

def get(url: str, **kwargs: Any) -> Response: ...
def post(url: str, **kwargs: Any) -> Response: ...
def put(url: str, **kwargs: Any) -> Response: ...
def delete(url: str, **kwargs: Any) -> Response: ...
def head(url: str, **kwargs: Any) -> Response: ...
def options(url: str, **kwargs: Any) -> Response: ...
def patch(url: str, **kwargs: Any) -> Response: ...
