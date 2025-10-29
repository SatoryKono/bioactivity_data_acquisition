from __future__ import annotations

from typing import Any

from .models import Request, Response

class RequestException(OSError):
    response: Response | None
    request: Request | None


class HTTPError(RequestException):
    ...


class Timeout(RequestException):
    ...


class ReadTimeout(Timeout):
    ...


class ConnectionError(RequestException):
    request: Request | None
    response: Response | None


class TooManyRedirects(RequestException):
    ...


class URLRequired(RequestException):
    ...


class MissingSchema(RequestException):
    ...


class InvalidSchema(RequestException):
    ...


class InvalidURL(RequestException):
    ...


class JSONDecodeError(RequestException):
    msg: str
    doc: str
    pos: int


class RetryError(RequestException):
    reason: Any
