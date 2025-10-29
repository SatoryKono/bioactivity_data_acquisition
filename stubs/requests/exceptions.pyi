from __future__ import annotations

from typing import Any


class RequestException(Exception):
    request: Any | None
    response: Any | None


class HTTPError(RequestException):
    ...


class Timeout(RequestException):
    ...


class ReadTimeout(Timeout):
    ...


class ConnectionError(RequestException):
    ...
