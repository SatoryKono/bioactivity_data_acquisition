"""HTTP client base class with rate limiting and shared session injection."""

from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from library.utils import logging as logging_utils
from library.utils import rate_limit


class BaseHttpClient:
    """Common functionality for HTTP clients.

    Parameters
    ----------
    base_url:
        Root endpoint of the remote API.
    session:
        Optional pre-configured :class:`requests.Session`. When omitted the
        shared cached session from :mod:`library.utils.logging` is used.
    rate_limiter:
        Optional composite limiter returned by :func:`rate_limit.get_rate_limiter`.
    """

    client_name = "base"

    def __init__(
        self,
        base_url: str,
        *,
        session: Optional[requests.Session] = None,
        rate_limiter: Optional[rate_limit.RateLimiterSet] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session or logging_utils.get_shared_session()
        self._rate_limiter = rate_limiter or rate_limit.get_rate_limiter(self.client_name)

    @property
    def rate_limiter(self) -> rate_limit.RateLimiterSet:
        return self._rate_limiter

    def build_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path:
            return self.base_url
        return f"{self.base_url}/{path.lstrip('/')}"

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        self._rate_limiter.acquire()
        response = self.session.request(method, self.build_url(path), **kwargs)
        return response

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> requests.Response:
        return self.request("GET", path, params=params, **kwargs)

    def post(self, path: str, json: Optional[Dict[str, Any]] = None, **kwargs: Any) -> requests.Response:
        return self.request("POST", path, json=json, **kwargs)
