"""Base HTTP client implementation with retry support."""
from __future__ import annotations

from typing import Any

import backoff
import requests
from requests import Response


class BaseClient:
    """Base class for HTTP clients with backoff retries."""

    def __init__(
        self,
        base_url: Any,
        *,
        timeout: float = 10.0,
        headers: dict[str, str] | None = None,
        max_tries: int = 5,
    ) -> None:
        self.base_url = str(base_url).rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(headers or {})
        self.max_tries = max_tries

    def close(self) -> None:
        """Close the underlying session."""
        self.session.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> Response:
        url = f"{self.base_url}{path}"
        @backoff.on_exception(  # type: ignore[misc]
            backoff.expo,
            (requests.exceptions.RequestException,),
            max_tries=self.max_tries,
            jitter=None,
        )
        def do_request() -> Response:
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)
            response.raise_for_status()
            return response

        return do_request()

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Issue a GET request returning JSON."""
        response = self._request("GET", path, params=params)
        return response.json()
