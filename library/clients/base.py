"""Shared utilities for HTTP clients."""
from __future__ import annotations

from typing import Any, Mapping
from urllib.parse import urljoin

import backoff
import requests


class BaseClient:
    """Basic resilient HTTP client with JSON parsing."""

    def __init__(self, base_url: str, timeout: float = 10.0, session: requests.Session | None = None):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()

    @backoff.on_exception(backoff.expo, (requests.RequestException,), max_tries=5)
    def _get(self, path: str, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        url = urljoin(f"{self.base_url}/", path.lstrip("/"))
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
