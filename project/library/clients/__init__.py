"""HTTP clients for publication sources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

import backoff
import requests

from library.utils.errors import ExtractionError
from library.utils.logging import get_logger
from library.utils.rate_limit import RateLimiter


@dataclass
class ClientConfig:
    """Configuration for a publication client."""

    name: str
    base_url: str
    api_key: str | None = None
    rate_limit_per_minute: int | None = None
    extra_headers: Mapping[str, str] | None = None


class BasePublicationsClient:
    """Base class encapsulating resilient HTTP access."""

    def __init__(self, config: ClientConfig, session: Optional[requests.Session] = None) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.logger = get_logger(config.name)
        self.rate_limiter = RateLimiter(config.rate_limit_per_minute)

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5, jitter=None)
    def _get(self, endpoint: str, *, params: Mapping[str, Any] | None = None) -> Any:
        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        headers = dict(self.config.extra_headers or {})
        if self.config.api_key and "authorization" not in {key.lower() for key in headers}:
            headers["Authorization"] = f"Bearer {self.config.api_key}"

        self.rate_limiter.throttle()
        response = self.session.get(url, params=params, headers=headers, timeout=30)
        if response.status_code >= 400:
            raise ExtractionError(
                f"{self.config.name} responded with {response.status_code}: {response.text[:200]}"
            )
        return response.json()

    def fetch_publications(self, query: str) -> list[dict[str, Any]]:
        """Fetch publications for the provided query."""

        raise NotImplementedError
