"""Request builder module for constructing HTTP requests."""

from __future__ import annotations

from typing import TYPE_CHECKING, Mapping, cast
from urllib.parse import urljoin

import requests

if TYPE_CHECKING:
    from bioetl.core.http.config import APIConfig


class RequestBuilder:
    """Create and configure ``requests.Session`` and build final URLs."""

    def __init__(
        self,
        config: "APIConfig",
        *,
        session: requests.Session | None = None,
        verify_ssl: bool = True,
        default_headers: Mapping[str, str] | None = None,
    ) -> None:
        self._config = config
        self._session = session or requests.Session()
        self._prepare_session(
            verify_ssl=verify_ssl, default_headers=default_headers
        )

    @property
    def session(self) -> requests.Session:
        """Return the underlying requests.Session."""
        return self._session

    def close(self) -> None:
        """Close the underlying session."""
        self._session.close()

    def build_url(self, path: str) -> str:
        """
        Build an absolute URL from a path.

        Args:
            path: Relative path or absolute URL.

        Returns:
            Absolute URL joined with base_url if path is relative.
        """
        if path.startswith("http://") or path.startswith("https://"):
            return path
        base = self._config.base_url.rstrip("/") + "/"
        return urljoin(base, path.lstrip("/"))

    def merge_headers(
        self, headers: Mapping[str, str] | None = None
    ) -> Mapping[str, str]:
        """
        Merge session headers with request-specific headers.

        Args:
            headers: Request-specific headers to override session defaults.

        Returns:
            Merged headers dictionary.
        """
        session_headers = cast(Mapping[str, str], self._session.headers)
        return {**dict(session_headers), **(headers or {})}

    def _prepare_session(
        self, *, verify_ssl: bool, default_headers: Mapping[str, str] | None
    ) -> None:
        headers = {
            "User-Agent": self._config.user_agent,
            **self._config.default_headers,
        }
        if default_headers:
            headers.update(default_headers)
        self._session.headers.update(headers)
        self._session.verify = verify_ssl


__all__ = ["RequestBuilder"]
