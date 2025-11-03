"""Shared request building primitives for external API integrations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import uuid4

from bioetl.core.api_client import APIConfig

__all__ = ["RequestSpec", "BaseRequestBuilder"]


@dataclass(frozen=True, slots=True)
class RequestSpec:
    """Container describing how to execute an HTTP request."""

    url: str
    params: dict[str, Any]
    headers: dict[str, str]
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseRequestBuilder:
    """Base helper providing deterministic request construction."""

    def __init__(self, api_config: APIConfig):
        self._api_config = api_config
        self._base_url = api_config.base_url.rstrip("/")
        self._base_headers = dict(api_config.headers or {})

    @property
    def base_headers(self) -> dict[str, str]:
        """Return a copy of the configured default headers."""

        return dict(self._base_headers)

    def build(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        request_id: str | None = None,
        page: int | str | None = None,
    ) -> RequestSpec:
        """Build a :class:`RequestSpec` with deterministic query parameters."""

        url = self._resolve_url(path)
        sorted_params = self._sorted_params(params)
        merged_headers = self.base_headers
        if headers:
            merged_headers.update(headers)

        metadata: dict[str, Any] = {"request_id": request_id or str(uuid4())}
        if page is not None:
            metadata["page"] = page

        return RequestSpec(url=url, params=sorted_params, headers=merged_headers, metadata=metadata)

    def _resolve_url(self, path: str) -> str:
        if path.startswith("http"):
            return path
        return f"{self._base_url}/{path.lstrip('/')}"

    @staticmethod
    def _sorted_params(params: Mapping[str, Any] | None) -> dict[str, Any]:
        if not params:
            return {}

        ordered: dict[str, Any] = {}
        for key in sorted(params.keys(), key=lambda item: str(item)):
            ordered[str(key)] = params[key]
        return ordered
