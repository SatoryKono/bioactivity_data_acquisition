"""Utilities for interacting with the ChEMBL API."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urljoin

import requests

__all__ = ["ChemblRelease", "SupportsRequestJson", "fetch_chembl_release"]


class SupportsRequestJson(Protocol):
    """Protocol describing the minimal ``request_json`` capability we rely on."""

    def request_json(
        self,
        url: str,
        params: Mapping[str, Any] | None = ...,
        method: str = ...,
        data: Mapping[str, Any] | None = ...,
        json: Mapping[str, Any] | None = ...,
        headers: Mapping[str, str] | None = ...,
    ) -> Mapping[str, Any]:
        """Perform an HTTP request and return the parsed JSON payload."""


@dataclass(frozen=True)
class ChemblRelease:
    """Container describing the resolved ChEMBL release metadata."""

    version: str | None
    status: Mapping[str, Any] | None


def _request_status(base_url: str) -> Mapping[str, Any]:
    """Fetch the ChEMBL status payload from an absolute base URL."""

    full_url = urljoin(base_url.rstrip("/") + "/", "status.json")
    response = requests.get(full_url, timeout=30)
    response.raise_for_status()
    payload: Mapping[str, Any] = response.json()
    return payload


def fetch_chembl_release(
    api_client: SupportsRequestJson | str,
) -> ChemblRelease:
    """Fetch the ChEMBL release information using the provided client or URL.

    Args:
        api_client: Either a client implementing :class:`SupportsRequestJson`
            or a base URL string. When a string is provided the helper will
            perform a direct HTTP GET request.

    Returns:
        ``ChemblRelease`` capturing both the version string (if present) and the
        raw status payload returned by the API.

    Raises:
        requests.HTTPError: If the HTTP request fails when a base URL string is
            supplied.
        Exception: Re-raised from the provided client implementation.
    """

    if isinstance(api_client, str):
        status = _request_status(api_client)
    else:
        status = api_client.request_json("/status.json")

    version = status.get("chembl_db_version") if status is not None else None
    version_str = str(version) if version is not None else None
    return ChemblRelease(version=version_str, status=status)
