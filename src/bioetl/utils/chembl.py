"""Utilities for interacting with the ChEMBL API."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urljoin

import requests

try:  # pragma: no cover - pandas is available in production but optional for tests
    import pandas as _pd
except Exception:  # pragma: no cover - fallback when pandas is unavailable
    _pd = None


__all__ = [
    "ChemblRelease",
    "SupportsRequestJson",
    "fetch_chembl_release",
    "resolve_release_name",
    "_resolve_release_name",
]


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


_RELEASE_KEYS: tuple[str, ...] = (
    "chembl_db_version",
    "chembl_db_release",
    "chembl_release",
    "chembl_release_version",
    "version",
    "name",
)


def _is_missing_value(value: Any) -> bool:
    """Return ``True`` when ``value`` should be considered missing."""

    if value is None:
        return True

    if _pd is not None:
        try:
            if _pd.isna(value):
                return True
        except (TypeError, ValueError):
            return False

    return False


def _coerce_release_value(value: Any) -> str | None:
    """Coerce ``value`` to a trimmed string if it contains meaningful data."""

    if _is_missing_value(value):
        return None

    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None

    return str(value).strip() or None


def resolve_release_name(payload: Any) -> str | None:
    """Return the first non-empty release identifier from ``payload``."""

    if isinstance(payload, Mapping):
        for key in _RELEASE_KEYS:
            if key in payload:
                candidate = resolve_release_name(payload[key])
                if candidate:
                    return candidate
        return None

    return _coerce_release_value(payload)


# Backwards compatibility alias for legacy import paths.
_resolve_release_name = resolve_release_name


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

    version = resolve_release_name(status)
    return ChemblRelease(version=version, status=status)
