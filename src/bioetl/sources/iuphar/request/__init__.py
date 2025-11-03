"""Request builders for the IUPHAR API."""

from __future__ import annotations

from typing import Any, Mapping

__all__ = [
    "DEFAULT_PAGE_SIZE",
    "build_targets_request",
    "build_families_request",
    "build_request_params",
]

DEFAULT_PAGE_SIZE = 200


def _normalise_params(params: Mapping[str, Any] | None) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    if params:
        for key, value in params.items():
            if value is None:
                continue
            resolved[str(key)] = value
    return resolved


def build_targets_request(
    *,
    page: int | None = None,
    page_size: int | None = None,
    annotation_status: str = "CURATED",
    extra_params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Construct request parameters for the ``/targets`` endpoint."""

    params = _normalise_params(extra_params)
    params.setdefault("annotationStatus", annotation_status)
    if page is not None:
        params.setdefault("page", page)
    if page_size is not None:
        params.setdefault("pageSize", page_size)
    return params


def build_families_request(
    *,
    page: int | None = None,
    page_size: int | None = None,
    extra_params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Construct request parameters for the ``/targets/families`` endpoint."""

    params = _normalise_params(extra_params)
    if page is not None:
        params.setdefault("page", page)
    if page_size is not None:
        params.setdefault("pageSize", page_size)
    return params


def build_request_params(
    *,
    page: int | None = None,
    page_size: int | None = None,
    annotation_status: str = "CURATED",
    extra_params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Generic helper for constructing paginated request parameters."""

    params = _normalise_params(extra_params)
    if annotation_status:
        params.setdefault("annotationStatus", annotation_status)
    if page is not None:
        params.setdefault("page", page)
    if page_size is not None:
        params.setdefault("pageSize", page_size)
    return params
