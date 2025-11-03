from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bioetl.sources.iuphar.request import (
    DEFAULT_PAGE_SIZE,
    build_families_request,
    build_targets_request,
)

__all__ = ["IupharRequestBuilder"]


@dataclass(slots=True)
class IupharRequestBuilder:
    """Factory for generating IUPHAR request parameters with pagination defaults."""

    page_size: int = DEFAULT_PAGE_SIZE

    def targets(
        self,
        *,
        page: int | None = None,
        annotation_status: str = "CURATED",
        extra_params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build parameters for the ``/targets`` endpoint."""

        return build_targets_request(
            page=page,
            page_size=self.page_size,
            annotation_status=annotation_status,
            extra_params=extra_params,
        )

    def families(
        self,
        *,
        page: int | None = None,
        extra_params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build parameters for the ``/targets/families`` endpoint."""

        return build_families_request(
            page=page,
            page_size=self.page_size,
            extra_params=extra_params,
        )
