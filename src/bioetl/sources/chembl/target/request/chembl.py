from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

__all__ = ["ChemblTargetRequest", "ChemblRequestBuilder"]


@dataclass(frozen=True)
class ChemblTargetRequest:
    """Encapsulates a ChEMBL request payload."""

    resource: str
    params: dict[str, Any]


class ChemblRequestBuilder:
    """Utility for constructing paginated ChEMBL target requests."""

    def __init__(self, base_url: str, batch_size: int, *, resource: str = "target") -> None:
        self._base_url = base_url.rstrip("/")
        self._batch_size = max(1, int(batch_size))
        self._resource = resource.strip("/")

    @property
    def batch_size(self) -> int:
        return self._batch_size

    @property
    def base_url(self) -> str:
        return self._base_url

    def build(self, *, offset: int = 0, fields: Sequence[str] | None = None) -> ChemblTargetRequest:
        """Construct a request for the configured resource."""

        params: dict[str, Any] = {
            "limit": self._batch_size,
            "offset": max(0, int(offset)),
        }
        if fields:
            unique_fields = sorted({field for field in fields if field})
            if unique_fields:
                params["fields"] = ",".join(unique_fields)
        return ChemblTargetRequest(resource=self.resource_url, params=params)

    @property
    def resource_url(self) -> str:
        return f"{self._base_url}/{self._resource}"

    def iter_offsets(self, total: int | None) -> Iterable[int]:
        """Yield offsets for the configured batch size given an optional total count."""

        if total is None:
            offset = 0
            while True:
                yield offset
                offset += self._batch_size
        else:
            for offset in range(0, max(0, int(total)), self._batch_size):
                yield offset
