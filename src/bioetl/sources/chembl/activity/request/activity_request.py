"""Helpers for constructing ChEMBL activity requests."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from urllib.parse import urlencode

from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


@dataclass(slots=True)
class ActivityRequestBuilder:
    """Construct batched activity requests respecting URL constraints."""

    base_url: str
    batch_size: int
    max_url_length: int | None = None

    def __post_init__(self) -> None:  # pragma: no cover - dataclass hook
        self.base_url = self.base_url.rstrip("/")
        self.batch_size = max(1, int(self.batch_size))

    def iter_batches(self, activity_ids: Sequence[int]) -> Iterator[list[int]]:
        """Yield activity identifier batches respecting all constraints."""

        total = len(activity_ids)
        for index in range(0, total, self.batch_size):
            chunk = list(activity_ids[index : index + self.batch_size])
            if not chunk:
                continue
            for split_chunk in self._split_by_url_length(chunk):
                if split_chunk:
                    yield split_chunk

    def build_url(self, activity_ids: Sequence[int]) -> str:
        """Return the fully qualified request URL for ``activity_ids``."""

        query = urlencode({"activity_id__in": ",".join(map(str, activity_ids))})
        return f"{self.base_url}/activity.json?{query}"

    def _split_by_url_length(self, candidate_ids: Sequence[int]) -> list[list[int]]:
        """Recursively split identifiers so that URLs stay within the limit."""

        identifiers = list(candidate_ids)
        if not identifiers:
            return []

        if self.max_url_length is None:
            return [identifiers]

        url = self.build_url(identifiers)
        if not url:
            return [identifiers]

        if len(url) <= self.max_url_length or len(identifiers) == 1:
            if len(url) > self.max_url_length and identifiers:
                logger.warning(
                    "activity_single_id_exceeds_url_limit",
                    activity_id=identifiers[0],
                    url_length=len(url),
                    max_length=self.max_url_length,
                )
            return [identifiers]

        midpoint = max(1, len(identifiers) // 2)
        head = self._split_by_url_length(identifiers[:midpoint])
        tail = self._split_by_url_length(identifiers[midpoint:])
        return head + tail


__all__ = ["ActivityRequestBuilder"]
