from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, Sequence

from bioetl.sources.uniprot.request.builder import (
    build_idmapping_payload,
    build_ortholog_query,
    build_search_query,
)

__all__ = ["UniProtRequestBatch", "UniProtRequestBuilder"]


@dataclass(frozen=True)
class UniProtRequestBatch:
    """Represents a batch of UniProt identifiers for a single request."""

    values: tuple[str, ...]
    query: str


class UniProtRequestBuilder:
    """Helper for batching UniProt requests for the target pipeline."""

    def __init__(self, batch_size: int) -> None:
        self._batch_size = max(1, int(batch_size))

    @property
    def batch_size(self) -> int:
        return self._batch_size

    def iter_batches(self, values: Iterable[str | int | float | None]) -> Iterator[UniProtRequestBatch]:
        """Yield stable batches for UniProt search queries."""

        buffer: list[str] = []
        for value in values:
            if value is None:
                continue
            text = str(value).strip()
            if not text:
                continue
            buffer.append(text)
            if len(buffer) >= self._batch_size:
                yield self._flush(buffer)
        if buffer:
            yield self._flush(buffer)

    def build_search(self, values: Sequence[str]) -> str:
        """Construct a UniProt search query for the provided batch."""

        return build_search_query(values)

    def build_id_mapping(self, identifiers: Sequence[str], *, source: str = "UniProtKB_AC-ID", target: str = "UniProtKB") -> dict[str, str]:
        """Construct the payload for the UniProt ID mapping endpoint."""

        return build_idmapping_payload(identifiers, source=source, target=target)

    def build_ortholog_query(self, accession: str) -> str:
        """Construct a UniProt ortholog query for a given accession."""

        return build_ortholog_query(accession)

    def _flush(self, buffer: list[str]) -> UniProtRequestBatch:
        values = tuple(buffer)
        buffer.clear()
        return UniProtRequestBatch(values=values, query=self.build_search(values))
