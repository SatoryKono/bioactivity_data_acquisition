"""Assay-specific HTTP client helpers built on top of :mod:`bioetl.clients.chembl`."""

from __future__ import annotations

from collections import deque
from typing import Iterable, Iterator, Mapping, Sequence
from urllib.parse import urlencode

from bioetl.clients.chembl import ChemblClient


class ChemblAssayClient:
    """High level helper focused on retrieving assay payloads."""

    def __init__(
        self,
        client: ChemblClient,
        *,
        batch_size: int,
        max_url_length: int,
    ) -> None:
        if batch_size <= 0:
            msg = "batch_size must be a positive integer"
            raise ValueError(msg)
        if max_url_length <= 0:
            msg = "max_url_length must be a positive integer"
            raise ValueError(msg)
        self._client = client
        self._batch_size = min(batch_size, 25)
        self._max_url_length = max_url_length
        self._chembl_release: str | None = None

    @property
    def chembl_release(self) -> str | None:
        """Return the ChEMBL release captured during handshake."""

        return self._chembl_release

    def handshake(
        self,
        *,
        endpoint: str = "/status",
        enabled: bool = True,
    ) -> Mapping[str, object]:
        """Perform the configured handshake and cache the release identifier."""

        if not enabled:
            return {}
        payload = self._client.handshake(endpoint)
        release = payload.get("chembl_db_version")
        if isinstance(release, str):
            self._chembl_release = release
        return payload

    def iterate_all(
        self,
        *,
        limit: int | None = None,
        page_size: int | None = None,
    ) -> Iterator[Mapping[str, object]]:
        """Iterate over assay records respecting optional limits."""

        effective_page_size = self._coerce_page_size(page_size)
        yielded = 0
        if limit is not None and limit > 0:
            params = {"limit": min(effective_page_size, limit)}
        else:
            params = {"limit": effective_page_size}
        for item in self._client.paginate("/assay.json", params=params, page_size=effective_page_size):
            yield item
            yielded += 1
            if limit is not None and yielded >= limit:
                break

    def iterate_by_ids(self, assay_ids: Sequence[str]) -> Iterator[Mapping[str, object]]:
        """Fetch assays by explicit identifiers using chunked requests."""

        for chunk in self._chunk_identifiers(assay_ids):
            params = {"assay_chembl_id__in": ",".join(chunk)}
            for item in self._client.paginate("/assay.json", params=params, page_size=len(chunk)):
                yield item

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _coerce_page_size(self, requested: int | None) -> int:
        if requested is None:
            return self._batch_size
        if requested <= 0:
            return self._batch_size
        return min(requested, self._batch_size)

    def _chunk_identifiers(self, assay_ids: Sequence[str]) -> Iterable[Sequence[str]]:
        chunk: deque[str] = deque()
        for identifier in assay_ids:
            if not isinstance(identifier, str) or not identifier:
                continue
            candidate_size = len(chunk) + 1
            candidate_param = self._encode_in_query(tuple(list(chunk) + [identifier]))
            if candidate_size > self._batch_size or candidate_param > self._max_url_length:
                if chunk:
                    yield tuple(chunk)
                    chunk.clear()
                chunk.append(identifier)
                continue
            chunk.append(identifier)
        if chunk:
            yield tuple(chunk)

    def _encode_in_query(self, identifiers: Sequence[str]) -> int:
        params = urlencode({"assay_chembl_id__in": ",".join(identifiers)})
        # Account for base endpoint length to approximate the final URL length.
        return len("/assay.json?") + len(params)
