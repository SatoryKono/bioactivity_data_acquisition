"""Request builder for the ChEMBL assay endpoint."""

from __future__ import annotations

from collections.abc import Sequence
from urllib.parse import urlencode

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


class AssayRequestBuilder:
    """Helper for composing batch-aware assay requests."""

    def __init__(
        self,
        api_client: UnifiedAPIClient,
        batch_size: int,
        max_url_length: int | None,
    ) -> None:
        self.api_client = api_client
        self.batch_size = batch_size
        self.max_url_length = max_url_length

    def build_assay_request_url(self, assay_ids: Sequence[str]) -> str:
        """Return a fully qualified URL for the provided identifiers."""

        base = str(self.api_client.config.base_url).rstrip("/")
        query = urlencode({"assay_chembl_id__in": ",".join(assay_ids)})
        return f"{base}/assay.json?{query}"

    def _split_by_url_length(self, assay_ids: Sequence[str]) -> list[list[str]]:
        ids = [assay_id for assay_id in assay_ids if assay_id]
        if not ids:
            return []

        if self.max_url_length is None:
            return [ids]

        limit = int(self.max_url_length)
        url = self.build_assay_request_url(ids)
        if len(url) <= limit or len(ids) == 1:
            if len(url) > limit:
                logger.warning(
                    "assay_single_id_exceeds_url_limit",
                    assay_id=ids[0],
                    url_length=len(url),
                    max_length=limit,
                )
            return [ids]

        midpoint = max(1, len(ids) // 2)
        return self._split_by_url_length(ids[:midpoint]) + self._split_by_url_length(ids[midpoint:])

    def iter_assay_batches(self, assay_ids: Sequence[str]) -> list[list[str]]:
        """Yield batches respecting configured batch size and URL limits."""

        batches: list[list[str]] = []
        for index in range(0, len(assay_ids), self.batch_size):
            chunk = [assay_id for assay_id in assay_ids[index : index + self.batch_size] if assay_id]
            if not chunk:
                continue
            batches.extend(self._split_by_url_length(chunk))
        return batches
