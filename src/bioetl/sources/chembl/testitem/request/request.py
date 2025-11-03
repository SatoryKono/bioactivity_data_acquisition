"""Request builder for TestItem pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from urllib.parse import urlencode

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


class TestItemRequestBuilder:
    """Builder for constructing ChEMBL molecule API requests."""

    def __init__(
        self,
        api_client: UnifiedAPIClient,
        batch_size: int,
        max_url_length: int | None,
    ):
        """Initialize request builder.

        Args:
            api_client: Unified API client instance
            batch_size: Batch size for requests
            max_url_length: Maximum URL length constraint
        """
        self.api_client = api_client
        self.batch_size = batch_size
        self.max_url_length = max_url_length

    def build_filter_params(self, molecule_ids: Sequence[str]) -> dict[str, str]:
        """Return query parameters for a molecule batch request."""

        limit = min(len(molecule_ids), self.batch_size)
        return {
            "molecule_chembl_id__in": ",".join(molecule_ids),
            "limit": str(limit),
        }

    def build_url(self, molecule_ids: Sequence[str]) -> str:
        """Build request URL for molecule IDs.

        Args:
            molecule_ids: List of ChEMBL molecule IDs

        Returns:
            Fully qualified request URL
        """
        base = str(self.api_client.config.base_url).rstrip("/")
        url = f"{base}/molecule.json"
        params = self.build_filter_params(molecule_ids)
        query_string = urlencode(params)
        return f"{url}?{query_string}"

    def split_by_url_length(self, candidate_ids: Sequence[str]) -> list[list[str]]:
        """Recursively split molecule IDs to satisfy URL length limit.

        Args:
            candidate_ids: Molecule IDs to split

        Returns:
            List of ID batches
        """
        ids = [molid for molid in candidate_ids if molid]
        if not ids:
            return []

        limit = self.max_url_length
        if limit is None:
            return [ids]

        limit_value = int(limit)
        url = self.build_url(ids)
        if not url:
            return [ids]

        if len(url) <= limit_value or len(ids) == 1:
            if len(url) > limit_value:
                logger.warning(
                    "testitem_single_id_exceeds_url_limit",
                    molecule_id=ids[0],
                    url_length=len(url),
                    max_length=limit_value,
                )
            return [ids]

        midpoint = max(1, len(ids) // 2)
        return self.split_by_url_length(ids[:midpoint]) + self.split_by_url_length(ids[midpoint:])

    def iter_batches(self, molecule_ids: list[str]) -> list[list[str]]:
        """Generate batches of molecule IDs respecting batch size and URL length.

        Args:
            molecule_ids: All molecule IDs to batch

        Returns:
            List of ID batches
        """
        batches: list[list[str]] = []
        for index in range(0, len(molecule_ids), self.batch_size):
            chunk = molecule_ids[index : index + self.batch_size]
            if not chunk:
                continue
            batches.extend(self.split_by_url_length(chunk))
        return batches
