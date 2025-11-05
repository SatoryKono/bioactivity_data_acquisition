"""ChEMBL-specific API helpers built on top of :mod:`bioetl.core.api_client`."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

import pandas as pd

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

__all__ = ["ChemblClient"]


class ChemblClient:
    """High level client for interacting with the ChEMBL REST API."""

    def __init__(self, client: UnifiedAPIClient) -> None:
        self._client = client
        self._log = UnifiedLogger.get(__name__).bind(component="chembl_client")
        self._status_cache: dict[str, Mapping[str, Any]] = {}

    # ------------------------------------------------------------------
    # Discovery / handshake
    # ------------------------------------------------------------------

    def handshake(self, endpoint: str = "/status") -> Mapping[str, Any]:
        """Perform the handshake for ``endpoint`` once and cache the payload."""

        if endpoint not in self._status_cache:
            payload = self._client.get(endpoint).json()
            self._status_cache[endpoint] = payload
            self._log.info(
                "chembl.handshake",
                endpoint=endpoint,
                chembl_release=payload.get("chembl_db_version"),
                api_version=payload.get("api_version"),
            )
        return self._status_cache[endpoint]

    # ------------------------------------------------------------------
    # Pagination helpers
    # ------------------------------------------------------------------

    def paginate(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 200,
        items_key: str | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Yield records for ``endpoint`` honouring ChEMBL pagination."""

        self.handshake()
        next_url: str | None = endpoint
        query = dict(params or {})
        if page_size:
            query.setdefault("limit", page_size)
        while next_url:
            response = self._client.get(next_url, params=query if next_url == endpoint else None)
            payload: Mapping[str, Any] = response.json()
            yield from self._extract_items(payload, items_key)
            next_url = self._next_link(payload)
            query = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_items(
        self,
        payload: Mapping[str, Any],
        items_key: str | None,
    ) -> Iterable[Mapping[str, Any]]:
        if items_key:
            items = payload.get(items_key)
            if isinstance(items, Sequence):
                return [item for item in items if isinstance(item, Mapping)]
            return []
        for key, value in payload.items():
            if key == "page_meta":
                continue
            if isinstance(value, Sequence):
                mappings = [item for item in value if isinstance(item, Mapping)]
                if mappings:
                    return mappings
        return []

    @staticmethod
    def _next_link(payload: Mapping[str, Any]) -> str | None:
        page_meta = payload.get("page_meta")
        if isinstance(page_meta, Mapping):
            next_link = page_meta.get("next")
            if isinstance(next_link, str) and next_link:
                return next_link
        return None

    # ------------------------------------------------------------------
    # Compound record fetching
    # ------------------------------------------------------------------

    def fetch_compound_records_by_pairs(
        self,
        pairs: Iterable[tuple[str, str]],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        """Fetch compound_record entries by (molecule_chembl_id, document_chembl_id) pairs.

        Parameters
        ----------
        pairs:
            Iterable of (molecule_chembl_id, document_chembl_id) tuples.
        fields:
            List of field names to fetch from compound_record API.
        page_limit:
            Page size for pagination requests.

        Returns
        -------
        dict[tuple[str, str], dict[str, Any]]:
            Dictionary keyed by (molecule_chembl_id, document_chembl_id) -> record dict.
            Only one record per pair (deduplicated by priority).
        """
        # Collect unique pairs, filtering out None/NA values
        unique_pairs: set[tuple[str, str]] = set()
        for mol_id, doc_id in pairs:
            if mol_id and doc_id and not (isinstance(mol_id, float) and pd.isna(mol_id)) and not (isinstance(doc_id, float) and pd.isna(doc_id)):
                unique_pairs.add((str(mol_id).strip(), str(doc_id).strip()))

        if not unique_pairs:
            self._log.debug("compound_record.no_pairs", message="No valid pairs to fetch")
            return {}

        # Group pairs by document_chembl_id
        doc_to_molecules: dict[str, list[str]] = {}
        for mol_id, doc_id in unique_pairs:
            doc_to_molecules.setdefault(doc_id, []).append(mol_id)

        # Fetch records grouped by document
        all_records: list[dict[str, Any]] = []
        for doc_id, mol_ids in doc_to_molecules.items():
            # ChEMBL API supports molecule_chembl_id__in filter
            # Process in chunks to avoid URL length limits
            chunk_size = 100  # Conservative limit for molecule_chembl_id__in
            for i in range(0, len(mol_ids), chunk_size):
                chunk = mol_ids[i : i + chunk_size]
                params: dict[str, Any] = {
                    "document_chembl_id": doc_id,
                    "molecule_chembl_id__in": ",".join(chunk),
                    "limit": page_limit,
                }
                # Build fields parameter for .only() equivalent
                if fields:
                    params["only"] = ",".join(fields)

                try:
                    for record in self.paginate(
                        "/compound_record.json",
                        params=params,
                        page_size=page_limit,
                        items_key="compound_records",
                    ):
                        all_records.append(dict(record))
                except Exception as exc:
                    self._log.warning(
                        "compound_record.fetch_error",
                        document_chembl_id=doc_id,
                        molecule_count=len(chunk),
                        error=str(exc),
                        exc_info=True,
                    )

        # Deduplicate records by (molecule_chembl_id, document_chembl_id)
        # Priority: curated=True > False; removed=False > True; min record_id
        result: dict[tuple[str, str], dict[str, Any]] = {}
        for record in all_records:
            mol_id = record.get("molecule_chembl_id")
            doc_id = record.get("document_chembl_id")
            if not mol_id or not doc_id:
                continue

            key = (str(mol_id), str(doc_id))
            existing = result.get(key)

            if existing is None:
                result[key] = record
            else:
                # Priority comparison
                existing_curated = self._safe_bool(existing.get("curated"))
                record_curated = self._safe_bool(record.get("curated"))
                existing_removed = self._safe_bool(existing.get("removed"))
                record_removed = self._safe_bool(record.get("removed"))

                # Priority 1: curated=True > False
                if record_curated and not existing_curated:
                    result[key] = record
                    continue
                if existing_curated and not record_curated:
                    continue

                # Priority 2: removed=False > True
                if not record_removed and existing_removed:
                    result[key] = record
                    continue
                if not existing_removed and record_removed:
                    continue

                # Priority 3: min record_id
                existing_id = existing.get("record_id")
                record_id = record.get("record_id")
                if existing_id is not None and record_id is not None:
                    try:
                        if int(record_id) < int(existing_id):
                            result[key] = record
                    except (ValueError, TypeError):
                        pass

        self._log.info(
            "compound_record.fetch_complete",
            pairs_requested=len(unique_pairs),
            records_fetched=len(all_records),
            records_deduped=len(result),
        )
        return result

    @staticmethod
    def _safe_bool(value: Any) -> bool:
        """Convert value to bool safely, handling 0/1, None, and boolean values."""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value) and value != 0
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)
