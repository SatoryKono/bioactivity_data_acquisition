"""Chembl compound record entity client."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from bioetl.clients.client_chembl_base import ChemblClientProtocol
from bioetl.core.logging import LogEvents, UnifiedLogger

__all__ = ["ChemblCompoundRecordEntityClient"]


class ChemblCompoundRecordEntityClient:
    """Client for retrieving ``compound_record`` entries from the ChEMBL API.

    The API requires pairs of ``(molecule_chembl_id, document_chembl_id)`` instead of
    single identifiers, therefore this client does not inherit from
    ``ChemblEntityFetcherBase``.
    """

    def __init__(self, chembl_client: ChemblClientProtocol) -> None:
        """Initialise the compound_record client.

        Parameters
        ----------
        chembl_client:
            ChemblClient instance used for API requests.
        """
        self._chembl_client = chembl_client
        self._log = UnifiedLogger.get(__name__).bind(component="compound_record")

    def fetch_by_pairs(
        self,
        pairs: Iterable[tuple[str, str]],
        fields: Sequence[str],
        page_limit: int = 1000,
        *,
        chunk_size: int = 100,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        """Fetch ``compound_record`` entries keyed by molecule/document pairs.

        Parameters
        ----------
        pairs:
            Iterable of ``(molecule_chembl_id, document_chembl_id)`` pairs.
        fields:
            List of fields to request from the API.
        page_limit:
            Page size used for pagination.
        chunk_size:
            Maximum number of ``molecule_chembl_id`` values per request.

        Returns
        -------
        dict[tuple[str, str], dict[str, Any]]:
            Mapping from ``(molecule_chembl_id, document_chembl_id)`` to records.
        """
        import pandas as pd

        if chunk_size < 1:
            msg = f"chunk_size must be positive, got {chunk_size}"
            raise ValueError(msg)

        # Collect unique pairs while filtering ``None`` and NA values.
        unique_pairs: set[tuple[str, str]] = set()
        for mol_id, doc_id in pairs:
            if (
                mol_id
                and doc_id
                and not (isinstance(mol_id, float) and pd.isna(mol_id))
                and not (isinstance(doc_id, float) and pd.isna(doc_id))
            ):
                unique_pairs.add((str(mol_id).strip(), str(doc_id).strip()))

        if not unique_pairs:
            self._log.debug(LogEvents.COMPOUND_RECORD_NO_PAIRS, message="No valid pairs to fetch")
            return {}

        # Group collected pairs by document identifier.
        doc_to_molecules: dict[str, list[str]] = {}
        for mol_id, doc_id in unique_pairs:
            doc_to_molecules.setdefault(doc_id, []).append(mol_id)

        # Fetch records for each document chunk.
        all_records: list[dict[str, Any]] = []
        for doc_id, mol_ids in doc_to_molecules.items():
            # ChEMBL API supports ``molecule_chembl_id__in`` filtering.
            # Chunk requests to avoid URL length limits.
            for i in range(0, len(mol_ids), chunk_size):
                chunk = mol_ids[i : i + chunk_size]
                params: dict[str, Any] = {
                    "document_chembl_id": doc_id,
                    "molecule_chembl_id__in": ",".join(chunk),
                    "limit": page_limit,
                }
                # Use the ``only`` parameter to reduce payload size.
                if fields:
                    params["only"] = ",".join(sorted(fields))

                try:
                    for record in self._chembl_client.paginate(
                        "/compound_record.json",
                        params=params,
                        page_size=page_limit,
                        items_key="compound_records",
                    ):
                        all_records.append(dict(record))
                except Exception as exc:
                    self._log.warning(
                        LogEvents.COMPOUND_RECORD_FETCH_ERROR,
                        document_chembl_id=doc_id,
                        molecule_count=len(chunk),
                        error=str(exc),
                        exc_info=True,
                    )

        # Deduplicate records per (molecule_chembl_id, document_chembl_id).
        # Priority: curated=True > False; removed=False > True; minimal record_id.
        result: dict[tuple[str, str], dict[str, Any]] = {}
        for record in all_records:
            mol_id_raw = record.get("molecule_chembl_id")
            doc_id_raw = record.get("document_chembl_id")
            if not mol_id_raw or not doc_id_raw:
                continue
            if not isinstance(mol_id_raw, str) or not isinstance(doc_id_raw, str):
                continue
            mol_id = mol_id_raw
            doc_id = doc_id_raw

            key = (mol_id, doc_id)
            existing = result.get(key)

            if existing is None:
                result[key] = record
            else:
                result[key] = _compound_record_dedup_priority(existing, record)

        self._log.info(
            LogEvents.COMPOUND_RECORD_FETCH_COMPLETE,
            pairs_requested=len(unique_pairs),
            records_fetched=len(all_records),
            records_deduped=len(result),
        )
        return result


def _compound_record_dedup_priority(
    existing: dict[str, Any],
    new: dict[str, Any],
) -> dict[str, Any]:
    """Deduplication priority function for ``compound_record`` entries.

    Priority order:
    1. ``curated=True`` over ``False``
    2. ``removed=False`` over ``True``
    3. Lower ``record_id`` wins

    Parameters
    ----------
    existing:
        Current record in the aggregation.
    new:
        Candidate record to compare.

    Returns
    -------
    dict[str, Any]:
        Record selected according to priority rules.
    """
    existing_curated = _safe_bool(existing.get("curated"))
    new_curated = _safe_bool(new.get("curated"))
    existing_removed = _safe_bool(existing.get("removed"))
    new_removed = _safe_bool(new.get("removed"))

    # Priority 1: curated=True > False
    if new_curated and not existing_curated:
        return new
    if existing_curated and not new_curated:
        return existing

    # Priority 2: removed=False > True
    if not new_removed and existing_removed:
        return new
    if not existing_removed and new_removed:
        return existing

    # Priority 3: min record_id
    existing_id = existing.get("record_id")
    new_id = new.get("record_id")
    if existing_id is not None and new_id is not None:
        try:
            if int(new_id) < int(existing_id):
                return new
        except (ValueError, TypeError):
            pass

    return existing


def _safe_bool(value: Any) -> bool:
    """Convert an arbitrary value to ``bool`` safely.

    Handles ``0/1``, ``None`` and boolean values explicitly.

    Parameters
    ----------
    value:
        Value to coerce.

    Returns
    -------
    bool:
        Coerced boolean value.
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value) and value != 0
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)

