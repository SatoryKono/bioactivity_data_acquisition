"""Chembl compound record entity client."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pandas as pd

from bioetl.clients.client_chembl_base import ChemblClientProtocol
from bioetl.core.logging import LogEvents, UnifiedLogger

__all__ = ["ChemblCompoundRecordEntityClient"]


class ChemblCompoundRecordEntityClient:
    """HTTP client for the ``compound_record`` endpoint returning raw DataFrames."""

    _ENDPOINT = "/compound_record.json"
    _ITEMS_KEY = "compound_records"
    _DEFAULT_PAGE_SIZE = 1000
    _DEFAULT_CHUNK_SIZE = 100
    _KEY_COLUMNS: Sequence[str] = ("document_chembl_id", "molecule_chembl_id")

    def __init__(self, chembl_client: ChemblClientProtocol) -> None:
        self._chembl_client = chembl_client
        self._log = UnifiedLogger.get(__name__).bind(component="compound_record")

    def fetch_by_pairs(
        self,
        pairs: Iterable[tuple[str, str]],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
        chunk_size: int | None = None,
    ) -> pd.DataFrame:
        """Fetch ``compound_record`` entries keyed by ``(molecule, document)`` pairs."""
        validated_pairs = self._prepare_pairs(pairs)
        if not validated_pairs:
            self._log.debug(
                LogEvents.COMPOUND_RECORD_NO_PAIRS,
                message="No valid pairs to fetch",
            )
            return self._empty_frame(fields)

        effective_chunk_size = self._resolve_chunk_size(chunk_size)
        page_size = self._resolve_page_size(page_limit)

        records: list[dict[str, Any]] = []
        grouped_pairs = self._group_by_document(validated_pairs)
        for document_id, molecule_ids in grouped_pairs.items():
            for chunk in self._chunk_molecules(molecule_ids, effective_chunk_size):
                params = self._build_params(
                    document_id=document_id,
                    molecules=chunk,
                    fields=fields,
                    page_size=page_size,
                )
                records.extend(
                    self._request_records(
                        params=params,
                        page_size=page_size,
                        document_id=document_id,
                        molecule_count=len(chunk),
                    )
                )

        frame = self._records_to_frame(records, fields)
        self._log.info(
            LogEvents.COMPOUND_RECORD_FETCH_COMPLETE,
            pairs_requested=len(validated_pairs),
            records_fetched=len(frame),
        )
        return frame

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    def _prepare_pairs(
        self,
        pairs: Iterable[tuple[str, str]],
    ) -> list[tuple[str, str]]:
        normalized: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()

        for index, pair in enumerate(pairs):
            if not isinstance(pair, tuple) or len(pair) != 2:
                msg = f"pair at index {index} must be a tuple[str, str], got {pair!r}"
                raise TypeError(msg)
            molecule, document = pair
            if not isinstance(molecule, str) or not isinstance(document, str):
                msg = (
                    "compound record pairs must contain string identifiers, got "
                    f"({type(molecule)!r}, {type(document)!r})"
                )
                raise TypeError(msg)
            if not molecule or not document:
                continue
            key = (molecule, document)
            if key not in seen:
                seen.add(key)
                normalized.append(key)
        return normalized

    def _group_by_document(
        self,
        pairs: Sequence[tuple[str, str]],
    ) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = defaultdict(list)
        for molecule, document in pairs:
            grouped[document].append(molecule)
        return dict(grouped)

    def _chunk_molecules(
        self,
        molecules: Sequence[str],
        chunk_size: int,
    ) -> Iterable[tuple[str, ...]]:
        for start in range(0, len(molecules), chunk_size):
            yield tuple(molecules[start : start + chunk_size])

    def _build_params(
        self,
        *,
        document_id: str,
        molecules: Sequence[str],
        fields: Sequence[str] | None,
        page_size: int,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "document_chembl_id": document_id,
            "molecule_chembl_id__in": ",".join(molecules),
            "limit": page_size,
        }
        if fields:
            params["only"] = ",".join(fields)
        return params

    def _request_records(
        self,
        *,
        params: Mapping[str, Any],
        page_size: int,
        document_id: str,
        molecule_count: int,
    ) -> list[dict[str, Any]]:
        try:
            return [
                dict(record)
                for record in self._chembl_client.paginate(
                    self._ENDPOINT,
                    params=params,
                    page_size=page_size,
                    items_key=self._ITEMS_KEY,
                )
            ]
        except Exception as exc:  # pragma: no cover - network errors exercised in integration tests
            self._log.warning(
                LogEvents.COMPOUND_RECORD_FETCH_ERROR,
                document_chembl_id=document_id,
                molecule_count=molecule_count,
                error=str(exc),
                exc_info=True,
            )
            return []

    def _records_to_frame(
        self,
        records: Sequence[Mapping[str, Any]],
        fields: Sequence[str] | None,
    ) -> pd.DataFrame:
        if not records:
            return self._empty_frame(fields)

        frame = pd.DataFrame.from_records(records)
        frame = frame.reindex(columns=self._resolve_column_order(frame.columns.tolist(), fields))
        sort_columns = [column for column in self._KEY_COLUMNS if column in frame.columns]
        if sort_columns:
            frame = frame.sort_values(by=sort_columns, kind="mergesort")
        return frame.reset_index(drop=True)

    def _resolve_column_order(
        self,
        columns: Sequence[str],
        fields: Sequence[str] | None,
    ) -> list[str]:
        ordered: list[str] = []
        if fields:
            ordered.extend(fields)
        for key_column in self._KEY_COLUMNS:
            if key_column not in ordered:
                ordered.append(key_column)
        for column in columns:
            if column not in ordered:
                ordered.append(column)
        return ordered

    def _empty_frame(self, fields: Sequence[str] | None) -> pd.DataFrame:
        columns = list(fields) if fields is not None else list(self._KEY_COLUMNS)
        return pd.DataFrame(columns=columns)

    def _resolve_page_size(self, page_limit: int | None) -> int:
        if page_limit is None:
            return self._DEFAULT_PAGE_SIZE
        if page_limit <= 0:
            msg = f"page_limit must be positive, got {page_limit}"
            raise ValueError(msg)
        return page_limit

    def _resolve_chunk_size(self, chunk_size: int | None) -> int:
        if chunk_size is None:
            return self._DEFAULT_CHUNK_SIZE
        if chunk_size <= 0:
            msg = f"chunk_size must be positive, got {chunk_size}"
            raise ValueError(msg)
        return chunk_size

