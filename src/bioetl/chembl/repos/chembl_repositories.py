"""Infrastructure adapters over ChemblClient implementations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

import pandas as pd

from bioetl.chembl.repos.interfaces import (
    ActivityRepository,
    CompoundRecordRepository,
    MoleculeRepository,
)
from bioetl.clients.client_chembl import ChemblClient
from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.core.logging import LogEvents, UnifiedLogger

__all__ = [
    "ChemblActivityRepository",
    "ChemblCompoundRecordRepository",
    "ChemblMoleculeRepository",
]


class ChemblActivityRepository(ActivityRepository):
    """Adapter that loads activity data via :class:`ChemblActivityClient`."""

    def __init__(self, client: ChemblClient, *, batch_size: int | None = None) -> None:
        self._client = client
        self._batch_size = batch_size
        self._log = UnifiedLogger.get(__name__).bind(component="activity_repo")

    def fetch_by_ids(
        self, activity_ids: Sequence[str], *, fields: Sequence[str]
    ) -> pd.DataFrame:
        normalized_ids = [
            str(activity_id).strip()
            for activity_id in activity_ids
            if activity_id is not None and str(activity_id).strip()
        ]
        if not normalized_ids:
            self._log.debug(LogEvents.ACTIVITY_NO_IDS)
            return pd.DataFrame(columns=list(fields))

        client = ChemblActivityClient(
            self._client, batch_size=self._batch_size or ChemblActivityClient.DEFAULT_BATCH_SIZE
        )
        records: list[dict[str, Any]] = []
        try:
            for record in client.iterate_by_ids(normalized_ids, select_fields=list(fields)):
                records.append(dict(record))
        except Exception as exc:  # pragma: no cover - defensive logging
            self._log.warning(
                LogEvents.ACTIVITY_FETCH_ERROR,
                ids_count=len(normalized_ids),
                error=str(exc),
                exc_info=True,
            )

        if not records:
            self._log.debug(LogEvents.ACTIVITY_NO_RECORDS_FETCHED)
            return pd.DataFrame(columns=list(fields))

        return pd.DataFrame.from_records(records)


class ChemblCompoundRecordRepository(CompoundRecordRepository):
    """Adapter that paginates ``compound_record`` lookups through ChemblClient."""

    def __init__(
        self,
        client: ChemblClient,
        *,
        page_limit: int | None = None,
        batch_size: int | None = None,
    ) -> None:
        self._client = client
        self._page_limit = page_limit or 1000
        self._batch_size = batch_size or 100
        self._log = UnifiedLogger.get(__name__).bind(component="compound_record_repo")

    def fetch_by_record_ids(
        self, record_ids: Sequence[str]
    ) -> Mapping[str, Mapping[str, object]]:
        if not record_ids:
            self._log.debug(LogEvents.COMPOUND_RECORD_NO_IDS_AFTER_CLEANUP)
            return {}

        unique_ids = []
        seen: set[str] = set()
        for record_id in record_ids:
            if record_id and record_id not in seen:
                seen.add(record_id)
                unique_ids.append(record_id)

        if not unique_ids:
            self._log.debug(LogEvents.COMPOUND_RECORD_NO_IDS_AFTER_CLEANUP)
            return {}

        fields = ["record_id", "compound_key", "compound_name"]
        all_records: list[dict[str, object]] = []
        for i in range(0, len(unique_ids), self._batch_size):
            chunk = unique_ids[i : i + self._batch_size]
            params: dict[str, Any] = {
                "record_id__in": ",".join(chunk),
                "limit": max(1, min(int(self._page_limit), 1000)),
                "only": ",".join(fields),
                "order_by": "record_id",
            }
            try:
                for record in self._client.paginate(
                    "/compound_record.json",
                    params=params,
                    page_size=params["limit"],
                    items_key="compound_records",
                ):
                    record_dict: dict[str, object] = {
                        str(k): cast(object, v)
                        for k, v in dict(record).items()
                    }
                    all_records.append(record_dict)
            except Exception as exc:  # pragma: no cover - defensive logging
                self._log.warning(
                    LogEvents.COMPOUND_RECORD_FETCH_ERROR,
                    chunk_size=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        result: dict[str, Mapping[str, object]] = {}
        for record in all_records:
            rid = record.get("record_id")
            rid_key = (
                rid
                if isinstance(rid, str)
                else (str(rid).strip() if rid is not None else "")
            )
            if not rid_key or rid_key in result:
                continue
            payload: dict[str, object] = {
                "record_id": rid_key,
                "compound_key": cast(object, record.get("compound_key")),
                "compound_name": cast(object, record.get("compound_name")),
            }
            result[rid_key] = payload

        self._log.info(
            LogEvents.COMPOUND_RECORD_FETCH_COMPLETE,
            ids_requested=len(unique_ids),
            records_fetched=len(all_records),
            records_deduped=len(result),
            page_limit=self._page_limit,
            order_by="record_id",
            has_total=False,
            total_count=None,
            collected=len(all_records),
        )
        return result


class ChemblMoleculeRepository(MoleculeRepository):
    """Adapter that uses :class:`ChemblClient.fetch_molecules_by_ids`."""

    def __init__(self, client: ChemblClient, *, page_limit: int | None = None) -> None:
        self._client = client
        self._page_limit = page_limit or 1000
        self._log = UnifiedLogger.get(__name__).bind(component="molecule_repo")

    def fetch_by_ids(
        self,
        molecule_ids: Sequence[str],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        if not molecule_ids:
            return pd.DataFrame(columns=list(fields) if fields else None)

        limit = page_limit or self._page_limit
        try:
            return self._client.fetch_molecules_by_ids(
                ids=molecule_ids,
                fields=list(fields) if fields else None,
                page_limit=limit,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            self._log.warning(
                LogEvents.MOLECULE_FETCH_ERROR,
                ids_requested=len(molecule_ids),
                error=str(exc),
                exc_info=True,
            )
            return pd.DataFrame(columns=list(fields) if fields else None)
