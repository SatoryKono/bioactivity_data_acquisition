"""Base classes for ChEMBL entity fetching."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from math import isnan
from typing import Any, Protocol

from bioetl.clients.chembl_config import EntityConfig
from bioetl.core.logging import UnifiedLogger

__all__ = [
    "ChemblEntityFetcherBase",
    "ChemblClientProtocol",
    "EntityConfig",
]


class ChemblClientProtocol(Protocol):
    """Minimal contract for a ChEMBL client consumed by entity fetchers."""

    def paginate(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 200,
        items_key: str | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Iterate over paginated responses from the ChEMBL API."""
        ...

class ChemblEntityFetcherBase:
    """Base helper for fetching ChEMBL entities by identifier.

    Provides a unified interface shared across multiple ChEMBL API entities.
    """

    def __init__(self, chembl_client: ChemblClientProtocol, config: EntityConfig) -> None:
        """Initialize the fetcher for a ChEMBL entity.

        Parameters
        ----------
        chembl_client:
            ChemblClient-compatible implementation that executes requests.
        config:
            Entity configuration describing endpoint details.
        """
        self._chembl_client: ChemblClientProtocol = chembl_client
        self._config = config
        self._log = UnifiedLogger.get(__name__).bind(
            component="chembl_entity",
            entity=config.log_prefix,
        )

    def fetch_by_ids(
        self,
        ids: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[str, dict[str, Any]] | dict[str, list[dict[str, Any]]]:
        """Retrieve entity payloads by identifiers.

        Parameters
        ----------
        ids:
            Iterable with identifiers that should be fetched.
        fields:
            Collection of fields requested from the API.
        page_limit:
            Page size passed to the API.

        Returns
        -------
        dict[str, dict[str, Any]] | dict[str, list[dict[str, Any]]]:
            Mapping of IDs to a record or list of records
            (depends on ``supports_list_result``).

        Notes
        -----
        NaN handling is implemented without pandas by using :func:`math.isnan` for floats.
        """
        # Normalize and filter identifiers
        unique_ids: set[str] = set()
        for entity_id in ids:
            if entity_id is None:
                continue
            if isinstance(entity_id, float) and isnan(entity_id):
                continue

            normalized_id = str(entity_id).strip()
            if not normalized_id:
                continue
            unique_ids.add(normalized_id)

        if not unique_ids:
            self._log.debug(
                f"{self._config.log_prefix}.no_ids",
                message="No valid IDs to fetch",
            )
            if self._config.supports_list_result:
                return {}
            return {}

        # Process identifiers in chunks
        all_records: list[dict[str, Any]] = []
        ids_list = list(unique_ids)

        for i in range(0, len(ids_list), self._config.chunk_size):
            chunk = ids_list[i : i + self._config.chunk_size]
            params: dict[str, Any] = {
                self._config.filter_param: ",".join(chunk),
                "limit": page_limit,
            }
            # Include the ``only`` parameter to limit returned fields
            if fields:
                params["only"] = ",".join(sorted(fields))

            try:
                for record in self._chembl_client.paginate(
                    self._config.endpoint,
                    params=params,
                    page_size=page_limit,
                    items_key=self._config.items_key,
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                self._log.warning(
                    f"{self._config.log_prefix}.fetch_error",
                    entity_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        # Build the result mapping
        if self._config.supports_list_result:
            return self._build_list_result(all_records, unique_ids)
        return self._build_dict_result(all_records, unique_ids)

    def _build_dict_result(
        self,
        records: list[dict[str, Any]],
        unique_ids: set[str],
    ) -> dict[str, dict[str, Any]]:
        """Build the final mapping with a single record per identifier.

        Parameters
        ----------
        records:
            Collection of fetched records.
        unique_ids:
            Set of requested identifiers.

        Returns
        -------
        dict[str, dict[str, Any]]:
            Mapping ``ID -> record``.
        """
        result: dict[str, dict[str, Any]] = {}
        for record in records:
            entity_id_raw = record.get(self._config.id_field)
            if not entity_id_raw:
                continue
            if not isinstance(entity_id_raw, str):
                continue
            entity_id = entity_id_raw

            if entity_id not in result:
                result[entity_id] = record
            elif self._config.dedup_priority:
                # Apply the priority function when deduplicating records
                existing = result[entity_id]
                result[entity_id] = self._config.dedup_priority(existing, record)

        self._log.info(
            f"{self._config.log_prefix}.fetch_complete",
            ids_requested=len(unique_ids),
            records_fetched=len(records),
            records_deduped=len(result),
        )
        return result

    def _build_list_result(
        self,
        records: list[dict[str, Any]],
        unique_ids: set[str],
    ) -> dict[str, list[dict[str, Any]]]:
        """Build the final mapping with a list of records per identifier.

        Parameters
        ----------
        records:
            Collection of fetched records.
        unique_ids:
            Set of requested identifiers.

        Returns
        -------
        dict[str, list[dict[str, Any]]]:
            Mapping ``ID -> list of records``.
        """
        result: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            entity_id_raw = record.get(self._config.id_field)
            if not entity_id_raw:
                continue
            if not isinstance(entity_id_raw, str):
                continue
            entity_id = entity_id_raw
            if entity_id not in result:
                result[entity_id] = []
            result[entity_id].append(record)

        self._log.info(
            f"{self._config.log_prefix}.fetch_complete",
            ids_requested=len(unique_ids),
            records_fetched=len(records),
            entities_with_records=len(result),
        )
        return result

