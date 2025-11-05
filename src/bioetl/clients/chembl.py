"""ChEMBL-specific API helpers built on top of :mod:`bioetl.core.api_client`."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, cast

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
        query: dict[str, Any] | None = dict(params) if params is not None else None
        if page_size and query is not None:
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
                result: list[Mapping[str, Any]] = []
                for item_raw in items:  # pyright: ignore[reportUnknownVariableType]
                    item = cast(Any, item_raw)
                    if isinstance(item, Mapping):
                        result.append(cast(Mapping[str, Any], item))
                return result
            return []
        for key, value in payload.items():
            if key == "page_meta":
                continue
            if isinstance(value, Sequence):
                mappings: list[Mapping[str, Any]] = []
                for item_raw in value:  # pyright: ignore[reportUnknownVariableType]
                    item = cast(Any, item_raw)
                    if isinstance(item, Mapping):
                        mappings.append(cast(Mapping[str, Any], item))
                if mappings:
                    return mappings
        return []

    @staticmethod
    def _next_link(payload: Mapping[str, Any]) -> str | None:
        page_meta = payload.get("page_meta")
        if isinstance(page_meta, Mapping):
            page_meta_mapping = cast(Mapping[str, Any], page_meta)
            next_link: str | None = cast(str | None, page_meta_mapping.get("next"))
            if isinstance(next_link, str) and next_link:
                return next_link
        return None

    # ------------------------------------------------------------------
    # Assay fetching
    # ------------------------------------------------------------------

    def fetch_assays_by_ids(
        self,
        ids: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[str, dict[str, Any]]:
        """Fetch assay entries by assay_chembl_id.

        Parameters
        ----------
        ids:
            Iterable of assay_chembl_id values.
        fields:
            List of field names to fetch from assay API.
        page_limit:
            Page size for pagination requests.

        Returns
        -------
        dict[str, dict[str, Any]]:
            Dictionary keyed by assay_chembl_id -> record dict.
        """
        # Collect unique IDs, filtering out None/NA values
        unique_ids: set[str] = set()
        for assay_id in ids:
            if assay_id and not (isinstance(assay_id, float) and pd.isna(assay_id)):
                unique_ids.add(str(assay_id).strip())

        if not unique_ids:
            self._log.debug("assay.no_ids", message="No valid IDs to fetch")
            return {}

        # Process in chunks to avoid URL length limits
        chunk_size = 100  # Conservative limit for assay_chembl_id__in
        all_records: list[dict[str, Any]] = []
        ids_list = list(unique_ids)

        for i in range(0, len(ids_list), chunk_size):
            chunk = ids_list[i : i + chunk_size]
            params: dict[str, Any] = {
                "assay_chembl_id__in": ",".join(chunk),
                "limit": page_limit,
            }
            # Build fields parameter for .only() equivalent
            if fields:
                params["only"] = ",".join(fields)

            try:
                for record in self.paginate(
                    "/assay.json",
                    params=params,
                    page_size=page_limit,
                    items_key="assays",
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                self._log.warning(
                    "assay.fetch_error",
                    assay_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        # Build result dictionary keyed by assay_chembl_id
        result: dict[str, dict[str, Any]] = {}
        for record in all_records:
            assay_id_raw = record.get("assay_chembl_id")
            if not assay_id_raw:
                continue
            if not isinstance(assay_id_raw, str):
                continue
            assay_id = assay_id_raw
            result[assay_id] = record

        self._log.info(
            "assay.fetch_complete",
            ids_requested=len(unique_ids),
            records_fetched=len(all_records),
            records_deduped=len(result),
        )
        return result

    # ------------------------------------------------------------------
    # Molecule fetching
    # ------------------------------------------------------------------

    def fetch_molecules_by_ids(
        self,
        ids: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[str, dict[str, Any]]:
        """Fetch molecule entries by molecule_chembl_id.

        Parameters
        ----------
        ids:
            Iterable of molecule_chembl_id values.
        fields:
            List of field names to fetch from molecule API.
        page_limit:
            Page size for pagination requests.

        Returns
        -------
        dict[str, dict[str, Any]]:
            Dictionary keyed by molecule_chembl_id -> record dict.
        """
        # Collect unique IDs, filtering out None/NA values
        unique_ids: set[str] = set()
        for mol_id in ids:
            if mol_id and not (isinstance(mol_id, float) and pd.isna(mol_id)):
                unique_ids.add(str(mol_id).strip())

        if not unique_ids:
            self._log.debug("molecule.no_ids", message="No valid IDs to fetch")
            return {}

        # Process in chunks to avoid URL length limits
        chunk_size = 100  # Conservative limit for molecule_chembl_id__in
        all_records: list[dict[str, Any]] = []
        ids_list = list(unique_ids)

        for i in range(0, len(ids_list), chunk_size):
            chunk = ids_list[i : i + chunk_size]
            params: dict[str, Any] = {
                "molecule_chembl_id__in": ",".join(chunk),
                "limit": page_limit,
            }
            # Build fields parameter for .only() equivalent
            if fields:
                params["only"] = ",".join(fields)

            try:
                for record in self.paginate(
                    "/molecule.json",
                    params=params,
                    page_size=page_limit,
                    items_key="molecules",
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                self._log.warning(
                    "molecule.fetch_error",
                    molecule_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        # Build result dictionary keyed by molecule_chembl_id
        result: dict[str, dict[str, Any]] = {}
        for record in all_records:
            mol_id_raw = record.get("molecule_chembl_id")
            if not mol_id_raw:
                continue
            if not isinstance(mol_id_raw, str):
                continue
            mol_id = mol_id_raw
            result[mol_id] = record

        self._log.info(
            "molecule.fetch_complete",
            ids_requested=len(unique_ids),
            records_fetched=len(all_records),
            records_deduped=len(result),
        )
        return result

    # ------------------------------------------------------------------
    # Data validity lookup fetching
    # ------------------------------------------------------------------

    def fetch_data_validity_lookup(
        self,
        comments: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[str, dict[str, Any]]:
        """Fetch data_validity_lookup entries by data_validity_comment.

        Parameters
        ----------
        comments:
            Iterable of data_validity_comment values.
        fields:
            List of field names to fetch from data_validity_lookup API.
        page_limit:
            Page size for pagination requests.

        Returns
        -------
        dict[str, dict[str, Any]]:
            Dictionary keyed by data_validity_comment -> record dict.
        """
        # Collect unique comments, filtering out None/NA values
        unique_comments: set[str] = set()
        for comment in comments:
            if comment and not (isinstance(comment, float) and pd.isna(comment)):
                unique_comments.add(str(comment).strip())

        if not unique_comments:
            self._log.debug("data_validity_lookup.no_comments", message="No valid comments to fetch")
            return {}

        # Process in chunks to avoid URL length limits
        chunk_size = 100  # Conservative limit for data_validity_comment__in
        all_records: list[dict[str, Any]] = []
        comments_list = list(unique_comments)

        for i in range(0, len(comments_list), chunk_size):
            chunk = comments_list[i : i + chunk_size]
            params: dict[str, Any] = {
                "data_validity_comment__in": ",".join(chunk),
                "limit": page_limit,
            }
            # Build fields parameter for .only() equivalent
            if fields:
                params["only"] = ",".join(fields)

            try:
                for record in self.paginate(
                    "/data_validity_lookup.json",
                    params=params,
                    page_size=page_limit,
                    items_key="data_validity_lookups",
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                self._log.warning(
                    "data_validity_lookup.fetch_error",
                    comment_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        # Build result dictionary keyed by data_validity_comment
        result: dict[str, dict[str, Any]] = {}
        for record in all_records:
            comment_raw = record.get("data_validity_comment")
            if not comment_raw:
                continue
            if not isinstance(comment_raw, str):
                continue
            comment = comment_raw
            result[comment] = record

        self._log.info(
            "data_validity_lookup.fetch_complete",
            comments_requested=len(unique_comments),
            records_fetched=len(all_records),
            records_deduped=len(result),
        )
        return result

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

    # ------------------------------------------------------------------
    # Document term fetching
    # ------------------------------------------------------------------

    def fetch_document_terms_by_ids(
        self,
        ids: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch document_term entries by document_chembl_id.

        Parameters
        ----------
        ids:
            Iterable of document_chembl_id values.
        fields:
            List of field names to fetch from document_term API.
        page_limit:
            Page size for pagination requests.

        Returns
        -------
        dict[str, list[dict[str, Any]]]:
            Dictionary keyed by document_chembl_id -> list of record dicts.
            Each document can have multiple terms, so values are lists.
        """
        # Collect unique IDs, filtering out None/NA values
        unique_ids: set[str] = set()
        for doc_id in ids:
            if doc_id and not (isinstance(doc_id, float) and pd.isna(doc_id)):
                unique_ids.add(str(doc_id).strip())

        if not unique_ids:
            self._log.debug("document_term.no_ids", message="No valid IDs to fetch")
            return {}

        # Process in chunks to avoid URL length limits
        chunk_size = 100  # Conservative limit for document_chembl_id__in
        all_records: list[dict[str, Any]] = []
        ids_list = list(unique_ids)

        for i in range(0, len(ids_list), chunk_size):
            chunk = ids_list[i : i + chunk_size]
            params: dict[str, Any] = {
                "document_chembl_id__in": ",".join(chunk),
                "limit": page_limit,
            }
            # Build fields parameter for .only() equivalent
            if fields:
                params["only"] = ",".join(fields)

            try:
                for record in self.paginate(
                    "/document_term.json",
                    params=params,
                    page_size=page_limit,
                    items_key="document_terms",
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                self._log.warning(
                    "document_term.fetch_error",
                    document_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        # Build result dictionary keyed by document_chembl_id -> list of records
        result: dict[str, list[dict[str, Any]]] = {}
        for record in all_records:
            doc_id_raw = record.get("document_chembl_id")
            if not doc_id_raw:
                continue
            if not isinstance(doc_id_raw, str):
                continue
            doc_id = doc_id_raw
            if doc_id not in result:
                result[doc_id] = []
            result[doc_id].append(record)

        self._log.info(
            "document_term.fetch_complete",
            ids_requested=len(unique_ids),
            records_fetched=len(all_records),
            documents_with_terms=len(result),
        )
        return result

    # ------------------------------------------------------------------
    # Assay class map fetching
    # ------------------------------------------------------------------

    def fetch_assay_class_map_by_assay_ids(
        self,
        assay_ids: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch assay_class_map entries by assay_chembl_id.

        Parameters
        ----------
        assay_ids:
            Iterable of assay_chembl_id values.
        fields:
            List of field names to fetch from assay_class_map API.
        page_limit:
            Page size for pagination requests.

        Returns
        -------
        dict[str, list[dict[str, Any]]]:
            Dictionary keyed by assay_chembl_id -> list of record dicts.
            Each assay can have multiple class mappings, so values are lists.
        """
        # Collect unique IDs, filtering out None/NA values
        unique_ids: set[str] = set()
        for assay_id in assay_ids:
            if assay_id and not (isinstance(assay_id, float) and pd.isna(assay_id)):
                unique_ids.add(str(assay_id).strip())

        if not unique_ids:
            self._log.debug("assay_class_map.no_ids", message="No valid IDs to fetch")
            return {}

        # Process in chunks to avoid URL length limits
        chunk_size = 100  # Conservative limit for assay_chembl_id__in
        all_records: list[dict[str, Any]] = []
        ids_list = list(unique_ids)

        for i in range(0, len(ids_list), chunk_size):
            chunk = ids_list[i : i + chunk_size]
            params: dict[str, Any] = {
                "assay_chembl_id__in": ",".join(chunk),
                "limit": page_limit,
            }
            # Build fields parameter for .only() equivalent
            if fields:
                params["only"] = ",".join(fields)

            try:
                for record in self.paginate(
                    "/assay_class_map.json",
                    params=params,
                    page_size=page_limit,
                    items_key="assay_class_maps",
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                self._log.warning(
                    "assay_class_map.fetch_error",
                    assay_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        # Build result dictionary keyed by assay_chembl_id -> list of records
        result: dict[str, list[dict[str, Any]]] = {}
        for record in all_records:
            assay_id_raw = record.get("assay_chembl_id")
            if not assay_id_raw:
                continue
            if not isinstance(assay_id_raw, str):
                continue
            assay_id = assay_id_raw
            if assay_id not in result:
                result[assay_id] = []
            result[assay_id].append(record)

        self._log.info(
            "assay_class_map.fetch_complete",
            ids_requested=len(unique_ids),
            records_fetched=len(all_records),
            assays_with_mappings=len(result),
        )
        return result

    # ------------------------------------------------------------------
    # Assay parameters fetching
    # ------------------------------------------------------------------

    def fetch_assay_parameters_by_assay_ids(
        self,
        assay_ids: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
        active_only: bool = True,
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch assay_parameters entries by assay_chembl_id.

        Parameters
        ----------
        assay_ids:
            Iterable of assay_chembl_id values.
        fields:
            List of field names to fetch from assay_parameters API.
        page_limit:
            Page size for pagination requests.
        active_only:
            If True, filter only active parameters (active=1).

        Returns
        -------
        dict[str, list[dict[str, Any]]]:
            Dictionary keyed by assay_chembl_id -> list of record dicts.
            Each assay can have multiple parameters, so values are lists.
        """
        # Collect unique IDs, filtering out None/NA values
        unique_ids: set[str] = set()
        for assay_id in assay_ids:
            if assay_id and not (isinstance(assay_id, float) and pd.isna(assay_id)):
                unique_ids.add(str(assay_id).strip())

        if not unique_ids:
            self._log.debug("assay_parameters.no_ids", message="No valid IDs to fetch")
            return {}

        # Process in chunks to avoid URL length limits
        chunk_size = 100  # Conservative limit for assay_chembl_id__in
        all_records: list[dict[str, Any]] = []
        ids_list = list(unique_ids)

        for i in range(0, len(ids_list), chunk_size):
            chunk = ids_list[i : i + chunk_size]
            params: dict[str, Any] = {
                "assay_chembl_id__in": ",".join(chunk),
                "limit": page_limit,
            }
            # Filter active parameters if requested
            if active_only:
                params["active"] = "1"
            # Build fields parameter for .only() equivalent
            if fields:
                params["only"] = ",".join(fields)

            try:
                for record in self.paginate(
                    "/assay_parameters.json",
                    params=params,
                    page_size=page_limit,
                    items_key="assay_parameters",
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                self._log.warning(
                    "assay_parameters.fetch_error",
                    assay_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        # Build result dictionary keyed by assay_chembl_id -> list of records
        result: dict[str, list[dict[str, Any]]] = {}
        for record in all_records:
            assay_id_raw = record.get("assay_chembl_id")
            if not assay_id_raw:
                continue
            if not isinstance(assay_id_raw, str):
                continue
            assay_id = assay_id_raw
            if assay_id not in result:
                result[assay_id] = []
            result[assay_id].append(record)

        self._log.info(
            "assay_parameters.fetch_complete",
            ids_requested=len(unique_ids),
            records_fetched=len(all_records),
            assays_with_parameters=len(result),
        )
        return result

    # ------------------------------------------------------------------
    # Assay classification fetching
    # ------------------------------------------------------------------

    def fetch_assay_classifications_by_class_ids(
        self,
        class_ids: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[str, dict[str, Any]]:
        """Fetch assay_classification entries by assay_class_id.

        Parameters
        ----------
        class_ids:
            Iterable of assay_class_id values.
        fields:
            List of field names to fetch from assay_classification API.
        page_limit:
            Page size for pagination requests.

        Returns
        -------
        dict[str, dict[str, Any]]:
            Dictionary keyed by assay_class_id -> record dict.
        """
        # Collect unique IDs, filtering out None/NA values
        unique_ids: set[str] = set()
        for class_id in class_ids:
            if class_id and not (isinstance(class_id, float) and pd.isna(class_id)):
                unique_ids.add(str(class_id).strip())

        if not unique_ids:
            self._log.debug("assay_classification.no_ids", message="No valid IDs to fetch")
            return {}

        # Process in chunks to avoid URL length limits
        chunk_size = 100  # Conservative limit for assay_class_id__in
        all_records: list[dict[str, Any]] = []
        ids_list = list(unique_ids)

        for i in range(0, len(ids_list), chunk_size):
            chunk = ids_list[i : i + chunk_size]
            params: dict[str, Any] = {
                "assay_class_id__in": ",".join(chunk),
                "limit": page_limit,
            }
            # Build fields parameter for .only() equivalent
            if fields:
                params["only"] = ",".join(fields)

            try:
                for record in self.paginate(
                    "/assay_classification.json",
                    params=params,
                    page_size=page_limit,
                    items_key="assay_classifications",
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                self._log.warning(
                    "assay_classification.fetch_error",
                    class_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        # Build result dictionary keyed by assay_class_id
        result: dict[str, dict[str, Any]] = {}
        for record in all_records:
            class_id_raw = record.get("assay_class_id")
            if not class_id_raw:
                continue
            if not isinstance(class_id_raw, str):
                continue
            class_id = class_id_raw
            result[class_id] = record

        self._log.info(
            "assay_classification.fetch_complete",
            ids_requested=len(unique_ids),
            records_fetched=len(all_records),
            records_deduped=len(result),
        )
        return result