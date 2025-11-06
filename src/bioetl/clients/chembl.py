"""ChEMBL-specific API helpers built on top of :mod:`bioetl.core.api_client`."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, cast

from bioetl.clients.assay.chembl_assay_entity import ChemblAssayEntityClient
from bioetl.clients.chembl_entities import (
    ChemblAssayClassificationEntityClient,
    ChemblAssayClassMapEntityClient,
    ChemblAssayParametersEntityClient,
    ChemblCompoundRecordEntityClient,
    ChemblDataValidityEntityClient,
    ChemblMoleculeEntityClient,
)
from bioetl.clients.document.chembl_document_entity import ChemblDocumentTermEntityClient
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

__all__ = ["ChemblClient"]


class ChemblClient:
    """High level client for interacting with the ChEMBL REST API."""

    def __init__(self, client: UnifiedAPIClient) -> None:
        self._client = client
        self._log = UnifiedLogger.get(__name__).bind(component="chembl_client")
        self._status_cache: dict[str, Mapping[str, Any]] = {}
        # Инициализация специализированных клиентов для сущностей
        self._assay_entity = ChemblAssayEntityClient(self)
        self._molecule_entity = ChemblMoleculeEntityClient(self)
        self._data_validity_entity = ChemblDataValidityEntityClient(self)
        self._document_term_entity = ChemblDocumentTermEntityClient(self)
        self._assay_class_map_entity = ChemblAssayClassMapEntityClient(self)
        self._assay_parameters_entity = ChemblAssayParametersEntityClient(self)
        self._assay_classification_entity = ChemblAssayClassificationEntityClient(self)
        self._compound_record_entity = ChemblCompoundRecordEntityClient(self)

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
        result = self._assay_entity.fetch_by_ids(ids, fields, page_limit)
        return cast(dict[str, dict[str, Any]], result)

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
        result = self._molecule_entity.fetch_by_ids(ids, fields, page_limit)
        return cast(dict[str, dict[str, Any]], result)

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
        result = self._data_validity_entity.fetch_by_ids(comments, fields, page_limit)
        return cast(dict[str, dict[str, Any]], result)

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
        result = self._compound_record_entity.fetch_by_pairs(pairs, fields, page_limit)
        return result

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
        result = self._document_term_entity.fetch_by_ids(ids, fields, page_limit)
        return cast(dict[str, list[dict[str, Any]]], result)

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
        result = self._assay_class_map_entity.fetch_by_ids(assay_ids, fields, page_limit)
        return cast(dict[str, list[dict[str, Any]]], result)

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
        result = self._assay_parameters_entity.fetch_by_ids(assay_ids, fields, page_limit, active_only=active_only)
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
        result = self._assay_classification_entity.fetch_by_ids(class_ids, fields, page_limit)
        return cast(dict[str, dict[str, Any]], result)
