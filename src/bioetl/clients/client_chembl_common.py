"""ChEMBL-specific API helpers built on top of :mod:`bioetl.core.http`."""

from __future__ import annotations

import warnings
from collections.abc import Iterable, Iterator, Mapping, Sequence
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from bioetl.clients.client_chembl_base import ChemblEntityFetcherBase
from bioetl.clients.client_exceptions import ConnectionError, HTTPError, RequestException, Timeout
from bioetl.clients.entities.client_assay_class_map import ChemblAssayClassMapEntityClient
from bioetl.clients.entities.client_assay_classification import (
    ChemblAssayClassificationEntityClient,
)
from bioetl.clients.entities.client_assay_entity import ChemblAssayEntityClient
from bioetl.clients.entities.client_assay_parameters import ChemblAssayParametersEntityClient
from bioetl.clients.entities.client_compound_record import ChemblCompoundRecordEntityClient
from bioetl.clients.entities.client_data_validity import ChemblDataValidityEntityClient
from bioetl.clients.entities.client_document_term import ChemblDocumentTermEntityClient
from bioetl.clients.entities.client_molecule import ChemblMoleculeEntityClient
from bioetl.config.loader import _load_yaml
from bioetl.core.http import UnifiedAPIClient
from bioetl.core.logging import LogEvents
from bioetl.core.logging import UnifiedLogger

if TYPE_CHECKING:
    from bioetl.core.runtime.load_meta_store import LoadMetaStore

__all__ = ["ChemblClient", "_resolve_status_endpoint"]

_DEFAULT_STATUS_ENDPOINT = "/status.json"
_CHEMBL_DEFAULTS_PATH = Path(__file__).resolve().parents[3] / "configs" / "defaults" / "chembl.yaml"


@lru_cache(maxsize=1)
def _load_chembl_client_defaults() -> Mapping[str, Any]:
    """Load cached Chembl client defaults from the YAML profile."""
    try:
        payload = _load_yaml(_CHEMBL_DEFAULTS_PATH)
    except FileNotFoundError:
        return {}

    if not isinstance(payload, Mapping):
        return {}

    # Preferred location under top-level `chembl` (schema-compliant profile).
    chembl_section = payload.get("chembl")
    if isinstance(chembl_section, Mapping):
        return dict(chembl_section)

    # Backwards compatibility for legacy `clients.chembl` structure.
    clients_section = payload.get("clients")
    if isinstance(clients_section, Mapping):
        legacy_chembl = clients_section.get("chembl")
        if isinstance(legacy_chembl, Mapping):
            return dict(legacy_chembl)

    return {}


def _resolve_status_endpoint() -> str:
    """Resolve the status endpoint from defaults or fall back to the constant."""
    chembl_defaults = _load_chembl_client_defaults()
    candidate: Any = chembl_defaults.get("status_endpoint")
    if isinstance(candidate, str):
        normalized = candidate.strip()
        if normalized:
            return normalized
    return _DEFAULT_STATUS_ENDPOINT


class ChemblClient:
    """High level client for interacting with the ChEMBL REST API."""

    def __init__(
        self,
        client: UnifiedAPIClient,
        *,
        load_meta_store: "LoadMetaStore | None" = None,
        job_id: str | None = None,
        operator: str | None = None,
    ) -> None:
        """Initialise the high-level client and supporting entity adapters."""
        self._client = client
        self._log = UnifiedLogger.get(__name__).bind(component="chembl_client")
        self._status_cache: dict[str, Mapping[str, Any]] = {}
        self._load_meta_store = load_meta_store
        self._job_id = job_id
        self._operator = operator
        self._chembl_release: str | None = None
        self._api_version: str | None = None
        # Initialize specialized entity clients.
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

    def handshake(self, endpoint: str | None = None) -> Mapping[str, Any]:
        """Perform the handshake once and cache the payload.

        If ``endpoint`` is ``None``, the value is resolved from
        ``chembl.status_endpoint`` (with backwards compatibility for legacy
        ``clients.chembl.status_endpoint``; fallback ``"/status.json"``). The
        effective endpoint is used verbatim; no path normalization is applied.
        """

        resolved_endpoint = endpoint if endpoint is not None else _resolve_status_endpoint()
        if resolved_endpoint not in self._status_cache:
            try:
                response = self._client.get(resolved_endpoint)
                payload = response.json()
            except (ConnectionError, Timeout, HTTPError, RequestException) as exc:
                self._log.error(LogEvents.HTTP_REQUEST_FAILED,
                    endpoint=resolved_endpoint,
                    error=str(exc),
                )
                raise
            self._status_cache[resolved_endpoint] = payload
            release = payload.get("chembl_db_version")
            api_version = payload.get("api_version")
            if isinstance(release, str):
                self._chembl_release = release
            if isinstance(api_version, str):
                self._api_version = api_version
            self._log.info(LogEvents.CHEMBL_HANDSHAKE,
                endpoint=resolved_endpoint,
                chembl_release=self._chembl_release,
                api_version=self._api_version,
            )
        return self._status_cache[resolved_endpoint]

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
        """Yield paginated records and propagate public network exceptions from ``client_exceptions``."""

        self.handshake()
        next_url: str | None = endpoint
        query: dict[str, Any] | None = dict(params) if params is not None else None
        if page_size and query is not None:
            query.setdefault("limit", page_size)
        load_meta_id: str | None = None
        store = self._load_meta_store
        records_fetched = 0
        page_index = 0
        try:
            if store is not None:
                load_meta_id = store.begin_record(
                    "chembl_rest",
                    self._resolve_request_base_url(endpoint),
                    query or {},
                    source_release=self._chembl_release,
                    source_api_version=self._api_version,
                    job_id=self._job_id,
                    operator=self._operator,
                )
            while next_url:
                normalized_url = self._normalize_endpoint(next_url)
                try:
                    response = self._client.get(
                        normalized_url, params=query if next_url == endpoint else None
                    )
                    payload: Mapping[str, Any] = response.json()
                except (ConnectionError, Timeout, HTTPError, RequestException) as exc:
                    self._log.error(LogEvents.HTTP_REQUEST_FAILED,
                        endpoint=normalized_url,
                        error=str(exc),
                    )
                    raise
                items = list(self._extract_items(payload, items_key))
                if load_meta_id is not None and store is not None:
                    pagination_snapshot: dict[str, Any] = {
                        "page_index": page_index,
                        "endpoint": normalized_url,
                        "status_code": response.status_code,
                        "result_count": len(items),
                    }
                    if query is not None and page_index == 0:
                        pagination_snapshot["params"] = dict(query)
                    store.update_pagination(
                        load_meta_id,
                        pagination_snapshot,
                        records_fetched_delta=len(items),
                    )
                for item_raw in items:
                    item_dict = dict(item_raw)
                    if load_meta_id is not None:
                        item_dict["load_meta_id"] = load_meta_id
                    records_fetched += 1
                    yield item_dict
                next_url = self._next_link(payload)
                query = None
                page_index += 1
            if load_meta_id is not None and store is not None:
                store.finish_record(
                    load_meta_id,
                    status="success",
                    records_fetched=records_fetched,
                )
        except Exception as exc:
            if load_meta_id is not None and store is not None:
                store.finish_record(
                    load_meta_id,
                    status="error",
                    records_fetched=records_fetched,
                    error_message=str(exc),
                )
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_items(
        self,
        payload: Mapping[str, Any],
        items_key: str | None,
    ) -> Iterable[Mapping[str, Any]]:
        """Extract entity items from the ChEMBL response payload.

        The implicit lookup heuristic without ``items_key`` is deprecated and will be
        removed in a future release—pass an explicit key (see ``EntityConfig.items_key``).
        """
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
        warnings.warn(
            "The items_key=None heuristic will be removed in a future version; "
            "pass an explicit items_key (for example, via EntityConfig.items_key).",
            PendingDeprecationWarning,
            stacklevel=2,
        )
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
        """Return the pagination link from ``page_meta`` if available."""
        page_meta = payload.get("page_meta")
        if isinstance(page_meta, Mapping):
            page_meta_mapping = cast(Mapping[str, Any], page_meta)
            next_link: str | None = cast(str | None, page_meta_mapping.get("next"))
            if isinstance(next_link, str) and next_link:
                return next_link
        return None

    def _normalize_endpoint(self, endpoint: str) -> str:
        """Normalise the endpoint to be relative when base URL is applied."""
        normalized_url = endpoint
        if not normalized_url.startswith(("http://", "https://")):
            if normalized_url.startswith("/chembl/api/data/"):
                normalized_url = normalized_url[len("/chembl/api/data/") :]
            elif normalized_url.startswith("chembl/api/data/"):
                normalized_url = normalized_url[len("chembl/api/data/") :]
        return normalized_url

    def _resolve_request_base_url(self, endpoint: str) -> str:
        """Combine the client base URL with the endpoint for metadata recording."""
        if endpoint.startswith(("http://", "https://")):
            return endpoint.split("?", 1)[0]
        base = self._client.base_url or ""
        combined = f"{base.rstrip('/')}/{endpoint.lstrip('/')}" if base else endpoint
        return combined.split("?", 1)[0]

    def _fetch_entity_by_ids(
        self,
        entity: ChemblEntityFetcherBase,
        ids: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[str, dict[str, Any]]:
        """Fetch entity records by identifiers using the provided client."""
        result = entity.fetch_by_ids(ids, fields, page_limit)
        return cast(dict[str, dict[str, Any]], result)

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
        return self._fetch_entity_by_ids(self._assay_entity, ids, fields, page_limit)

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
        return self._fetch_entity_by_ids(self._molecule_entity, ids, fields, page_limit)

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
        result = self._assay_parameters_entity.fetch_by_ids(
            assay_ids, fields, page_limit, active_only=active_only
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
        result = self._assay_classification_entity.fetch_by_ids(class_ids, fields, page_limit)
        return cast(dict[str, dict[str, Any]], result)
