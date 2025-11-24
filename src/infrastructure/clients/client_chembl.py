"""ChEMBL-specific API helpers built on top of :mod:`infrastructure.http`."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from infrastructure.clients.client_chembl_entity_base import ChemblEntityClientProtocol
from infrastructure.clients.client_exceptions import (
    ConnectionError,
    HTTPError,
    RequestException,
    Timeout,
)
from infrastructure.clients.entities.client_assay_class_map import (
    ChemblAssayClassMapEntityClient,
)
from infrastructure.clients.entities.client_assay_classification import (
    ChemblAssayClassificationEntityClient,
)
from infrastructure.clients.entities.client_assay_entity import ChemblAssayEntityClient
from infrastructure.clients.entities.client_assay_parameters import (
    ChemblAssayParametersEntityClient,
)
from infrastructure.clients.entities.client_compound_record import (
    ChemblCompoundRecordEntityClient,
)
from infrastructure.clients.entities.client_data_validity import (
    ChemblDataValidityEntityClient,
)
from infrastructure.clients.entities.client_document_term import (
    ChemblDocumentTermEntityClient,
)
from infrastructure.clients.entities.client_molecule import ChemblMoleculeEntityClient
from infrastructure.clients.http import PageResult, Paginator, RetryingSession
from infrastructure.config.loader import _load_yaml
from infrastructure.http import UnifiedAPIClient
from infrastructure.logging import LogEvents, UnifiedLogger

if TYPE_CHECKING:
    from infrastructure.runtime.load_meta_store import LoadMetaStore

__all__ = ["ChemblClient", "_resolve_status_endpoint"]

_DEFAULT_STATUS_ENDPOINT = "/status"
_CHEMBL_DEFAULTS_PATH = (
    Path(__file__).resolve().parents[3]
    / "configs"
    / "defaults"
    / "chembl.yaml"
)


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
        load_meta_store: LoadMetaStore | None = None,
        job_id: str | None = None,
        operator: str | None = None,
    ) -> None:
        """Initialise the high-level client and supporting entity adapters."""
        self._client = client
        self._log = UnifiedLogger.get(__name__).bind(component="chembl_client")
        self._session = RetryingSession(client, logger=self._log)
        self._paginator = Paginator(self._session, logger=self._log)
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
        self._assay_classification_entity = (
            ChemblAssayClassificationEntityClient(self)
        )
        self._compound_record_entity = ChemblCompoundRecordEntityClient(self)

    def circuit_breaker_time_until_half_open(self) -> float | None:
        """Return the time in seconds until the circuit breaker transitions to half-open.

        Returns None if the circuit breaker is not in open state or if it's already
        ready to transition to half-open.

        Returns
        -------
        float | None:
            Time in seconds until half-open transition, or None if not applicable.
        """
        return self._client.circuit_breaker_time_until_half_open()

    # ------------------------------------------------------------------
    # Discovery / handshake
    # ------------------------------------------------------------------

    def handshake(self, endpoint: str | None = None) -> Mapping[str, Any]:
        """Perform the handshake once and cache the payload.

        If ``endpoint`` is ``None``, the value is resolved from
        ``chembl.status_endpoint`` (with backwards compatibility for legacy
        ``clients.chembl.status_endpoint``; fallback ``"/status"``). The
        effective endpoint is used verbatim; no path normalization is applied.
        """

        resolved_endpoint = (
            endpoint if endpoint is not None else _resolve_status_endpoint()
        )
        if resolved_endpoint not in self._status_cache:
            try:
                response = self._client.get(resolved_endpoint)
                payload = response.json()
            except (
                ConnectionError,
                Timeout,
                HTTPError,
                RequestException,
            ) as exc:
                self._log.error(
                    LogEvents.HTTP_REQUEST_FAILED,
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
            self._log.info(
                LogEvents.CHEMBL_HANDSHAKE,
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
        limit: int | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Yield paginated records and propagate public network exceptions from ``client_exceptions``.

        Parameters
        ----------
        endpoint
            API endpoint path (e.g., "/activity.json").
        params
            Optional query parameters to include in requests.
        page_size
            Number of records per page (default: 200).
        items_key
            Key in response payload containing the items array (auto-detected if None).
        limit
            Maximum number of records to fetch across all pages. If None, fetches all available records.
        """

        self.handshake()
        query: dict[str, Any] | None = (
            dict(params) if params is not None else None
        )
        load_meta_id: str | None = None
        store = self._load_meta_store
        records_fetched = 0
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
            for page in self._paginator.iterate_pages(
                endpoint,
                params=query,
                items_key=items_key,
                page_size=page_size,
                limit=limit,
            ):
                self._record_pagination_snapshot(page, store, load_meta_id)
                for item_raw in page.items:
                    item_dict = dict(item_raw)
                    if load_meta_id is not None:
                        item_dict["load_meta_id"] = load_meta_id
                    records_fetched += 1
                    yield item_dict
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

    def _resolve_request_base_url(self, endpoint: str) -> str:
        """Combine the client base URL with the endpoint for metadata recording."""
        if endpoint.startswith(("http://", "https://")):
            return endpoint.split("?", 1)[0]
        base = self._client.base_url or ""
        combined = (
            f"{base.rstrip('/')}/{endpoint.lstrip('/')}" if base else endpoint
        )
        return combined.split("?", 1)[0]

    def _record_pagination_snapshot(
        self,
        page: PageResult,
        store: LoadMetaStore | None,
        load_meta_id: str | None,
    ) -> None:
        if load_meta_id is None or store is None:
            return
        snapshot: dict[str, Any] = {
            "page_index": page.page_index,
            "endpoint": self._resolve_request_base_url(page.endpoint),
            "status_code": page.status_code,
            "result_count": len(page.items),
        }
        if page.page_index == 0 and page.params is not None:
            snapshot["params"] = dict(page.params)
        store.update_pagination(
            load_meta_id,
            snapshot,
            records_fetched_delta=len(page.items),
        )

    def _fetch_entities(
        self,
        entity: str,
        ids: Iterable[str],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """Fetch entity records by identifiers using a named entity adapter."""

        attr_name = f"_{entity}_entity"
        try:
            entity_client: ChemblEntityClientProtocol = getattr(
                self, attr_name
            )
        except AttributeError as exc:  # pragma: no cover - defensive branch
            msg = f"ChemblClient does not define entity adapter '{entity}'."
            raise AttributeError(msg) from exc

        identifiers = tuple(ids)
        result = entity_client.fetch_by_ids(
            identifiers,
            fields=fields,
            page_limit=page_limit,
        )
        return result.frame

    # ------------------------------------------------------------------
    # Assay fetching
    # ------------------------------------------------------------------

    def fetch_assays_by_ids(
        self,
        ids: Iterable[str],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """Fetch assay entries by ``assay_chembl_id`` and return a DataFrame."""
        return self._fetch_entities(
            "assay",
            ids,
            fields=fields,
            page_limit=page_limit,
        )

    # ------------------------------------------------------------------
    # Molecule fetching
    # ------------------------------------------------------------------

    def fetch_molecules_by_ids(
        self,
        ids: Iterable[str],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """Fetch molecule entries by ``molecule_chembl_id`` and return a DataFrame."""
        return self._fetch_entities(
            "molecule",
            ids,
            fields=fields,
            page_limit=page_limit,
        )

    # ------------------------------------------------------------------
    # Data validity lookup fetching
    # ------------------------------------------------------------------

    def fetch_data_validity_lookup(
        self,
        comments: Iterable[str],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """Fetch ``data_validity_lookup`` entries by comment and return a DataFrame."""
        return self._fetch_entities(
            "data_validity",
            comments,
            fields=fields,
            page_limit=page_limit,
        )

    # ------------------------------------------------------------------
    # Compound record fetching
    # ------------------------------------------------------------------

    def fetch_compound_records_by_pairs(
        self,
        pairs: Iterable[tuple[str, str]],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
        chunk_size: int | None = None,
    ) -> pd.DataFrame:
        """Fetch ``compound_record`` entries for ``(molecule, document)`` pairs."""
        return self._compound_record_entity.fetch_by_pairs(
            pairs,
            fields=fields,
            page_limit=page_limit,
            chunk_size=chunk_size,
        )

    # ------------------------------------------------------------------
    # Document term fetching
    # ------------------------------------------------------------------

    def fetch_document_terms_by_ids(
        self,
        ids: Iterable[str],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """Fetch ``document_term`` entries by ``document_chembl_id``."""
        return self._fetch_entities(
            "document_term",
            ids,
            fields=fields,
            page_limit=page_limit,
        )

    # ------------------------------------------------------------------
    # Assay parameters fetching
    # ------------------------------------------------------------------

    def fetch_assay_parameters_by_assay_ids(
        self,
        assay_ids: Iterable[str],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
        active_only: bool = True,
    ) -> pd.DataFrame:
        """Fetch ``assay_parameters`` entries by ``assay_chembl_id``."""
        return self._assay_parameters_entity.fetch_by_ids(
            assay_ids,
            fields=fields,
            page_limit=page_limit,
            active_only=active_only,
        ).frame

    # ------------------------------------------------------------------
    # Assay class fetching
    # ------------------------------------------------------------------

    def fetch_assay_classes_by_class_ids(
        self,
        class_ids: Iterable[str],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """Fetch ``assay_class`` entries by ``assay_class_id`` from /assay_class.json."""
        return self._fetch_entities(
            "assay_class",
            class_ids,
            fields=fields,
            page_limit=page_limit,
        )

    def fetch_assay_classifications_by_class_ids(
        self,
        class_ids: Iterable[str],
        *,
        fields: Sequence[str] | None = None,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """Fetch ``assay_class`` entries by ``assay_class_id`` from /assay_class.json.

        Legacy method name - now uses /assay_class.json endpoint.
        """
        return self.fetch_assay_classes_by_class_ids(
            class_ids,
            fields=fields,
            page_limit=page_limit,
        )
