"""ChEMBL-specific API helpers built on top of :mod:`bioetl.core.api_client`."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from functools import partial
from typing import Any, Callable, cast

from requests import RequestException

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
from bioetl.core.load_meta_store import LoadMetaStore
from bioetl.core.logger import UnifiedLogger

__all__ = ["ChemblClient"]


class ChemblClient:
    """High level client for interacting with the ChEMBL REST API."""

    def __init__(
        self,
        client: UnifiedAPIClient,
        *,
        load_meta_store: LoadMetaStore | None = None,
        job_id: str | None = None,
        operator: str | None = None,
        handshake_timeout: float | tuple[float, float] | None = 10.0,
    ) -> None:
        self._client = client
        self._log = UnifiedLogger.get(__name__).bind(component="chembl_client")
        self._status_cache: dict[str, Mapping[str, Any]] = {}
        self._load_meta_store = load_meta_store
        self._job_id = job_id
        self._operator = operator
        self._chembl_release: str | None = None
        self._api_version: str | None = None
        self._handshake_timeout = self._normalize_timeout(handshake_timeout)
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

    def handshake(
        self,
        endpoint: str = "/status",
        *,
        enabled: bool = True,
        timeout: float | tuple[float, float] | None = None,
    ) -> Mapping[str, Any]:
        """Perform the handshake for ``endpoint`` once and cache the payload."""

        if not enabled:
            self._log.info(
                "chembl.handshake.skipped",
                endpoint=endpoint,
                handshake_enabled=enabled,
            )
            return self._status_cache.get(endpoint, {})

        cached = self._status_cache.get(endpoint)
        if cached is not None:
            return cached

        endpoints_to_try = self._expand_handshake_endpoints(endpoint)
        last_error: Exception | None = None

        effective_timeout = (
            self._handshake_timeout
            if timeout is None
            else self._normalize_timeout(timeout)
        )

        for candidate in endpoints_to_try:
            cached_candidate = self._status_cache.get(candidate)
            if cached_candidate is not None:
                self._status_cache.setdefault(endpoint, cached_candidate)
                return cached_candidate

            try:
                response = self._client.get(candidate, timeout=effective_timeout)
                payload = response.json()
            except RequestException as exc:
                last_error = exc
                self._log.warning(
                    "chembl.handshake.failed",
                    endpoint=candidate,
                    error=str(exc),
                )
                continue
            except Exception as exc:  # pragma: no cover - defensive
                last_error = exc
                self._log.warning(
                    "chembl.handshake.failed",
                    endpoint=candidate,
                    error=str(exc),
                )
                continue

            release = payload.get("chembl_db_version")
            api_version = payload.get("api_version")
            if isinstance(release, str):
                self._chembl_release = release
            if isinstance(api_version, str):
                self._api_version = api_version

            self._status_cache[candidate] = payload
            self._status_cache.setdefault(endpoint, payload)
            self._log.info(
                "chembl.handshake",
                endpoint=candidate,
                chembl_release=self._chembl_release,
                api_version=self._api_version,
            )
            return payload

        self._status_cache.setdefault(endpoint, {})
        if last_error is not None:
            self._log.error(
                "chembl.handshake.unavailable",
                endpoints=endpoints_to_try,
                error=str(last_error),
            )
        return self._status_cache[endpoint]

    @staticmethod
    def _normalize_timeout(
        timeout: float | tuple[float, float] | None,
    ) -> tuple[float, float] | None:
        """Coerce timeout to the shape expected by ``requests``."""

        if timeout is None:
            return None
        if isinstance(timeout, (int, float)):
            value = float(timeout)
            if value <= 0:
                msg = "handshake_timeout must be positive"
                raise ValueError(msg)
            connect = min(value, 3.05)
            return (connect, value)
        if (
            isinstance(timeout, tuple)
            and len(timeout) == 2
            and all(isinstance(part, (int, float)) for part in timeout)
        ):
            connect = float(timeout[0])
            read = float(timeout[1])
            if connect <= 0 or read <= 0:
                msg = "handshake_timeout components must be positive"
                raise ValueError(msg)
            return (connect, read)
        msg = "handshake_timeout must be a positive float or a (connect, read) tuple"
        raise ValueError(msg)

    @staticmethod
    def _expand_handshake_endpoints(endpoint: str) -> tuple[str, ...]:
        """Return a deterministic list of handshake endpoints to try."""

        normalized = endpoint or "/status"
        normalized = normalized if normalized.startswith("/") else f"/{normalized}"

        if normalized.endswith(".json"):
            fallback = normalized[: -len(".json")]
            return (normalized, fallback)

        fallback_json = f"{normalized}.json"
        return (normalized, fallback_json)

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
                response = self._client.get(
                    normalized_url, params=query if next_url == endpoint else None
                )
                payload: Mapping[str, Any] = response.json()
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

    def _normalize_endpoint(self, endpoint: str) -> str:
        normalized_url = endpoint
        if not normalized_url.startswith(("http://", "https://")):
            if normalized_url.startswith("/chembl/api/data/"):
                normalized_url = normalized_url[len("/chembl/api/data/") :]
            elif normalized_url.startswith("chembl/api/data/"):
                normalized_url = normalized_url[len("chembl/api/data/") :]
        return normalized_url

    def _resolve_request_base_url(self, endpoint: str) -> str:
        if endpoint.startswith(("http://", "https://")):
            return endpoint.split("?", 1)[0]
        base = self._client.base_url or ""
        combined = f"{base.rstrip('/')}/{endpoint.lstrip('/')}" if base else endpoint
        return combined.split("?", 1)[0]

    # ------------------------------------------------------------------
    # Internal fetching helper
    # ------------------------------------------------------------------

    def _fetch_entity(
        self,
        entity: Callable[[Iterable[Any], Sequence[str], int], Any] | Any,
        ids: Iterable[Any],
        fields: Sequence[str],
        page_limit: int,
    ) -> Any:
        """Invoke ``fetch_by_ids`` (or a compatible callable) on ``entity``."""

        fetch_by_ids = getattr(entity, "fetch_by_ids", None)
        if fetch_by_ids is not None:
            fetcher = cast(Callable[[Iterable[Any], Sequence[str], int], Any], fetch_by_ids)
        elif callable(entity):
            fetcher = cast(Callable[[Iterable[Any], Sequence[str], int], Any], entity)
        else:
            raise AttributeError("Entity does not provide fetch_by_ids")
        return fetcher(ids, fields, page_limit)

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
        result = self._fetch_entity(self._assay_entity, ids, fields, page_limit)
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
        result = self._fetch_entity(self._molecule_entity, ids, fields, page_limit)
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
        result = self._fetch_entity(self._data_validity_entity, comments, fields, page_limit)
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
        result = self._fetch_entity(
            lambda fetch_pairs, fetch_fields, fetch_page_limit: self._compound_record_entity.fetch_by_pairs(  # noqa: E501
                cast(Iterable[tuple[str, str]], fetch_pairs),
                fetch_fields,
                fetch_page_limit,
            ),
            pairs,
            fields,
            page_limit,
        )
        return cast(dict[tuple[str, str], dict[str, Any]], result)

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
        result = self._fetch_entity(self._document_term_entity, ids, fields, page_limit)
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
        result = self._fetch_entity(self._assay_class_map_entity, assay_ids, fields, page_limit)
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
        fetcher = partial(
            self._assay_parameters_entity.fetch_by_ids,
            active_only=active_only,
        )
        result = self._fetch_entity(fetcher, assay_ids, fields, page_limit)
        return cast(dict[str, list[dict[str, Any]]], result)

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
        result = self._fetch_entity(self._assay_classification_entity, class_ids, fields, page_limit)
        return cast(dict[str, dict[str, Any]], result)
