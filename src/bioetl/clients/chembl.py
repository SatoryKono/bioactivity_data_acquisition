"""ChEMBL-specific API helpers built on top of :mod:`bioetl.core.api_client`."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from time import monotonic, perf_counter, sleep
from typing import Any, cast

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
from bioetl.config.models.models import ChemblClientConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.load_meta_store import LoadMetaStore
from bioetl.core.logger import UnifiedLogger
from bioetl.core.mapping_utils import stringify_mapping

__all__ = ["ChemblClient"]


@dataclass(slots=True)
class _HandshakeCacheEntry:
    """Cached payload for preflight handshake."""

    payload: dict[str, Any]
    expires_at: float
    requested_at_utc: datetime
    success: bool
    status_code: int | None


class ChemblClient:
    """High level client for interacting with the ChEMBL REST API."""

    def __init__(
        self,
        client: UnifiedAPIClient,
        *,
        load_meta_store: LoadMetaStore | None = None,
        job_id: str | None = None,
        operator: str | None = None,
        settings: ChemblClientConfig | None = None,
        handshake_timeout: float | tuple[float, float] | None = None,
    ) -> None:
        self._client = client
        self._log = UnifiedLogger.get(__name__).bind(component="chembl_client")
        self._settings = settings or ChemblClientConfig()
        self._preflight_config = self._settings.preflight
        self._timeout_config = self._settings.timeout
        self._circuit_breaker_config = self._settings.circuit_breaker
        self._default_timeout = (
            self._coerce_timeout(handshake_timeout)
            if handshake_timeout is not None
            else self._timeout_from_config()
        )
        self._handshake_budget = max(float(self._preflight_config.budget_seconds), 0.0)
        self._handshake_cache_ttl = max(float(self._preflight_config.cache_ttl_seconds), 0.0)
        self._failure_cache_ttl = max(float(self._circuit_breaker_config.open_seconds), 0.0)
        self._status_cache: dict[str, _HandshakeCacheEntry] = {}
        self._load_meta_store = load_meta_store
        self._job_id = job_id
        self._operator = operator
        self._chembl_release: str | None = None
        self._api_version: str | None = None
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
        endpoint: str | None = None,
        *,
        enabled: bool | None = None,
        timeout: float | tuple[float, float] | None = None,
        budget_seconds: float | None = None,
    ) -> Mapping[str, Any]:
        """Perform the handshake for ``endpoint`` once and cache the payload."""

        resolved_endpoint = self._normalize_endpoint_path(
            endpoint if endpoint is not None else self._preflight_config.url
        )
        resolved_enabled = (
            self._preflight_config.enabled if enabled is None else bool(enabled)
        )

        if not resolved_enabled:
            self._log.info(
                "chembl.handshake.skipped",
                endpoint=resolved_endpoint,
                handshake_enabled=False,
                phase="skip",
            )
            cached_entry = self._get_cached_entry(resolved_endpoint)
            return dict(cached_entry.payload) if cached_entry is not None else {}

        endpoints = self._collect_handshake_endpoints(resolved_endpoint)
        cached_entry = self._get_first_cached_entry(endpoints)
        if cached_entry is not None:
            self._status_cache.setdefault(resolved_endpoint, cached_entry)
            if cached_entry.success:
                return dict(cached_entry.payload)
            self._log.debug(
                "chembl.handshake.cached_failure",
                endpoint=resolved_endpoint,
                requested_at=cached_entry.requested_at_utc.isoformat(),
                phase="cache",
            )
            return dict(cached_entry.payload)

        budget = (
            float(budget_seconds)
            if budget_seconds is not None
            else self._handshake_budget
        )
        deadline = monotonic() + budget if budget and budget > 0 else None
        base_timeout = (
            self._coerce_timeout(timeout)
            if timeout is not None
            else self._default_timeout
        )
        max_attempts = max(1, int(self._preflight_config.retry.total) + 1)
        allowed_methods = {
            method.strip().upper() for method in self._preflight_config.retry.allowed_methods
        }
        status_forcelist = set(self._preflight_config.retry.status_forcelist)
        backoff_factor = float(self._preflight_config.retry.backoff_factor)
        overall_attempt = 0
        last_error: BaseException | None = None

        for candidate in endpoints:
            for attempt in range(1, max_attempts + 1):
                overall_attempt += 1
                now_monotonic = monotonic()
                if deadline is not None and now_monotonic >= deadline:
                    self._log.warning(
                        "chembl.handshake.budget_exhausted",
                        endpoint=candidate,
                        attempt=overall_attempt,
                        budget_seconds=budget,
                        phase="budget_exhausted",
                    )
                    break

                remaining_budget = None if deadline is None else deadline - now_monotonic
                effective_timeout = self._compute_effective_timeout(
                    base_timeout, remaining_budget
                )
                requested_at = datetime.now(timezone.utc)
                start_perf = perf_counter()

                try:
                    response = self._client.get(
                        candidate,
                        timeout=effective_timeout,
                        retry_strategy="none",
                    )
                    payload_raw = response.json()
                    if not isinstance(payload_raw, Mapping):
                        raise TypeError(
                            f"handshake payload must be mapping, got {type(payload_raw).__name__}"
                        )
                    payload = self._coerce_mapping(payload_raw)
                    self._update_versions(payload)

                    duration_ms = (perf_counter() - start_perf) * 1000.0
                    elapsed_ms = self._elapsed_ms(response)
                    self._store_handshake_result(
                        payload=payload,
                        endpoints=endpoints,
                        requested_at=requested_at,
                        success=True,
                        status_code=response.status_code,
                        ttl=self._handshake_cache_ttl,
                    )
                    self._log.info(
                        "chembl.handshake.success",
                        endpoint=candidate,
                        attempt=overall_attempt,
                        status_code=response.status_code,
                        duration_ms=duration_ms,
                        ttfb_ms=elapsed_ms,
                        timeout_connect_ms=effective_timeout[0] * 1000.0,
                        timeout_read_ms=effective_timeout[1] * 1000.0,
                        phase="success",
                    )
                    return payload
                except RequestException as exc:
                    status_code = getattr(getattr(exc, "response", None), "status_code", None)
                    duration_ms = (perf_counter() - start_perf) * 1000.0
                    elapsed_ms = self._elapsed_ms(getattr(exc, "response", None))
                    last_error = exc
                    will_retry = (
                        attempt < max_attempts
                        and "GET" in allowed_methods
                        and (status_code is None or status_code in status_forcelist)
                    )
                    self._log.warning(
                        "chembl.handshake.attempt_failed",
                        endpoint=candidate,
                        attempt=overall_attempt,
                        status_code=status_code,
                        duration_ms=duration_ms,
                        ttfb_ms=elapsed_ms,
                        timeout_connect_ms=effective_timeout[0] * 1000.0,
                        timeout_read_ms=effective_timeout[1] * 1000.0,
                        will_retry=will_retry,
                        error=str(exc),
                        phase="attempt",
                    )
                    if not will_retry:
                        break
                    delay = self._calculate_backoff_delay(attempt, backoff_factor)
                    if deadline is not None:
                        remaining = deadline - monotonic()
                        delay = min(delay, max(remaining, 0.0))
                    if delay > 0:
                        sleep(delay)
                    continue
                except Exception as exc:  # pragma: no cover - defensive guard
                    last_error = exc
                    self._log.warning(
                        "chembl.handshake.exception",
                        endpoint=candidate,
                        attempt=overall_attempt,
                        error=str(exc),
                        phase="attempt",
                    )
                    break

            if deadline is not None and monotonic() >= deadline:
                break

        failure_status = self._extract_status_code(last_error)
        self._store_handshake_result(
            payload={},
            endpoints=endpoints,
            requested_at=datetime.now(timezone.utc),
            success=False,
            status_code=failure_status,
            ttl=self._failure_cache_ttl,
        )
        self._log.error(
            "chembl.handshake.fail_open",
            endpoints=endpoints,
            error=str(last_error) if last_error else None,
            handshake_enabled=resolved_enabled,
            phase="fail_open",
        )
        return {}

    def _collect_handshake_endpoints(self, primary: str) -> tuple[str, ...]:
        sequence: list[str] = []
        for candidate in self._expand_handshake_endpoints(primary):
            sequence.append(candidate)
        for fallback in self._preflight_config.fallback_urls:
            for candidate in self._expand_handshake_endpoints(fallback):
                sequence.append(candidate)

        ordered: list[str] = []
        seen: set[str] = set()
        for candidate in sequence:
            normalized = self._normalize_endpoint_path(candidate)
            if normalized not in seen:
                seen.add(normalized)
                ordered.append(normalized)
        return tuple(ordered)

    @staticmethod
    def _normalize_endpoint_path(endpoint: str | None) -> str:
        if endpoint is None or not endpoint.strip():
            return "/status"
        normalized = endpoint.strip()
        if not normalized.startswith("/"):
            normalized = f"/{normalized}"
        return normalized

    def _get_cached_entry(self, endpoint: str) -> _HandshakeCacheEntry | None:
        entry = self._status_cache.get(endpoint)
        if entry is None:
            return None
        if entry.expires_at <= monotonic():
            self._status_cache.pop(endpoint, None)
            return None
        return entry

    def _get_first_cached_entry(
        self,
        endpoints: Sequence[str],
    ) -> _HandshakeCacheEntry | None:
        for candidate in endpoints:
            entry = self._get_cached_entry(candidate)
            if entry is not None:
                return entry
        return None

    def _store_handshake_result(
        self,
        *,
        payload: Mapping[str, Any],
        endpoints: Sequence[str],
        requested_at: datetime,
        success: bool,
        status_code: int | None,
        ttl: float,
    ) -> None:
        expires_at = monotonic() + max(ttl, 0.0)
        entry = _HandshakeCacheEntry(
            payload=dict(payload),
            expires_at=expires_at,
            requested_at_utc=requested_at,
            success=success,
            status_code=status_code,
        )
        for candidate in endpoints:
            normalized = self._normalize_endpoint_path(candidate)
            self._status_cache[normalized] = entry

    def _timeout_from_config(self) -> tuple[float, float]:
        connect = max(float(self._timeout_config.connect_seconds), 0.001)
        read = max(float(self._timeout_config.read_seconds), 0.001)
        total = float(self._timeout_config.total_seconds)
        if total > 0:
            connect = min(connect, total)
            remaining = max(total - connect, 0.0)
            if remaining > 0:
                read = min(read, remaining)
            if read <= 0:
                read = min(float(self._timeout_config.read_seconds), total)
        return (max(connect, 0.001), max(read, 0.001))

    def _coerce_timeout(
        self,
        timeout: float | tuple[float, float] | None,
    ) -> tuple[float, float]:
        if timeout is None:
            return self._default_timeout
        if isinstance(timeout, (int, float)):
            value = float(timeout)
            if value <= 0:
                msg = "handshake_timeout must be positive"
                raise ValueError(msg)
            connect = min(value, 3.05)
            return (max(connect, 0.001), value)
        try:
            connect_raw, read_raw = tuple(cast(Iterable[Any], timeout))
        except (TypeError, ValueError) as exc:
            msg = "handshake_timeout must be a positive float or a (connect, read) tuple"
            raise ValueError(msg) from exc
        try:
            connect = float(connect_raw)
            read = float(read_raw)
        except (TypeError, ValueError) as exc:
            msg = "handshake_timeout tuple must contain numeric values"
            raise ValueError(msg) from exc
        if connect <= 0 or read <= 0:
            msg = "handshake_timeout components must be positive"
            raise ValueError(msg)
        return (max(connect, 0.001), max(read, 0.001))

    @staticmethod
    def _compute_effective_timeout(
        base_timeout: tuple[float, float],
        remaining_budget: float | None,
    ) -> tuple[float, float]:
        if remaining_budget is None:
            return base_timeout
        connect, read = base_timeout
        if remaining_budget <= 0:
            return (max(connect, 0.001), max(read, 0.001))
        adjusted_connect = min(connect, remaining_budget)
        remaining_after_connect = max(remaining_budget - adjusted_connect, 0.0)
        adjusted_read = (
            min(read, remaining_after_connect) if remaining_after_connect > 0 else min(read, remaining_budget)
        )
        return (max(adjusted_connect, 0.001), max(adjusted_read, 0.001))

    @staticmethod
    def _calculate_backoff_delay(attempt: int, factor: float) -> float:
        if attempt <= 1 or factor <= 0:
            return 0.0
        backoff_multiplier = float(2 ** (attempt - 2))
        return factor * backoff_multiplier

    @staticmethod
    def _elapsed_ms(response: Any | None) -> float | None:
        if response is None:
            return None
        elapsed = getattr(response, "elapsed", None)
        if elapsed is None:
            return None
        if isinstance(elapsed, timedelta):
            return elapsed.total_seconds() * 1000.0
        if isinstance(elapsed, (int, float)):
            return float(elapsed) * 1000.0
        duration_seconds = getattr(elapsed, "total_seconds", None)
        if callable(duration_seconds):
            seconds = duration_seconds()
            if isinstance(seconds, (int, float)):
                numeric_seconds = float(seconds)
                return numeric_seconds * 1000.0
            return None
        return None

    @staticmethod
    def _extract_status_code(error: BaseException | None) -> int | None:
        if isinstance(error, RequestException):
            response = getattr(error, "response", None)
            if response is not None:
                return getattr(response, "status_code", None)
        return None

    def _update_versions(self, payload: Mapping[str, Any]) -> None:
        release = payload.get("chembl_db_version")
        api_version = payload.get("api_version")
        if isinstance(release, str):
            self._chembl_release = release
        if isinstance(api_version, str):
            self._api_version = api_version

    @staticmethod
    def _expand_handshake_endpoints(endpoint: str) -> tuple[str, ...]:
        """Return a deterministic list of handshake endpoints to try."""

        normalized = ChemblClient._normalize_endpoint_path(endpoint)

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

        if self._preflight_config.enabled:
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
                payload = self._coerce_mapping(response.json())
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

    @staticmethod
    def _coerce_mapping(payload: Any) -> dict[str, Any]:
        """Return payload coerced to a plain dict with string keys."""

        if isinstance(payload, Mapping):
            mapping = cast(Mapping[object, Any], payload)
            return stringify_mapping(mapping)
        return {}

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
