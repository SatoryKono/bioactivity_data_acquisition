"""Reusable pagination primitives for HTTP-based clients."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

from structlog.stdlib import BoundLogger

from bioetl.clients.http.retry import RetryingSession
from bioetl.infrastructure.logging import LogEvents, UnifiedLogger

__all__ = ["PageResult", "Paginator"]

_DEFAULT_ITEMS_KEYS: tuple[str, ...] = (
    "activities",
    "assays",
    "documents",
    "targets",
    "testitems",
    "molecules",
    "assay_class_maps",
    "assay_classifications",
    "assay_parameters",
    "data",
    "items",
    "results",
)


@dataclass(slots=True)
class PageResult:
    """Container describing a paginated response page."""

    endpoint: str
    status_code: int
    page_index: int
    items: list[dict[str, Any]]
    payload: Mapping[str, Any]
    params: Mapping[str, Any] | None


class Paginator:
    """Iterate over HTTP pages while enforcing deterministic semantics."""

    def __init__(
        self,
        session: RetryingSession,
        *,
        logger: BoundLogger | None = None,
        limit_param_name: str = "limit",
        default_items_keys: Sequence[str] | None = None,
    ) -> None:
        self._session = session
        self._log = logger or UnifiedLogger.get(__name__).bind(component="clients.paginator")
        self._limit_param_name = limit_param_name
        self._default_items_keys = (
            tuple(dict.fromkeys(default_items_keys)) if default_items_keys else _DEFAULT_ITEMS_KEYS
        )
        self._base_url = session.base_url or ""
        self._base_path = urlparse(self._base_url).path.rstrip("/") if self._base_url else ""

    def iterate_pages(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        items_key: str | Sequence[str] | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[PageResult]:
        """Yield :class:`PageResult` objects for the provided endpoint."""

        next_endpoint: str | None = endpoint
        emitted = 0
        pending_params = self._prepare_initial_params(params, page_size)
        page_index = 0

        while next_endpoint:
            # Extract params from URL if present (for subsequent pages from next links)
            # Always extract params from next_endpoint URL for pages after the first one
            if page_index > 0:
                # Check if next_endpoint contains query string
                parsed = urlparse(next_endpoint)
                if parsed.query:
                    # Check if we need to override limit with page_size
                    # Only override if page_size is specified and limit exists in URL
                    query_params = parse_qs(parsed.query, keep_blank_values=True)
                    has_limit_in_url = (
                        self._limit_param_name
                        and self._limit_param_name in query_params
                    )
                    needs_limit_override = (
                        has_limit_in_url
                        and self._limit_param_name
                        and page_size is not None
                        and page_size > 0
                    )
                    
                    if needs_limit_override:
                        # Extract and update params from URL to override limit
                        extracted_params = self._extract_and_update_params_from_url(next_endpoint, page_size)
                        if extracted_params is not None:
                            pending_params = extracted_params
                            self._log.debug(
                                "Extracted params from next_endpoint URL to override limit",
                                next_endpoint=next_endpoint,
                                extracted_params=pending_params,
                                page_size=page_size,
                            )
                        # Remove query string from endpoint since we're passing params separately
                        next_endpoint = parsed.path or next_endpoint.split("?")[0]
                    else:
                        # Keep query string in endpoint, don't extract params
                        # This preserves the original next link as-is
                        pending_params = None
                        self._log.debug(
                            "Keeping query string in endpoint for next link",
                            next_endpoint=next_endpoint,
                        )
                elif page_size is not None and page_size > 0:
                    # No query string in URL, but we still need to set limit
                    if pending_params is None:
                        pending_params = {}
                    if self._limit_param_name:
                        pending_params[self._limit_param_name] = page_size

            normalized_endpoint = self._normalize_endpoint(next_endpoint)
            payload, response = self._session.get_payload(
                normalized_endpoint,
                params=pending_params,
            )
            page_items = self._extract_items(payload, items_key)
            if limit is not None:
                remaining = limit - emitted
                if remaining <= 0:
                    break
                if len(page_items) > remaining:
                    page_items = page_items[:remaining]
            page_length = len(page_items)
            emitted += page_length
            # Log params for all pages to verify limit override is working
            params_snapshot = pending_params.copy() if pending_params is not None else None
            log_kwargs: dict[str, Any] = {
                "endpoint": normalized_endpoint,
                "page_index": page_index,
                "status_code": response.status_code,
                "items_count": page_length,
                "emitted_total": emitted,
                "limit": limit,
            }
            if params_snapshot:
                log_kwargs["params"] = params_snapshot
            self._log.info(LogEvents.HTTP_PAGINATOR_PAGE_FETCHED, **log_kwargs)
            yield PageResult(
                endpoint=normalized_endpoint,
                status_code=response.status_code,
                page_index=page_index,
                items=page_items,
                payload=payload,
                params=pending_params.copy() if pending_params is not None else None,
            )

            page_index += 1
            next_endpoint = self._resolve_next_link(payload)
            self._log.debug(
                LogEvents.HTTP_PAGINATOR_NEXT_LINK_RESOLVED,
                current_endpoint=normalized_endpoint,
                next_endpoint=next_endpoint,
                page_index=page_index,
            )

            # Reset pending_params for next iteration (will be extracted from next_endpoint URL if present)
            pending_params = None

            if limit is not None and emitted >= limit:
                break

    def collect_records(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        items_key: str | Sequence[str] | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return all records for ``endpoint`` using the paginator."""

        records: list[dict[str, Any]] = []
        for page in self.iterate_pages(
            endpoint,
            params=params,
            items_key=items_key,
            page_size=page_size,
            limit=limit,
        ):
            records.extend(page.items)
        return records

    def _prepare_initial_params(
        self,
        params: Mapping[str, Any] | None,
        page_size: int | None,
    ) -> dict[str, Any] | None:
        if params is None:
            if self._limit_param_name and page_size is not None and page_size > 0:
                return {self._limit_param_name: page_size}
            return None

        prepared = dict(params)
        # Always override limit with page_size if specified (don't use setdefault)
        if self._limit_param_name and page_size is not None and page_size > 0:
            prepared[self._limit_param_name] = page_size
        return prepared

    def _extract_items(
        self,
        payload: Mapping[str, Any],
        items_key: str | Sequence[str] | None,
    ) -> list[dict[str, Any]]:
        if items_key is None:
            candidate_keys: Sequence[str] = self._default_items_keys
        elif isinstance(items_key, str):
            candidate_keys = (items_key,)
        else:
            candidate_keys = tuple(items_key)

        for key in candidate_keys:
            items = payload.get(key)
            if isinstance(items, Sequence) and not isinstance(items, (str, bytes, bytearray)):
                collected: list[dict[str, Any]] = []
                for item in items:
                    if isinstance(item, Mapping):
                        collected.append(dict(item))
                if collected:
                    return collected

        fallback: list[dict[str, Any]] = []
        for key, value in payload.items():
            if key == "page_meta":
                continue
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                for item in value:
                    if isinstance(item, Mapping):
                        fallback.append(dict(item))
                if fallback:
                    return fallback
        return []

    def _resolve_next_link(self, payload: Mapping[str, Any]) -> str | None:
        page_meta = payload.get("page_meta")
        if not isinstance(page_meta, Mapping):
            return None
        raw_next = page_meta.get("next")
        if not isinstance(raw_next, str):
            return None
        candidate = raw_next.strip()
        if not candidate:
            return None

        if candidate.startswith(("http://", "https://")):
            parsed = urlparse(candidate)
            path = parsed.path or "/"
            relative = self._strip_base_path(path)
            if parsed.query:
                relative = f"{relative}?{parsed.query}"
            return relative

        return self._strip_base_path(candidate)

    def _strip_base_path(self, value: str) -> str:
        normalized = value.strip()
        if not normalized.startswith("/"):
            normalized = f"/{normalized}" if normalized else "/"

        base_path = self._base_path.lstrip("/")
        candidate = normalized.lstrip("/")

        if base_path and candidate.startswith(base_path):
            candidate = candidate[len(base_path) :]
            if not candidate.startswith("/"):
                candidate = f"/{candidate}" if candidate else "/"
            return candidate or "/"

        chembl_prefix = "chembl/api/data/"
        if candidate.startswith(chembl_prefix):
            candidate = candidate[len(chembl_prefix) :]
            if not candidate.startswith("/"):
                candidate = f"/{candidate}" if candidate else "/"
            return candidate or "/"

        return normalized

    def _normalize_endpoint(self, endpoint: str) -> str:
        if not endpoint.startswith(("http://", "https://")):
            return endpoint

        parsed = urlparse(endpoint)
        path = parsed.path or ""
        relative = self._strip_base_path(path)
        if parsed.query:
            return f"{relative}?{parsed.query}"
        return relative

    def _extract_and_update_params_from_url(
        self,
        endpoint: str,
        page_size: int | None,
    ) -> dict[str, Any] | None:
        """Extract query parameters from endpoint URL and update limit to match page_size.

        This ensures that when ChEMBL API returns a next link with limit=25,
        we override it with our desired page_size (e.g., 200).
        """
        parsed = urlparse(endpoint)
        if not parsed.query:
            # No query params in URL, return page_size as limit if specified
            if self._limit_param_name and page_size is not None and page_size > 0:
                return {self._limit_param_name: page_size}
            return None

        # Parse query string
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        # Convert lists to single values (parse_qs returns lists)
        params: dict[str, Any] = {}
        for key, value_list in query_params.items():
            if value_list and len(value_list) == 1:
                params[key] = value_list[0]
            elif value_list:
                params[key] = value_list

        # Override limit with page_size if specified
        if self._limit_param_name and page_size is not None and page_size > 0:
            params[self._limit_param_name] = page_size

        return params if params else None

