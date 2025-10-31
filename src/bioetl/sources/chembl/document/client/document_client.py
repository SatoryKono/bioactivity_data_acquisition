"""High-level ChEMBL client tailored for the document pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import urlencode

import requests

from bioetl.config import PipelineConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.deprecation import warn_legacy_client
from bioetl.core.logger import UnifiedLogger
from bioetl.core.chembl import build_chembl_client_context

logger = UnifiedLogger.get(__name__)

warn_legacy_client(__name__, replacement="bioetl.adapters.chembl.document")


@dataclass(slots=True)
class DocumentFetchCallbacks:
    """Container for pipeline-specific callbacks used by the client."""

    classify_error: Callable[[Exception], str]
    create_fallback: Callable[[str, str, str, Exception | None], Mapping[str, Any]]


class DocumentChEMBLClient:
    """Encapsulate ChEMBL API access for the document pipeline."""

    def __init__(
        self,
        config: PipelineConfig,
        *,
        defaults: Mapping[str, Any] | None = None,
        batch_size_cap: int | None = None,
    ) -> None:
        context = build_chembl_client_context(
            config,
            defaults=defaults,
            batch_size_cap=batch_size_cap,
        )
        self.api_client: UnifiedAPIClient = context.client
        self.batch_size = context.batch_size
        self.max_url_length = int(context.max_url_length or 0) or 1
        self.max_batch_size = int(batch_size_cap or 0) or self.batch_size
        self._document_cache: dict[str, dict[str, Any]] = {}
        self._chembl_release: str | None = None

    @property
    def release(self) -> str | None:
        """Return cached ChEMBL release identifier."""

        return self._chembl_release

    @release.setter
    def release(self, value: str | None) -> None:
        self._chembl_release = value

    def fetch_documents(
        self,
        ids: Sequence[str],
        callbacks: DocumentFetchCallbacks,
    ) -> list[dict[str, Any]]:
        """Fetch document payloads for the supplied identifiers."""

        if not ids:
            return []

        results: list[dict[str, Any]] = []
        for chunk in self._chunked(ids, max(1, self.batch_size)):
            cached_records, to_fetch = self._separate_cached(chunk)
            results.extend(cached_records)

            if not to_fetch:
                continue

            try:
                fetched = self._fetch_documents_recursive(to_fetch)
                for record in fetched:
                    document_id = record.get("document_chembl_id")
                    if document_id:
                        self._document_cache[self._document_cache_key(str(document_id))] = record
                results.extend(fetched)
            except Exception as exc:  # noqa: BLE001
                error_type = callbacks.classify_error(exc)
                logger.error(
                    "document_fetch_failed",
                    chunk=list(to_fetch),
                    error=str(exc),
                    error_type=error_type,
                )
                for document_id in to_fetch:
                    fallback = callbacks.create_fallback(
                        document_id,
                        error_type,
                        str(exc),
                        exc,
                    )
                    self._document_cache[self._document_cache_key(document_id)] = dict(fallback)
                    results.append(dict(fallback))

        return results

    def _fetch_documents_recursive(self, ids: Sequence[str]) -> list[dict[str, Any]]:
        """Recursively fetch documents handling URL and batch constraints."""

        if not ids:
            return []

        if len(ids) > self.max_batch_size:
            midpoint = max(1, len(ids) // 2)
            return self._fetch_documents_recursive(ids[:midpoint]) + self._fetch_documents_recursive(
                ids[midpoint:]
            )

        params = {"document_chembl_id__in": ",".join(ids)}
        full_url = self._build_full_url("/document.json", params)

        if len(full_url) > self.max_url_length and len(ids) > 1:
            midpoint = max(1, len(ids) // 2)
            return self._fetch_documents_recursive(ids[:midpoint]) + self._fetch_documents_recursive(
                ids[midpoint:]
            )

        try:
            response = self.api_client.request_json("/document.json", params=params)
        except requests.exceptions.ReadTimeout:
            if len(ids) <= 1:
                raise
            midpoint = max(1, len(ids) // 2)
            return self._fetch_documents_recursive(ids[:midpoint]) + self._fetch_documents_recursive(
                ids[midpoint:]
            )

        documents = response.get("documents") or response.get("document") or []
        return [doc for doc in documents if isinstance(doc, dict)]

    def _separate_cached(self, ids: Sequence[str]) -> tuple[list[dict[str, Any]], list[str]]:
        cached: list[dict[str, Any]] = []
        missing: list[str] = []
        for document_id in ids:
            key = self._document_cache_key(document_id)
            cached_record = self._document_cache.get(key)
            if cached_record is not None:
                cached.append(cached_record)
            else:
                missing.append(document_id)
        return cached, missing

    def _document_cache_key(self, document_id: str) -> str:
        release = self._chembl_release or "unknown"
        return f"document:{release}:{document_id}"

    def _chunked(self, items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
        for idx in range(0, len(items), size):
            yield items[idx : idx + size]

    def _build_full_url(self, endpoint: str, params: Mapping[str, Any]) -> str:
        base = self.api_client.config.base_url.rstrip("/")
        query = urlencode(params, doseq=False)
        return f"{base}{endpoint}?{query}" if query else f"{base}{endpoint}"

    def clear_cache(self) -> None:
        """Reset in-memory cache (used in tests)."""

        self._document_cache.clear()
