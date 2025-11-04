"""Activity pipeline implementation for ChEMBL."""

from __future__ import annotations

import time
from typing import Any, Mapping, Sequence

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.config.models import SourceConfig
from bioetl.core import APIClientFactory, UnifiedLogger
from bioetl.core.api_client import UnifiedAPIClient

from ..base import PipelineBase


class ChemblActivityPipeline(PipelineBase):
    """ETL pipeline extracting activity records from the ChEMBL API."""

    actor = "activity_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._client_factory = APIClientFactory(config)
        self._chembl_release: str | None = None

    @property
    def chembl_release(self) -> str | None:
        """Return the cached ChEMBL release captured during extraction."""

        return self._chembl_release

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # type: ignore[override]
        """Fetch activity payloads from ChEMBL using the unified HTTP client."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        stage_start = time.perf_counter()

        source_config = self._resolve_source_config("chembl")
        base_url = self._resolve_base_url(source_config.parameters)
        client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_activity_client", client)

        self._chembl_release = self._fetch_chembl_release(client, log)

        batch_size = self._resolve_batch_size(source_config)
        limit = self.config.cli.limit
        page_size = min(batch_size, 25)
        if limit is not None:
            page_size = min(page_size, limit)
        page_size = max(page_size, 1)

        records: list[Mapping[str, Any]] = []
        next_endpoint: str | None = "/activity.json"
        params: Mapping[str, Any] | None = {"limit": page_size}
        pages = 0

        while next_endpoint:
            page_start = time.perf_counter()
            response = client.get(next_endpoint, params=params)
            payload = self._coerce_mapping(response.json())
            page_items = self._extract_page_items(payload)

            if limit is not None:
                remaining = max(limit - len(records), 0)
                if remaining == 0:
                    break
                page_items = page_items[:remaining]

            records.extend(page_items)
            pages += 1
            page_duration_ms = (time.perf_counter() - page_start) * 1000.0
            log.debug(
                "chembl_activity.page_fetched",
                endpoint=next_endpoint,
                batch_size=len(page_items),
                total_records=len(records),
                duration_ms=page_duration_ms,
            )

            next_link = self._next_link(payload)
            if not next_link or (limit is not None and len(records) >= limit):
                break
            next_endpoint = next_link
            params = None

        dataframe = pd.DataFrame.from_records(records)
        if not dataframe.empty and "activity_id" in dataframe.columns:
            dataframe = dataframe.sort_values("activity_id").reset_index(drop=True)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(
            "chembl_activity.extract_summary",
            rows=int(dataframe.shape[0]),
            duration_ms=duration_ms,
            chembl_release=self._chembl_release,
            pages=pages,
        )
        return dataframe

    def transform(self, payload: object) -> pd.DataFrame:  # type: ignore[override]
        """Placeholder transform that returns the payload as-is when possible."""

        if isinstance(payload, pd.DataFrame):
            return payload
        if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
            return pd.DataFrame(payload)
        if isinstance(payload, Mapping):
            return pd.DataFrame([payload])
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_source_config(self, name: str) -> SourceConfig:
        try:
            return self.config.sources[name]
        except KeyError as exc:  # pragma: no cover - configuration error path
            msg = f"Source '{name}' is not configured for pipeline '{self.pipeline_code}'"
            raise KeyError(msg) from exc

    @staticmethod
    def _resolve_base_url(parameters: Mapping[str, Any]) -> str:
        base_url = parameters.get("base_url")
        if not isinstance(base_url, str) or not base_url.strip():
            msg = "sources.chembl.parameters.base_url must be a non-empty string"
            raise ValueError(msg)
        return base_url

    @staticmethod
    def _resolve_batch_size(source_config: SourceConfig) -> int:
        batch_size: int | None = getattr(source_config, "batch_size", None)
        if batch_size is None:
            parameters = getattr(source_config, "parameters", {})
            if isinstance(parameters, Mapping):
                candidate = parameters.get("batch_size")
                if isinstance(candidate, int) and candidate > 0:
                    batch_size = candidate
        if batch_size is None or batch_size <= 0:
            batch_size = 25
        return batch_size

    def _fetch_chembl_release(
        self,
        client: UnifiedAPIClient,
        log: Any,
    ) -> str | None:
        response = client.get("/status.json")
        status_payload = self._coerce_mapping(response.json())
        release_value = self._extract_chembl_release(status_payload)
        log.info("chembl_activity.status", chembl_release=release_value)
        return release_value

    @staticmethod
    def _coerce_mapping(payload: Any) -> Mapping[str, Any]:
        if isinstance(payload, Mapping):
            return payload
        return {}

    @staticmethod
    def _extract_chembl_release(payload: Mapping[str, Any]) -> str | None:
        for key in ("chembl_release", "chembl_db_version", "release", "version"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
            if value is not None:
                return str(value)
        return None

    @staticmethod
    def _extract_page_items(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        candidates: list[Mapping[str, Any]] = []
        for key in ("activities", "data", "items", "results"):
            value = payload.get(key)
            if isinstance(value, Sequence):
                candidates = [item for item in value if isinstance(item, Mapping)]
                if candidates:
                    return candidates
        for key, value in payload.items():
            if key == "page_meta":
                continue
            if isinstance(value, Sequence):
                candidates = [item for item in value if isinstance(item, Mapping)]
                if candidates:
                    return candidates
        return []

    @staticmethod
    def _next_link(payload: Mapping[str, Any]) -> str | None:
        page_meta = payload.get("page_meta")
        if isinstance(page_meta, Mapping):
            next_link = page_meta.get("next")
            if isinstance(next_link, str) and next_link:
                return next_link
        return None
