"""Assay pipeline implementation for ChEMBL."""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core import APIClientFactory, UnifiedLogger
from bioetl.clients.chembl import ChemblClient

from ..base import PipelineBase
from .assay_config import AssaySourceConfig
from ...sources.chembl.assay import ChemblAssayClient


class ChemblAssayPipeline(PipelineBase):
    """ETL pipeline extracting assay records from the ChEMBL API."""

    actor = "assay_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._client_factory = APIClientFactory(config)
        self._chembl_release: str | None = None

    @property
    def chembl_release(self) -> str | None:
        """Return the cached ChEMBL release captured during extraction."""

        return self._chembl_release

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Fetch assay payloads from ChEMBL using the configured HTTP client."""

        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config("chembl")
        source_config = AssaySourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(source_config)

        http_client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_assay_http", http_client)

        chembl_client = ChemblClient(http_client)
        assay_client = ChemblAssayClient(
            chembl_client,
            batch_size=source_config.batch_size,
            max_url_length=source_config.max_url_length,
        )

        assay_client.handshake(
            endpoint=source_config.parameters.handshake_endpoint,
            enabled=source_config.parameters.handshake_enabled,
        )
        self._chembl_release = assay_client.chembl_release

        log.info(  # type: ignore[misc]
            "chembl_assay.handshake",
            chembl_release=self._chembl_release,
            handshake_endpoint=source_config.parameters.handshake_endpoint,
            handshake_enabled=source_config.parameters.handshake_enabled,
        )

        if self.config.cli.dry_run:
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(  # type: ignore[misc]
                "chembl_assay.extract_skipped",
                dry_run=True,
                duration_ms=duration_ms,
                chembl_release=self._chembl_release,
            )
            return pd.DataFrame()

        records: list[Mapping[str, Any]] = []
        limit = self.config.cli.limit
        page_size = source_config.batch_size

        for item in assay_client.iterate_all(limit=limit, page_size=page_size):
            records.append(item)

        dataframe = pd.DataFrame.from_records(records)
        if not dataframe.empty and "assay_chembl_id" in dataframe.columns:
            dataframe = dataframe.sort_values("assay_chembl_id").reset_index(drop=True)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(  # type: ignore[misc]
            "chembl_assay.extract_summary",
            rows=int(dataframe.shape[0]),
            duration_ms=duration_ms,
            chembl_release=self._chembl_release,
            handshake_endpoint=source_config.parameters.handshake_endpoint,
            limit=limit,
        )
        return dataframe

    # The transform/validate/write stages will be implemented in subsequent steps.

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_source_config(self, name: str):
        try:
            return self.config.sources[name]
        except KeyError as exc:  # pragma: no cover - configuration error path
            msg = f"Source '{name}' is not configured for pipeline '{self.pipeline_code}'"
            raise KeyError(msg) from exc

    @staticmethod
    def _resolve_base_url(source_config: AssaySourceConfig) -> str:
        base_url = source_config.parameters.base_url or "https://www.ebi.ac.uk/chembl/api/data"
        if not isinstance(base_url, str) or not base_url.strip():
            msg = "sources.chembl.parameters.base_url must be a non-empty string"
            raise ValueError(msg)
        return base_url.rstrip("/")
