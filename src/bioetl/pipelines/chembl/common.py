"""Общий каркас Chembl пайплайнов."""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from bioetl.clients.chembl_entity_factory import ChemblClientBundle, ChemblEntityClientFactory
from bioetl.config.models.models import PipelineConfig
from bioetl.pipelines.base import PipelineBase
from .helpers import build_dataframe
from .io import ChemblIO
from .mixins import EnrichmentMixin, NormalizationMixin, ValidationMixin


class BaseChemblPipeline(PipelineBase, NormalizationMixin, EnrichmentMixin, ValidationMixin):
    """Базовый класс, инкапсулирующий повторяемый цикл fetch→normalize→enrich→validate→write."""

    entity_name: str = ""
    id_column: str | None = None

    def __init__(
        self,
        config: PipelineConfig,
        run_id: str,
        source: Iterable[dict[str, Any]] | None = None,
        *,
        io: ChemblIO | None = None,
        writer=None,
    ) -> None:
        super().__init__(config, run_id)
        self.io = io or ChemblIO()
        self._source = source
        self.writer = writer
        self.results: list[pd.DataFrame] = []

    # --- Hooks ---
    def _fetch_source(self) -> Iterable[dict[str, Any]]:
        if self._source is None:
            raise NotImplementedError("_fetch_source must be implemented when source is not provided")
        return self._source

    def _normalize(self, chunk: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        rules = self.get_normalization_rules()
        return self.normalize_records(chunk, **rules)

    def _enrich(self, records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        return self.enrich_records(records, self.get_enrichment_rules())

    def _validate(self, df: pd.DataFrame) -> None:
        self.validate_dataframe(df, self.get_schema())

    def _write(self, df: pd.DataFrame) -> Any:
        writer = getattr(self, "writer", None)
        if writer is None:
            # По умолчанию просто накапливаем результаты в памяти.
            self.results.append(df)
            return None
        return self.io.write_dataframe(df, writer)

    # --- Abstract rule providers ---
    def get_normalization_rules(self) -> Mapping[str, Any]:
        raise NotImplementedError

    def get_enrichment_rules(self) -> Iterable[Any]:
        return []

    def get_schema(self) -> Mapping[str, Any] | None:
        return None

    # --- Configuration helpers ---
    def _resolve_source_config(self, source_name: str) -> Any:
        if self.config is None:
            return None
        return getattr(self.config.domain, "sources", {}).get(source_name)

    def _resolve_batch_size(self, source_name: str, fallback: int) -> int:
        source_config = self._resolve_source_config(source_name)
        if source_config is None:
            return fallback
        batch_size = getattr(source_config, "batch_size", None)
        if batch_size is None and hasattr(source_config, "parameters"):
            batch_size = getattr(source_config.parameters, "batch_size", None)
        return int(batch_size) if batch_size else fallback

    # --- Client helpers ---
    def build_chembl_entity_bundle(self) -> ChemblClientBundle:
        if self.config is None:
            msg = "Pipeline config is required to build Chembl clients"
            raise RuntimeError(msg)
        factory = ChemblEntityClientFactory(self.config)
        source_config = self._resolve_source_config("chembl")
        return factory.build(self.entity_name or "", source_name="chembl", source_config=source_config)

    def fetch_chembl_release(self, bundle: ChemblClientBundle) -> str | None:
        try:
            release_info = bundle.chembl_client.handshake()
            return str(release_info.get("chembl_db_version")) if release_info else None
        except Exception:
            return None

    def _get_entity_client(self, bundle: ChemblClientBundle):
        if self.entity_name == "document" and hasattr(self, "_build_document_client"):
            try:
                return self._build_document_client(bundle)
            except Exception:
                return bundle.entity_client
        return bundle.entity_client

    # --- Extraction ---
    def extract_by_ids(self, ids: Sequence[str], *, select_fields: Sequence[str] | None = None) -> pd.DataFrame:
        normalized_ids = [str(i).strip() for i in ids if str(i).strip()]
        unique_ids = list(dict.fromkeys(normalized_ids))

        if not unique_ids:
            return pd.DataFrame()

        if self.config and getattr(self.config.cli, "dry_run", False):
            return pd.DataFrame()

        limit = getattr(self.config.cli, "limit", None) if self.config else None
        if limit is not None:
            unique_ids = unique_ids[: int(limit)]

        bundle = self.build_chembl_entity_bundle()
        self.fetch_chembl_release(bundle)
        entity_client = self._get_entity_client(bundle)
        if entity_client is None or not hasattr(entity_client, "iterate_by_ids"):
            msg = "Entity client does not support iterate_by_ids"
            raise RuntimeError(msg)

        batch_size = self._resolve_batch_size("chembl", len(unique_ids))
        results: list[Mapping[str, Any]] = []
        for start in range(0, len(unique_ids), batch_size):
            batch = unique_ids[start : start + batch_size]
            fetched = entity_client.iterate_by_ids(batch, select_fields=select_fields)
            results.extend(list(fetched))

        return build_dataframe(results)

    # --- PipelineBase abstract methods implementation ---
    def extract_all(self) -> pd.DataFrame:
        """Extract all records from the source."""
        if self._source is not None:
            combined: list[pd.DataFrame] = []
            for chunk in self.io.chunked_fetch(self._fetch_source()):
                normalized = self._normalize(chunk)
                enriched = self._enrich(normalized)
                df = build_dataframe(enriched)
                combined.append(df)
            if combined:
                return pd.concat(combined, ignore_index=True)
        return pd.DataFrame()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw DataFrame by applying normalization and enrichment."""
        if df.empty:
            return df
        # Convert DataFrame to records for processing
        records = df.to_dict("records")
        normalized = self._normalize(records)
        enriched = self._enrich(normalized)
        return build_dataframe(enriched)

    # --- Pipeline orchestration (legacy method for backward compatibility) ---
    def run(self) -> pd.DataFrame:  # type: ignore[override]
        """Legacy run method that returns DataFrame instead of RunResult."""
        combined: list[pd.DataFrame] = []
        for chunk in self.io.chunked_fetch(self._fetch_source()):
            normalized = self._normalize(chunk)
            enriched = self._enrich(normalized)
            df = build_dataframe(enriched)
            self._validate(df)
            self._write(df)
            combined.append(df)
        if combined:
            return pd.concat(combined, ignore_index=True)
        return pd.DataFrame()
