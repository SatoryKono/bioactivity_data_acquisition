"""Общий каркас Chembl пайплайнов."""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from .helpers import build_dataframe
from .io import ChemblIO
from .mixins import EnrichmentMixin, NormalizationMixin, ValidationMixin


class BaseChemblPipeline(NormalizationMixin, EnrichmentMixin, ValidationMixin):
    """Базовый класс, инкапсулирующий повторяемый цикл fetch→normalize→enrich→validate→write."""

    def __init__(
        self,
        source: Iterable[dict[str, Any]] | None = None,
        *,
        io: ChemblIO | None = None,
    ) -> None:
        self.io = io or ChemblIO()
        self._source = source
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

    # --- Pipeline orchestration ---
    def run(self) -> pd.DataFrame:
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
