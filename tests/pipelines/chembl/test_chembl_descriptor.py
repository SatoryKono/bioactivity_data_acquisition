from __future__ import annotations

import pandas as pd
import pandera as pa

from bioetl.core.http import CircuitBreakerOpenError
from bioetl.core.pipeline.unified import (
    BatchExtractionStats,
    ChemblExtractionServiceDescriptor,
    ChemblPipelineBase,
    RunResult,
    UnifiedPipelineBase,
)
from bioetl.pipelines.chembl.common.chembl_extraction_service import (
    ChemblExtractionService,
)


class DummyChemblClient:
    def __init__(self, release: str = "34") -> None:
        self._release = release

    def status(self):  # pragma: no cover - trivial
        return {"chembl_release": self._release}


class DummyChemblPipeline(ChemblPipelineBase):
    def extract(
        self,
        *args,
        **kwargs,
    ) -> pd.DataFrame:  # pragma: no cover - not used
        return pd.DataFrame()

    def transform(
        self,
        df: pd.DataFrame,
        *_args,
        **_kwargs,
    ) -> pd.DataFrame:  # pragma: no cover - passthrough
        return df

    def validate(
        self,
        df: pd.DataFrame,
        *_args,
        **_kwargs,
    ) -> pd.DataFrame:  # pragma: no cover - passthrough
        return df


def test_run_descriptor_extraction_stats_and_data():
    client = DummyChemblClient()
    cache = {"B"}

    def build_context(_pipeline: DummyChemblPipeline):
        return {"chembl_client": client}

    def fetcher_factory(context):
        def fetch(batch):
            if batch is None:
                return pd.DataFrame()
            rows = []
            meta = {"api_calls": 0, "cache_hit": False, "fallback": 0}
            for chembl_id in batch:
                if chembl_id in cache:
                    meta["cache_hit"] = True
                    rows.append(
                        {
                            "chembl_id": chembl_id,
                            "value": chembl_id.lower(),
                        }
                    )
                    continue
                if chembl_id == "C":
                    raise CircuitBreakerOpenError("circuit open")
                meta["api_calls"] += 1
                if chembl_id == "A":
                    meta["fallback"] += 1
                rows.append(
                    {
                        "chembl_id": chembl_id,
                        "value": chembl_id.lower(),
                    }
                )
            return pd.DataFrame(rows), meta

        return fetch

    def finalizer_factory(_context):
        return lambda df: df.assign(processed=True)

    descriptor = ChemblExtractionServiceDescriptor[
        DummyChemblPipeline
    ](
        build_context=build_context,
        fetcher_factory=fetcher_factory,
        finalizer_factory=finalizer_factory,
    )

    pipeline = DummyChemblPipeline(
        config={}, extraction_service=ChemblExtractionService()
    )
    df, stats = pipeline.run_descriptor_extraction(
        descriptor,
        ["A", "B", "C"],
        summary_event="summary",
        batch_size=2,
    )

    assert df.shape[0] == 2
    assert set(df["chembl_id"]) == {"A", "B"}
    assert all(df["processed"])
    assert isinstance(stats, BatchExtractionStats)
    assert stats.rows == 2
    assert stats.api_calls == 1  # B взят из кэша
    assert stats.cache_hits == 2
    assert stats.fallback_count == 1
    assert stats.error_count == 1


def test_run_descriptor_extraction_handles_circuit_breaker():
    def build_context(_pipeline: DummyChemblPipeline):
        return {}

    def fetcher_factory(_context):
        def fetch(_batch):
            raise CircuitBreakerOpenError("circuit open")

        return fetch

    def finalizer_factory(_context):
        return lambda df: df.assign(processed=True)

    descriptor = ChemblExtractionServiceDescriptor[
        DummyChemblPipeline
    ](
        build_context=build_context,
        fetcher_factory=fetcher_factory,
        finalizer_factory=finalizer_factory,
    )

    pipeline = DummyChemblPipeline(
        config={}, extraction_service=ChemblExtractionService()
    )
    df, stats = pipeline.run_descriptor_extraction(
        descriptor,
        ["X"],
        summary_event="summary",
    )

    assert df.empty
    assert stats.error_count == 1
    assert stats.rows == 0


class MinimalPipeline(UnifiedPipelineBase):
    def __init__(
        self,
        config,
        *,
        validator: pa.DataFrameSchema | None = None,
        run_id: str | None = None,
    ):
        super().__init__(config, validator=validator, run_id=run_id)
        self._extracted = False

    def extract(self, *args, **kwargs) -> pd.DataFrame:
        self._extracted = True
        return pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})

    def transform(self, df: pd.DataFrame, *_args, **_kwargs) -> pd.DataFrame:
        return df.assign(value=df["value"].str.upper())

    def validate(self, df: pd.DataFrame, *_args, **_kwargs) -> pd.DataFrame:
        return df


def test_unified_pipeline_dry_run_metadata(tmp_path):
    schema = pa.DataFrameSchema(
        {
            "id": pa.Column(int),
            "value": pa.Column(str),
        }
    )
    pipeline = MinimalPipeline(config={"stage": "demo"}, validator=schema)

    result = pipeline.run(tmp_path / "out", dry_run=True, extended=True)

    assert isinstance(result, RunResult)
    assert result.success is True
    assert result.rows == 0
    assert (tmp_path / "out" / "meta.yaml").exists()
    assert (tmp_path / "out" / "run_manifest.json").exists()
    # dry_run не должен запускать extract
    assert pipeline._extracted is False

