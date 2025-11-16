"""Unit tests for helper methods implemented in ``ChemblPipelineBase``."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any, Type

import pandas as pd
import pytest
from pytest import MonkeyPatch
from structlog.testing import capture_logs

from bioetl.config.models.models import PipelineConfig
from bioetl.config.models.source import SourceConfig
from bioetl.core import LogEvents, UnifiedLogger
from bioetl.pipelines.chembl.activity import run as activity_run
from bioetl.pipelines.chembl.assay import run as assay_run
from bioetl.chembl.common import ChemblPipelineBase
from bioetl.pipelines.chembl.document import run as document_run
from bioetl.pipelines.chembl.target import run as target_run
from bioetl.pipelines.chembl.testitem import run as testitem_run
from bioetl.schemas import SchemaRegistryEntry
from bioetl.schemas.pipeline_contracts import get_out_schema


class _DummyChemblPipeline(ChemblPipelineBase):
    actor = "dummy_chembl"

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - pass-through
        return df


class _NormalizationProbePipeline(ChemblPipelineBase):
    actor = "normalization_probe"

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - pass-through
        return df

    def _normalize_identifiers(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:  # noqa: PLR6301
        if df.empty:
            return df

        normalized = df.copy()
        if "identifier" in normalized.columns:
            normalized["identifier"] = (
                normalized["identifier"].astype(str).str.strip().str.upper()
            )
        return normalized

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:  # noqa: PLR6301
        if df.empty:
            return df

        normalized = df.copy()
        if "name" in normalized.columns:
            normalized["name"] = normalized["name"].astype(str).str.strip()
        return normalized


@pytest.fixture
def dummy_pipeline(pipeline_config_fixture: PipelineConfig, run_id: str) -> _DummyChemblPipeline:
    return _DummyChemblPipeline(config=pipeline_config_fixture, run_id=run_id)


@pytest.mark.unit
def test_initialize_output_schema_uses_registry_lookup(
    dummy_pipeline: _DummyChemblPipeline,
    monkeypatch: MonkeyPatch,
) -> None:
    registry_entry = get_out_schema(dummy_pipeline.pipeline_code)
    recorded: list[str] = []

    def fake_get_out_schema(pipeline_code: str) -> SchemaRegistryEntry:
        recorded.append(pipeline_code)
        if pipeline_code == dummy_pipeline.pipeline_code:
            return registry_entry
        raise KeyError(pipeline_code)

    monkeypatch.setattr(
        "bioetl.chembl.common.descriptor.get_out_schema",
        fake_get_out_schema,
    )

    dummy_pipeline.initialize_output_schema()

    assert recorded[-1] == dummy_pipeline.pipeline_code
    assert recorded[0] == dummy_pipeline.actor
    assert dummy_pipeline._output_schema_entry is registry_entry
    assert dummy_pipeline._output_schema is registry_entry.schema
    assert dummy_pipeline._output_column_order == registry_entry.column_order


@pytest.mark.unit
def test_initialize_output_schema_accepts_override(
    dummy_pipeline: _DummyChemblPipeline,
) -> None:
    registry_entry = get_out_schema(dummy_pipeline.pipeline_code)
    extra_cache = {"probe": "value"}

    dummy_pipeline.initialize_output_schema(
        registry_entry,
        extra_cache=extra_cache,
    )

    assert dummy_pipeline._output_schema_entry is registry_entry
    assert dummy_pipeline._output_schema is registry_entry.schema
    assert dummy_pipeline._output_column_order == registry_entry.column_order
    assert dummy_pipeline._output_schema_cache == extra_cache


@pytest.mark.unit
def test_resolve_base_url_defaults_and_validation(dummy_pipeline: _DummyChemblPipeline) -> None:
    assert (
        dummy_pipeline._resolve_base_url({})  # noqa: SLF001
        == "https://www.ebi.ac.uk/chembl/api/data"
    )
    assert (
        dummy_pipeline._resolve_base_url({"base_url": "https://example.org/api/"})  # noqa: SLF001
        == "https://example.org/api"
    )
    with pytest.raises(ValueError):
        dummy_pipeline._resolve_base_url({"base_url": "   "})  # noqa: SLF001


@pytest.mark.unit
def test_resolve_select_fields_from_source_config(dummy_pipeline: _DummyChemblPipeline) -> None:
    source = SourceConfig(
        enabled=True,
        parameters={"select_fields": ["activity_id", 123]},
    )
    resolved = dummy_pipeline._resolve_select_fields(source, default_fields=("fallback",))  # noqa: SLF001
    assert resolved == ["activity_id", "123"]

    empty_source = SourceConfig(enabled=True, parameters={})
    fallback = dummy_pipeline._resolve_select_fields(empty_source, default_fields=("fallback",))  # noqa: SLF001
    assert fallback == ["fallback"]


@pytest.mark.unit
def test_merge_select_fields_deduplicates(dummy_pipeline: _DummyChemblPipeline) -> None:
    merged = dummy_pipeline._merge_select_fields(["field_a", "field_b"], ("field_b", "field_c"))  # noqa: SLF001
    assert merged == ["field_a", "field_b", "field_c"]

    no_fields = dummy_pipeline._merge_select_fields(None, None)  # noqa: SLF001
    assert no_fields is None


@pytest.mark.unit
def test_resolve_page_size_respects_limit(dummy_pipeline: _DummyChemblPipeline) -> None:
    capped = dummy_pipeline._resolve_page_size(batch_size=100, limit=None, hard_cap=50)  # noqa: SLF001
    assert capped == 50

    limited = dummy_pipeline._resolve_page_size(batch_size=40, limit=10, hard_cap=50)  # noqa: SLF001
    assert limited == 10


@pytest.mark.unit
def test_dispatch_extract_mode_prefers_cli_input(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
    monkeypatch: MonkeyPatch,
) -> None:
    cli_with_input = pipeline_config_fixture.cli.model_copy(update={"input_file": "ids.csv"})
    config_with_input = pipeline_config_fixture.model_copy(update={"cli": cli_with_input})
    pipeline = _DummyChemblPipeline(config=config_with_input, run_id=run_id)

    monkeypatch.setattr(
        pipeline,
        "_read_input_ids",
        lambda *, id_column_name, limit, sample: ["ID1", "ID2"],
    )

    batch_calls: dict[str, Any] = {}

    def batch_callback(ids: Sequence[str]) -> pd.DataFrame:
        batch_calls["ids"] = list(ids)
        return pd.DataFrame({"identifier": ids})

    def full_callback() -> pd.DataFrame:
        batch_calls["full_called"] = True
        return pd.DataFrame({"identifier": []})

    log = UnifiedLogger.get(__name__).bind(component="chembl_test")
    result = pipeline._dispatch_extract_mode(  # noqa: SLF001
        log,
        event_name="chembl.dispatch_test",
        batch_callback=batch_callback,
        full_callback=full_callback,
        id_column_name="identifier",
    )

    assert list(result["identifier"]) == ["ID1", "ID2"]
    assert batch_calls["ids"] == ["ID1", "ID2"]
    assert "full_called" not in batch_calls


@pytest.mark.unit
def test_dispatch_extract_mode_falls_back_to_full(
    dummy_pipeline: _DummyChemblPipeline,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        dummy_pipeline,
        "_read_input_ids",
        lambda *, id_column_name, limit, sample: [],
    )

    execution: dict[str, Any] = {}

    def batch_callback(ids: Sequence[str]) -> pd.DataFrame:
        execution["batch_called"] = True
        return pd.DataFrame({"identifier": ids})

    def full_callback() -> pd.DataFrame:
        execution["full_called"] = True
        return pd.DataFrame({"identifier": ["ALL"]})

    log = UnifiedLogger.get(__name__).bind(component="chembl_test")
    result = dummy_pipeline._dispatch_extract_mode(  # noqa: SLF001
        log,
        event_name="chembl.dispatch_test",
        batch_callback=batch_callback,
        full_callback=full_callback,
        id_column_name="identifier",
    )

    assert list(result["identifier"]) == ["ALL"]
    assert "batch_called" not in execution
    assert execution["full_called"] is True


@pytest.mark.integration
def test_read_input_ids_logs_stage_and_path(
    dummy_pipeline: _DummyChemblPipeline,
    tmp_path: Path,
) -> None:
    UnifiedLogger.configure()

    csv_path = tmp_path / "input_ids.csv"
    pd.DataFrame({"identifier": ["CHEMBL1", "CHEMBL2"]}).to_csv(csv_path, index=False)

    dummy_pipeline.config.cli.input_file = csv_path.name  # type: ignore[attr-defined]
    dummy_pipeline.config.paths.input_root = str(tmp_path)  # type: ignore[attr-defined]

    with capture_logs() as captured:
        ids = dummy_pipeline._read_input_ids(  # noqa: SLF001
            id_column_name="identifier",
            limit=None,
            sample=None,
        )

    assert ids == ["CHEMBL1", "CHEMBL2"]

    log_events = [event for event in captured if event.get("event") == LogEvents.INPUT_IDS_READ]
    assert log_events, "Expected INPUT_IDS_READ event to be emitted"
    last_event = log_events[-1]
    assert last_event.get("stage") == "extract"
    assert last_event.get("path") == str(csv_path)

    UnifiedLogger.reset()


@pytest.mark.unit
def test_normalize_and_enforce_schema_applies_shared_rules(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    pipeline = _NormalizationProbePipeline(config=pipeline_config_fixture, run_id=run_id)
    log = UnifiedLogger.get(__name__).bind(component="chembl_test")

    raw = pd.DataFrame(
        {
            "identifier": [" chembl123 "],
            "name": ["  Example Record  "],
        }
    )
    column_order = ["identifier", "name", "row_subtype", "row_index"]

    normalized = pipeline._normalize_and_enforce_schema(  # noqa: SLF001
        raw,
        column_order,
        log,
        order_columns=True,
    )

    assert list(normalized.columns[: len(column_order)]) == column_order
    assert normalized.loc[0, "identifier"] == "CHEMBL123"
    assert normalized.loc[0, "name"] == "Example Record"
    assert normalized.loc[0, "row_subtype"] == pipeline.pipeline_code
    assert normalized.loc[0, "row_index"] == 0
    # Original input should remain unchanged
    assert raw.loc[0, "identifier"] == " chembl123 "


@pytest.mark.unit
@pytest.mark.parametrize(
    ("pipeline_cls", "expected_code", "schema_suffix"),
    [
        (activity_run.ChemblActivityPipeline, "activity_chembl", "ActivitySchema"),
        (assay_run.ChemblAssayPipeline, "assay_chembl", "AssaySchema"),
        (document_run.ChemblDocumentPipeline, "document_chembl", "DocumentSchema"),
        (target_run.ChemblTargetPipeline, "target_chembl", "TargetSchema"),
        (testitem_run.TestItemChemblPipeline, "testitem_chembl", "TestItemSchema"),
    ],
)
def test_pipeline_code_configures_output_schema(
    pipeline_cls: Type[ChemblPipelineBase],
    expected_code: str,
    schema_suffix: str,
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    pipeline_metadata = pipeline_config_fixture.pipeline
    config = pipeline_config_fixture.model_copy(
        update={"pipeline": pipeline_metadata.model_copy(update={"name": expected_code})},
    )

    pipeline = pipeline_cls(config=config, run_id=run_id)  # type: ignore[reportAbstractUsage]

    descriptor = get_out_schema(expected_code)

    assert pipeline.pipeline_code == expected_code
    assert pipeline._output_schema_entry is not None
    assert pipeline._output_schema_entry.identifier.endswith(schema_suffix)
    assert pipeline._output_schema_entry.identifier == descriptor.identifier
    assert pipeline._output_schema is descriptor.schema
    assert pipeline._output_column_order == descriptor.column_order

