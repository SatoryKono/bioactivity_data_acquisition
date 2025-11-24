import types
from collections.abc import Sequence

import pandas as pd
import pytest
from structlog.stdlib import BoundLogger

from infrastructure.chembl.descriptor import (
    ChemblDescriptorBuilderMixin,
    ChemblDescriptorSpec,
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
)
from infrastructure.config.models.models import PipelineConfig, PipelineMetadata
from infrastructure.config.models.policies import HTTPClientConfig, HTTPConfig
from application.pipelines.errors import PipelineError


class DummyChemblPipeline(  # pyright: ignore[reportIncompatibleMethodOverride]
    ChemblDescriptorBuilderMixin[ChemblPipelineBase],
    ChemblPipelineBase,
):
    """Lightweight pipeline used to exercise descriptor building logic.

    The implementation satisfies :class:`ChemblDescriptorPipelineProtocol` while
    avoiding any external network or configuration dependencies.
    """

    pipeline_code = "dummy_pipeline"
    id_column = "chembl_id"

    def __init__(self) -> None:  # pragma: no cover - no side effects
        dummy_config = PipelineConfig(
            version=1,
            pipeline=PipelineMetadata(name="dummy_pipeline", version="0.0.0"),
            http=HTTPConfig(default=HTTPClientConfig()),
        )  # pyright: ignore[reportCallIssue]
        super().__init__(dummy_config, run_id="test-run")

    # Minimal helpers required by the protocol, implemented with no-op logic

    def _resolve_source_config(self, name: str):  # type: ignore[override]
        return types.SimpleNamespace(
            parameters=None,
            parameters_mapping=lambda: {},
            batch_size=None,
        )

    def ensure_chembl_release(  # type: ignore[override]
        self,
        context: ChemblExtractionContext,
        log: BoundLogger,
    ) -> tuple[None, dict[str, object]]:
        _ = (context, log)
        return None, {}

    def _resolve_batch_size(self, source_config):  # type: ignore[override]
        return 25

    def _resolve_page_size(self, batch_size, limit, *, hard_cap=25):  # type: ignore[override]
        return min(int(batch_size), int(hard_cap))

    def _normalize_parameters(self, parameters):  # type: ignore[override]
        return {}

    def publish_release_metadata(  # type: ignore[override]
        self,
        payload=None,
        *,
        release=None,
        metadata=None,
        include_metadata=True,
    ):
        return dict(payload or {})

    def record_extract_metadata(  # type: ignore[override]
        self,
        *,
        filters,
        requested_at_utc,
        **kwargs,
    ) -> None:
        return None

    def _coerce_mapping(self, payload):  # type: ignore[override]
        return dict(payload or {})

    # Implement minimal concrete hooks so the pipeline is not abstract for tests

    def extract_by_ids(  # type: ignore[override]
        self,
        ids: Sequence[str],
    ) -> pd.DataFrame:
        _ = ids
        return pd.DataFrame()

    def transform(  # type: ignore[override]
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        return df

    def get_descriptor_strategy_factory(self):  # pragma: no cover - not used
        raise AssertionError("strategy factory should not be used in unit tests")

    def descriptor_spec(self) -> ChemblDescriptorSpec[ChemblPipelineBase]:  # type: ignore[override]
        raise AssertionError("descriptor_spec is provided explicitly by tests")


@pytest.fixture()
def dummy_pipeline() -> DummyChemblPipeline:
    return DummyChemblPipeline()


def _make_minimal_spec(**overrides):
    base = dict(
        name="test_descriptor",
        source_name="chembl",
        source_config_factory=lambda cfg: cfg,
        context=types.SimpleNamespace(),
        id_column="chembl_id",
        summary_event="chembl.test.summary",
        must_have_fields=("chembl_id", "value"),
        default_select_fields=("chembl_id", "value"),
        post_processors=(),
        sort_by=("chembl_id",),
        empty_frame_factory=None,
        dry_run_handler=None,
        summary_extra=None,
        hard_page_size_cap=25,
    )
    base.update(overrides)
    return ChemblDescriptorSpec[ChemblPipelineBase](**base)  # type: ignore[arg-type]


def test_build_descriptor_happy_path(dummy_pipeline: DummyChemblPipeline) -> None:
    spec = _make_minimal_spec()
    dummy_pipeline.descriptor_spec = lambda: spec  # type: ignore[assignment]

    descriptor = dummy_pipeline.build_descriptor()

    assert isinstance(descriptor, ChemblExtractionDescriptor)
    assert descriptor.id_column == "chembl_id"
    assert isinstance(descriptor.must_have_fields, tuple)
    assert descriptor.must_have_fields == ("chembl_id", "value")
    assert isinstance(descriptor.default_select_fields, tuple)
    assert descriptor.default_select_fields == ("chembl_id", "value")
    assert isinstance(descriptor.post_processors, tuple)


def test_build_descriptor_invalid_id_column_raises(dummy_pipeline: DummyChemblPipeline) -> None:
    spec = _make_minimal_spec(id_column="missing", must_have_fields=("chembl_id",))

    dummy_pipeline.descriptor_spec = lambda: spec  # type: ignore[assignment]

    with pytest.raises(PipelineError) as excinfo:
        dummy_pipeline.build_descriptor()

    message = str(excinfo.value)
    assert "missing" in message
    assert "test_descriptor" in message


def test_build_descriptor_invalid_hard_page_size_cap_raises(dummy_pipeline: DummyChemblPipeline) -> None:
    spec = _make_minimal_spec(hard_page_size_cap=0)

    dummy_pipeline.descriptor_spec = lambda: spec  # type: ignore[assignment]

    with pytest.raises(PipelineError) as excinfo:
        dummy_pipeline.build_descriptor()

    assert "hard_page_size_cap" in str(excinfo.value)


@pytest.mark.parametrize("default_select_fields", [[], ()])
def test_build_descriptor_empty_default_select_fields_raise(
    dummy_pipeline: DummyChemblPipeline,
    default_select_fields,
) -> None:
    spec = _make_minimal_spec(default_select_fields=default_select_fields)

    dummy_pipeline.descriptor_spec = lambda: spec  # type: ignore[assignment]

    with pytest.raises(PipelineError) as excinfo:
        dummy_pipeline.build_descriptor()

    assert "default_select_fields" in str(excinfo.value)


def test_build_descriptor_normalizes_collections(dummy_pipeline: DummyChemblPipeline) -> None:
    spec = _make_minimal_spec(
        must_have_fields=["chembl_id", "value"],
        default_select_fields=["chembl_id", "value"],
        post_processors=[
            lambda pipeline, df, ctx, log: df,  # type: ignore[arg-type]
        ],
        sort_by=["value", "chembl_id"],
    )

    dummy_pipeline.descriptor_spec = lambda: spec  # type: ignore[assignment]

    descriptor = dummy_pipeline.build_descriptor()

    assert isinstance(descriptor.must_have_fields, tuple)
    assert descriptor.must_have_fields == ("chembl_id", "value")

    assert isinstance(descriptor.default_select_fields, tuple | type(None))
    assert descriptor.default_select_fields == ("chembl_id", "value")

    assert isinstance(descriptor.post_processors, tuple)
    assert len(descriptor.post_processors) == 1

    assert isinstance(descriptor.sort_by, tuple | type(None))
    assert descriptor.sort_by == ("value", "chembl_id")


def test_build_descriptor_is_deterministic_for_same_spec(dummy_pipeline: DummyChemblPipeline) -> None:
    spec = _make_minimal_spec(
        must_have_fields=["chembl_id", "value"],
        default_select_fields=["chembl_id", "value"],
        sort_by=["value", "chembl_id"],
    )

    dummy_pipeline.descriptor_spec = lambda: spec  # type: ignore[assignment]

    first = dummy_pipeline.build_descriptor()
    second = dummy_pipeline.build_descriptor()

    assert isinstance(first, ChemblExtractionDescriptor)
    assert isinstance(second, ChemblExtractionDescriptor)

    assert first.name == second.name
    assert first.must_have_fields == second.must_have_fields
    assert first.default_select_fields == second.default_select_fields
    assert first.sort_by == second.sort_by
