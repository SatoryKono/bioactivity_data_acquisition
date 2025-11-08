"""Unit-тесты общего адаптера SourceConfig."""

from __future__ import annotations

from typing import Any, TypeAlias

import pytest

from bioetl.config.activity import ActivitySourceConfig
from bioetl.config.assay import AssaySourceConfig
from bioetl.config.document import DocumentSourceConfig
from bioetl.config.models.source import SourceConfig
from bioetl.config.pipeline_source import ChemblPipelineSourceConfig
from bioetl.config.target import TargetSourceConfig
from bioetl.config.testitem import TestItemSourceConfig

AdapterType: TypeAlias = type[ChemblPipelineSourceConfig[Any]]


@pytest.mark.unit
@pytest.mark.parametrize(
    "adapter_cls",
    [ActivitySourceConfig, AssaySourceConfig, TestItemSourceConfig],
)
def test_source_adapter_caps_batch_size_and_normalizes_fields(
    adapter_cls: AdapterType,
) -> None:
    source = adapter_cls.from_source(
        SourceConfig(
            batch_size=100,
            parameters={
                "select_fields": [" activity_id ", "activity_id", None, "value"],
                "handshake_endpoint": "/health",
            },
        )
    )

    assert source.batch_size == adapter_cls.defaults.page_size_cap
    assert source.page_size == adapter_cls.defaults.page_size_cap
    assert source.parameters.select_fields == ("activity_id", "value")


@pytest.mark.unit
@pytest.mark.parametrize(
    "adapter_cls",
    [DocumentSourceConfig, TargetSourceConfig, AssaySourceConfig, TestItemSourceConfig],
)
def test_source_adapter_parses_string_select_fields(
    adapter_cls: AdapterType,
) -> None:
    source = adapter_cls.from_source(
        SourceConfig(
            parameters={"select_fields": " id , name , id ,, "},
        )
    )

    assert source.parameters.select_fields == ("id", "name")


@pytest.mark.unit
def test_source_adapter_legacy_from_source_config_alias() -> None:
    config = SourceConfig(batch_size=500, parameters={"select_fields": ["assay_chembl_id"]})

    modern = AssaySourceConfig.from_source(config)
    legacy = AssaySourceConfig.from_source_config(config)

    assert modern.model_dump() == legacy.model_dump()

