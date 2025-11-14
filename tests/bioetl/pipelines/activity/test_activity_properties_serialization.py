"""Unit tests targeting activity_properties normalization logic."""

from __future__ import annotations

import json
from typing import Any, cast

import pytest
from structlog.stdlib import BoundLogger

from bioetl.config.models.models import PipelineConfig
from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline


@pytest.fixture
def activity_pipeline(
    pipeline_config_fixture: PipelineConfig, run_id: str
) -> ChemblActivityPipeline:
    return ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]


@pytest.mark.unit
def test_normalize_activity_properties_sequence(activity_pipeline: ChemblActivityPipeline) -> None:
    payload = [
        {"type": "IC50", "value": 10.5, "text_value": None, "result_flag": 1},
        {"type": "NOTE", "text_value": "approximate"},
    ]

    normalized = activity_pipeline._normalize_activity_properties_items(payload)  # noqa: SLF001

    assert normalized is not None
    assert normalized[0]["result_flag"] is True
    assert normalized[0]["type"] == "IC50"
    assert normalized[1]["text_value"] == "approximate"


@pytest.mark.unit
def test_normalize_activity_properties_from_string(
    activity_pipeline: ChemblActivityPipeline,
) -> None:
    payload = '{"type": "Ki", "value": 5.0, "result_flag": 0}'

    normalized = activity_pipeline._normalize_activity_properties_items(payload)  # noqa: SLF001

    assert normalized is not None
    assert normalized[0]["type"] == "Ki"
    assert normalized[0]["result_flag"] is False


@pytest.mark.unit
def test_normalize_activity_properties_invalid_json(
    activity_pipeline: ChemblActivityPipeline, caplog: pytest.LogCaptureFixture
) -> None:
    payload = "free text value"

    with caplog.at_level("WARNING"):
        normalized = activity_pipeline._normalize_activity_properties_items(payload)  # noqa: SLF001

    assert normalized is not None
    assert normalized[0]["text_value"] == "free text value"


@pytest.mark.unit
def test_normalize_activity_properties_unhandled_type(
    activity_pipeline: ChemblActivityPipeline,
) -> None:
    class CaptureLog:
        def __init__(self) -> None:
            self.events: list[dict[str, Any]] = []

        def warning(self, event: str, **fields: Any) -> None:
            self.events.append({"event": event, **fields})

    capture_log = CaptureLog()
    normalized = activity_pipeline._normalize_activity_properties_items(  # noqa: SLF001
        42,
        cast(BoundLogger, capture_log),
    )

    assert normalized is None
    assert capture_log.events
    assert capture_log.events[0]["event"] == "activity_properties_unhandled_type"


@pytest.mark.unit
def test_serialize_activity_properties(activity_pipeline: ChemblActivityPipeline) -> None:
    payload = [{"type": "IC50", "value": 5.0}]

    serialized = activity_pipeline._serialize_activity_properties(payload)  # noqa: SLF001

    assert serialized is not None
    decoded = json.loads(serialized)
    assert isinstance(decoded, list)
    assert decoded[0]["type"] == "IC50"
