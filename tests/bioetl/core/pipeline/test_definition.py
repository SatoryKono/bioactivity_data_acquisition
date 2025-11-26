from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from bioetl.core.pipeline.definition import PipelineDefinition
from bioetl.core.pipeline.factory import StageFactory


@dataclass
class DummyPipeline:
    name: str = "dummy"


class DummyStageFactory(StageFactory):
    def __init__(self) -> None:  # pragma: no cover - not used
        super().__init__(DummyPipeline())


def _runtime_factory() -> Any:
    return DummyPipeline()


def test_validate_accepts_minimal_definition() -> None:
    definition = PipelineDefinition(name="demo", runtime_factory=_runtime_factory)

    definition.validate()


@pytest.mark.parametrize(
    "field, value, expected",
    [
        ("name", "", "pipeline name must be provided"),
        ("runtime_factory", None, "runtime_factory must be callable"),
        ("stages", ("extract", "extract"), "stages must be unique"),
        ("stages", ("",), "stages must not contain empty names"),
        ("stage_factory", object, "stage_factory must be a StageFactory subclass"),
    ],
)
def test_validate_raises_for_invalid_payload(field: str, value: Any, expected: str) -> None:
    kwargs: dict[str, Any] = {
        "name": "demo",
        "runtime_factory": _runtime_factory,
        "stage_factory": DummyStageFactory,
        "stages": ("extract", "transform"),
    }
    kwargs[field] = value
    definition = PipelineDefinition(**kwargs)

    with pytest.raises(ValueError, match=expected):
        definition.validate()
