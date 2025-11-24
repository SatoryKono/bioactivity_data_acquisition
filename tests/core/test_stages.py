from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator
from contextlib import contextmanager

import pytest

from bioetl.core.pipeline.stages import (
    BaseStageCommand,
    _CleanupStageCommand,
    _ExtractStageCommand,
    _TransformStageCommand,
    _ValidateStageCommand,
    _WriteStageCommand,
)
from bioetl.core.pipeline.types import StageExecutionOptions


class _DummyLogger:
    def __init__(self, records: list[tuple[str, dict[str, Any]]]) -> None:
        self.records = records

    def info(self, event: str, **kwargs: Any) -> None:  # pragma: no cover - trivial
        self.records.append((event, kwargs))

    def warning(self, event: str, **kwargs: Any) -> None:  # pragma: no cover - trivial
        self.records.append((event, kwargs))


class _DummyContext:
    def __init__(self, pipeline: "_DummyPipeline", output_dir: Path) -> None:
        self.pipeline = pipeline
        self.options = StageExecutionOptions()
        self.output_dir = output_dir
        self.data: dict[str, Any] = {}
        self.result = None
        self._records: list[tuple[str, dict[str, Any]]] = []

    @contextmanager
    def pipeline_stage(self, _stage: str) -> Iterator[_DummyLogger]:
        yield _DummyLogger(self._records)

    def set_payload(self, key: str, value: Any) -> None:
        self.data[key] = value

    def require_payload(self, key: str) -> Any:
        return self.data[key]

    def set_result(self, result: Any) -> None:
        self.result = result


class _DummyPipeline:
    def __init__(self) -> None:
        self.config = SimpleNamespace(cli=SimpleNamespace(extended=False), postprocess=None)
        self.saved_payload: Any = None
        self.closed = False
        self.cleaned = False

    def extract(self, *, mode: Any, ids: Any) -> list[int]:  # pragma: no cover - simple
        return [1, 2, 3]

    def transform(self, df: list[int]) -> list[int]:  # pragma: no cover - simple
        return [value * 2 for value in df]

    def validate(self, df: list[int]) -> list[int]:  # pragma: no cover - simple
        return df

    def save_results(self, df: list[int], output_dir: Path, **_: Any) -> Any:
        self.saved_payload = (list(df), output_dir)
        return SimpleNamespace(write_result=SimpleNamespace(dataset=output_dir / "dataset.csv"))

    def close_resources(self) -> None:  # pragma: no cover - trivial
        self.closed = True

    def _cleanup_registered_clients(self) -> None:  # pragma: no cover - trivial
        self.cleaned = True

    def _safe_len(self, candidate: Any) -> int:  # pragma: no cover - trivial
        return len(candidate)

    def _apply_cli_sample(self, df: list[int]) -> list[int]:  # pragma: no cover - trivial
        return df


def test_stage_commands_execute_in_sequence(tmp_path: Path) -> None:
    pipeline = _DummyPipeline()
    context = _DummyContext(pipeline, tmp_path)

    commands: list[BaseStageCommand] = [
        _ExtractStageCommand(pipeline),
        _TransformStageCommand(pipeline),
        _ValidateStageCommand(pipeline),
        _WriteStageCommand(pipeline),
        _CleanupStageCommand(pipeline),
    ]

    for command in commands:
        assert command.should_run(context.options)
        command.execute(context)

    assert context.result is not None
    assert pipeline.saved_payload == ([2, 4, 6], tmp_path)
    assert pipeline.cleaned is True and pipeline.closed is True


def test_stage_commands_require_payload_order(tmp_path: Path) -> None:
    pipeline = _DummyPipeline()
    context = _DummyContext(pipeline, tmp_path)

    transform = _TransformStageCommand(pipeline)
    with pytest.raises(KeyError):
        transform.execute(context)
