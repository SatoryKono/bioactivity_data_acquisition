import re
from typing import Any, cast

import pytest
from structlog.stdlib import BoundLogger

from bioetl.core.logging import LogEvents, client_event, emit, stage_event


def test_log_event_values_format() -> None:
    pattern = re.compile(r"^[a-z0-9_.-]+$")
    for event in LogEvents:
        assert pattern.fullmatch(event.value)
        assert event.value.count(".") >= 2


def test_stage_event_valid() -> None:
    event = stage_event("extract", "start")
    assert event == "stage.extract.start"


@pytest.mark.parametrize("stage, suffix", [("", "start"), ("extract", "start!"), ("UPPER", "start")])
def test_stage_event_invalid(stage: str, suffix: str) -> None:
    with pytest.raises(ValueError):
        stage_event(stage, suffix)


def test_client_event_known() -> None:
    assert client_event("request") == LogEvents.CLIENT_REQUEST.value
    assert client_event("retry") == LogEvents.CLIENT_RETRY.value


def test_client_event_unknown() -> None:
    with pytest.raises(ValueError):
        client_event("unknown")


def test_emit_delegates_to_logger() -> None:
    class DummyLogger:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        def info(self, message: str, **fields: Any) -> None:
            self.calls.append((message, fields))

    dummy = DummyLogger()
    emit(cast(BoundLogger, dummy), LogEvents.CLI_RUN_START, run_id="x")
    assert dummy.calls == [(LogEvents.CLI_RUN_START.value, {"run_id": "x"})]
