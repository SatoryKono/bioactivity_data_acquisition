from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bioetl.tools import check_comments


class DummyLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any]]] = []

    def warning(self, event: Any, **kwargs: Any) -> None:
        self.records.append((event, kwargs))


class DummyUnifiedLogger:
    _captured: DummyLogger | None = None

    @staticmethod
    def configure() -> None:
        pass

    @staticmethod
    def get(_: str) -> DummyLogger:
        DummyUnifiedLogger._captured = DummyLogger()
        return DummyUnifiedLogger._captured


def test_run_comment_check_raises_not_implemented(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(check_comments, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(check_comments, "get_project_root", lambda: tmp_path)

    with pytest.raises(NotImplementedError):
        check_comments.run_comment_check()

    assert DummyUnifiedLogger._captured is not None
    event, payload = DummyUnifiedLogger._captured.records[0]
    assert event == check_comments.LogEvents.COMMENT_CHECK_NOT_IMPLEMENTED
    assert payload["root"] == str(tmp_path)

