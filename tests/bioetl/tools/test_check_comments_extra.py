"""Дополнительный тест для check_comments."""

from __future__ import annotations

import pytest

from bioetl.tools import check_comments as module


class _DummyLogger:
    def __init__(self) -> None:
        self.events: list[str] = []

    def warning(self, event: str, **_: object) -> None:
        self.events.append(event)

    def bind(self, **_: object) -> "_DummyLogger":
        return self


class _DummyUnifiedLogger:
    def __init__(self) -> None:
        self.logger = _DummyLogger()

    def configure(self) -> None:
        return None

    def get(self, _: str) -> _DummyLogger:
        return self.logger


def test_run_comment_check_raises_not_implemented(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())
    monkeypatch.setattr(module, "get_project_root", lambda: None)

    with pytest.raises(NotImplementedError):
        module.run_comment_check()

