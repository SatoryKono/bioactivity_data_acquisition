"""Тесты для `bioetl.tools.determinism_check`."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import bioetl.tools.determinism_check as determinism_check


@pytest.mark.unit
def test_extract_structured_logs_filters_noise() -> None:
    """Убедимся, что извлекаются только корректные JSON-строки и фильтруются метаполя."""

    stdout = """
    {"event": "start", "timestamp": "2020-01-01T00:00:00Z", "run_id": "abc"}
    noise line
    {"event": "finish", "duration_ms": 100, "custom": 1}
    """
    stderr = """
    {"event": "finish", "time": "2020-01-01T00:01:00Z", "custom": 1}
    """

    logs = determinism_check.extract_structured_logs(stdout, stderr)

    assert logs == [
        {"event": "start"},
        {"event": "finish", "custom": 1},
        {"event": "finish", "custom": 1},
    ]


@pytest.mark.unit
def test_compare_logs_reports_differences() -> None:
    """Различия по событиям и ключам отражаются в списке отличий."""

    logs1 = [{"event": "start", "value": 1}, {"event": "finish", "value": 2}]
    logs2 = [{"event": "start", "value": 1, "extra": 1}, {"event": "finish", "value": 3}]

    identical, differences = determinism_check.compare_logs(logs1, logs2)

    assert identical is False
    assert any("key mismatch" in diff for diff in differences)
    assert any("value mismatch" in diff for diff in differences)


@pytest.mark.unit
def test_run_pipeline_dry_run_unknown_pipeline() -> None:
    """Неизвестный пайплайн завершается с ошибкой без запуска subprocess."""

    code, stdout, stderr = determinism_check.run_pipeline_dry_run("unknown", Path("out"))

    assert code == -1
    assert stdout == ""
    assert "Unknown pipeline" in stderr


@pytest.mark.unit
def test_run_pipeline_dry_run_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Таймаут процесса возвращает диагностическое сообщение."""

    def raise_timeout(*_: Any, **__: Any) -> None:
        raise determinism_check.subprocess.TimeoutExpired(cmd="cmd", timeout=1)

    monkeypatch.setattr(determinism_check.subprocess, "run", raise_timeout)

    code, stdout, stderr = determinism_check.run_pipeline_dry_run(
        "activity_chembl", Path("out")
    )

    assert code == -1
    assert stdout == ""
    assert "timed out" in stderr


@pytest.mark.unit
def test_run_determinism_check_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_unified_logger,
    track_path_replace,
) -> None:
    """Ветвь детерминированного пайплайна создает отчёт атомарно."""

    logs_template = '{"event": "start", "value": 1}\n'
    run_outputs = iter([(0, logs_template, ""), (0, logs_template, "")])

    def fake_run_pipeline(pipeline_name: str, output_dir: Path) -> tuple[int, str, str]:
        assert pipeline_name == "activity_chembl"
        assert output_dir.exists()
        return next(run_outputs)

    class DummyTempDir:
        def __enter__(self) -> str:
            run_dir = tmp_path / "runs"
            run_dir.mkdir(parents=True, exist_ok=True)
            return str(run_dir)

        def __exit__(self, *_: Any) -> None:
            return None

    monkeypatch.setattr(determinism_check, "ARTIFACTS_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(determinism_check, "run_pipeline_dry_run", fake_run_pipeline)
    monkeypatch.setattr(
        determinism_check.tempfile, "TemporaryDirectory", lambda: DummyTempDir()
    )
    patch_unified_logger(determinism_check)

    results = determinism_check.run_determinism_check(pipelines=("activity_chembl",))

    report_path = tmp_path / "artifacts" / "DETERMINISM_CHECK_REPORT.md"
    assert report_path.exists()
    assert track_path_replace, "ожидался вызов Path.replace для атомарной записи"

    result = results["activity_chembl"]
    assert result.deterministic is True
    assert result.run1_exit_code == 0
    assert result.run2_exit_code == 0
    assert result.differences == ()

