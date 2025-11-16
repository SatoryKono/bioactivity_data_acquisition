from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bioetl.devtools.cli_check_output_artifacts import check_output_artifacts


class CaptureLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, Any]]] = []

    def info(self, event: str, **context: Any) -> None:
        self.records.append(("info", event, context))

    def warning(self, event: str, **context: Any) -> None:
        self.records.append(("warning", event, context))

    def error(self, event: str, **context: Any) -> None:
        self.records.append(("error", event, context))


class LoggerFacade:
    def __init__(self) -> None:
        self.logger = CaptureLogger()

    @staticmethod
    def configure() -> None:
        return None

    def get(self, name: str | None = None) -> CaptureLogger:
        return self.logger


def test_check_output_artifacts_reports_all_issues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    output_dir = repo_root / "data" / "output"
    output_dir.mkdir(parents=True)
    oversized = output_dir / "huge.parquet"
    oversized.write_bytes(b"0" * 2)

    facade = LoggerFacade()
    monkeypatch.setattr("bioetl.devtools.cli_check_output_artifacts.UnifiedLogger", facade)
    monkeypatch.setattr("bioetl.devtools.cli_check_output_artifacts.get_project_root", lambda: repo_root)
    monkeypatch.setattr(
        "bioetl.devtools.cli_check_output_artifacts.git_ls",
        lambda *paths: [Path("data/output/tracked.csv"), Path("data/output/.gitkeep")],
    )
    monkeypatch.setattr(
        "bioetl.devtools.cli_check_output_artifacts.git_diff_cached",
        lambda *paths, **kwargs: [Path("data/output/new.csv")],
    )

    errors = check_output_artifacts(max_bytes=1)

    assert len(errors) == 3
    assert "Tracked artifacts detected" in errors[0]
    assert "New artifacts staged" in errors[1]
    assert "Large files found" in errors[2]
    assert facade.logger.records[-1][0] == "info"


def test_check_output_artifacts_passes_when_clean(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    (repo_root / "data" / "output").mkdir(parents=True)

    facade = LoggerFacade()
    monkeypatch.setattr("bioetl.devtools.cli_check_output_artifacts.UnifiedLogger", facade)
    monkeypatch.setattr("bioetl.devtools.cli_check_output_artifacts.get_project_root", lambda: repo_root)
    monkeypatch.setattr("bioetl.devtools.cli_check_output_artifacts.git_ls", lambda *paths: [])
    monkeypatch.setattr(
        "bioetl.devtools.cli_check_output_artifacts.git_diff_cached",
        lambda *paths, **kwargs: [],
    )

    errors = check_output_artifacts()

    assert errors == []
    assert facade.logger.records[-1][0] == "info"
