"""Общие структуры и утилиты для отчётов о запуске тестов."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

__all__ = [
    "TEST_REPORTS_ROOT",
    "TestReportArtifacts",
    "TestReportMeta",
    "build_timestamp_directory_name",
    "resolve_artifact_paths",
]

TEST_REPORTS_ROOT: Final[Path] = Path("data/output/test-reports")


def build_timestamp_directory_name(at: datetime) -> str:
    """Вернуть имя каталога отчёта по метке времени UTC ISO-8601."""

    return at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True, slots=True)
class TestReportArtifacts:
    """Абсолютные пути артефактов отчёта о тестировании."""

    root: Path
    pytest_report: Path
    coverage_xml: Path
    meta_yaml: Path


def resolve_artifact_paths(root: Path) -> TestReportArtifacts:
    """Построить структуру путей в каталоге *root* без побочных эффектов."""

    return TestReportArtifacts(
        root=root,
        pytest_report=root / "pytest-report.json",
        coverage_xml=root / "coverage.xml",
        meta_yaml=root / "meta.yaml",
    )


@dataclass(slots=True)
class TestReportMeta:
    """Данные для формирования `meta.yaml` отчёта."""

    pipeline_version: str
    git_commit: str
    config_hash: str
    row_count: int
    generated_at_utc: str
    blake2_checksum: str
    business_key_hash: str
    status: str

    def to_ordered_dict(self) -> dict[str, str | int]:
        """Вернуть словарь с фиксированным порядком ключей."""

        return {
            "pipeline_version": self.pipeline_version,
            "git_commit": self.git_commit,
            "config_hash": self.config_hash,
            "row_count": self.row_count,
            "generated_at_utc": self.generated_at_utc,
            "blake2_checksum": self.blake2_checksum,
            "business_key_hash": self.business_key_hash,
            "status": self.status,
        }

