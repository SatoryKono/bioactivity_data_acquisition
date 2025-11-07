"""Утилиты для формирования артефактов отчётов тестирования.

Модуль используется исключительно в тестовых сценариях и вспомогательных
скриптах, обеспечивая единообразную структуру артефактов pytest/coverage.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

TEST_REPORTS_ROOT: Final[Path] = Path("data/output/test-reports")

PYTEST_REPORT_NAME: Final[str] = "pytest-report.json"
COVERAGE_XML_NAME: Final[str] = "coverage.xml"
META_YAML_NAME: Final[str] = "meta.yaml"


def build_timestamp_directory_name(at: datetime) -> str:
    """Вернуть имя подкаталога с меткой времени в формате UTC ISO-8601."""

    return at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True, slots=True)
class TestReportArtifacts:
    """Абсолютные пути до артефактов тестового запуска."""

    root: Path
    pytest_report: Path
    coverage_xml: Path
    meta_yaml: Path


def resolve_artifact_paths(root: Path) -> TestReportArtifacts:
    """Собрать структуру путей в каталоге *root* без побочных эффектов."""

    return TestReportArtifacts(
        root=root,
        pytest_report=root / PYTEST_REPORT_NAME,
        coverage_xml=root / COVERAGE_XML_NAME,
        meta_yaml=root / META_YAML_NAME,
    )


@dataclass(slots=True)
class TestReportMeta:
    """Структура meta.yaml для отчётов тестирования."""

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

