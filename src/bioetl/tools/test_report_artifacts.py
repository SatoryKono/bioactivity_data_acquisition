"""Shared structures and helpers for test report artifacts."""

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
    """Return a UTC ISO-8601 timestamp directory name for reports."""

    return at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True, slots=True)
class TestReportArtifacts:
    """Absolute paths for generated test report artifacts."""

    root: Path
    pytest_report: Path
    coverage_xml: Path
    meta_yaml: Path


def resolve_artifact_paths(root: Path) -> TestReportArtifacts:
    """Construct artifact paths under ``root`` without side effects."""

    return TestReportArtifacts(
        root=root,
        pytest_report=root / "pytest-report.json",
        coverage_xml=root / "coverage.xml",
        meta_yaml=root / "meta.yaml",
    )


@dataclass(slots=True)
class TestReportMeta:
    """Metadata required to populate the test-report `meta.yaml` file."""

    pipeline_version: str
    git_commit: str
    config_hash: str
    row_count: int
    generated_at_utc: str
    blake2_checksum: str
    business_key_hash: str
    status: str

    def to_ordered_dict(self) -> dict[str, str | int]:
        """Return a dictionary with a fixed key order."""

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

