"""Tests for utilities that build test-report artifacts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from bioetl.tools.test_report_artifacts import (
    TEST_REPORTS_ROOT,
    TestReportArtifacts,
    TestReportMeta,
    build_timestamp_directory_name,
    resolve_artifact_paths,
)


def test_build_timestamp_directory_name_utc() -> None:
    at = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

    assert build_timestamp_directory_name(at) == "20241231T235959Z"


def test_build_timestamp_directory_name_converts_to_utc() -> None:
    local_zone = timezone(timedelta(hours=3))
    at = datetime(2024, 12, 31, 23, 59, 59, tzinfo=local_zone)

    assert build_timestamp_directory_name(at) == "20241231T205959Z"


def test_resolve_artifact_paths(tmp_path: Path) -> None:
    artifacts = resolve_artifact_paths(tmp_path)

    assert artifacts == TestReportArtifacts(
        root=tmp_path,
        pytest_report=tmp_path / "pytest-report.json",
        coverage_xml=tmp_path / "coverage.xml",
        meta_yaml=tmp_path / "meta.yaml",
    )


def test_test_report_meta_to_ordered_dict() -> None:
    meta = TestReportMeta(
        pipeline_version="1.2.3",
        git_commit="abc123",
        config_hash="cfg",
        row_count=42,
        generated_at_utc="2024-12-31T23:59:59Z",
        blake2_checksum="deadbeef",
        business_key_hash="cafebabe",
        status="passed",
    )

    assert list(meta.to_ordered_dict().keys()) == [
        "pipeline_version",
        "git_commit",
        "config_hash",
        "row_count",
        "generated_at_utc",
        "blake2_checksum",
        "business_key_hash",
        "status",
    ]


def test_test_reports_root_points_to_output_dir() -> None:
    assert TEST_REPORTS_ROOT == Path("data/output/test-reports")

