from __future__ import annotations

import os
from pathlib import Path

from tests.utils.determinism import compare_artifacts, enforce_determinism


def test_enforce_determinism_sets_timezone(monkeypatch):
    monkeypatch.delenv("TZ", raising=False)
    enforce_determinism(seed=123)
    assert os.environ["TZ"] == "UTC"


def test_compare_artifacts(tmp_path: Path):
    new_dir = tmp_path / "new"
    golden_dir = tmp_path / "golden"
    new_dir.mkdir()
    golden_dir.mkdir()
    (new_dir / "meta.yaml").write_text("data", encoding="utf-8")
    (golden_dir / "meta.yaml").write_text("data", encoding="utf-8")

    comparison = compare_artifacts(
        new_dir,
        golden_dir,
        filenames=["meta.yaml"],
    )
    assert comparison["meta.yaml"] is True
