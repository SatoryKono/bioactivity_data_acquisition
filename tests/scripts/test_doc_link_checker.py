"""Tests for documentation reference validation helpers."""

from __future__ import annotations

import sys
from importlib import util as importlib_util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "tools" / "qa" / "check_required_docs.py"
MODULE_SPEC = importlib_util.spec_from_file_location(
    "_check_required_docs_for_tests",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:  # pragma: no cover - defensive
    raise RuntimeError("Unable to load check_required_docs module for testing")
checker = importlib_util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = checker
MODULE_SPEC.loader.exec_module(checker)


def _prepare_refactoring_environment(tmp_path: Path) -> None:
    """Create the directory structure required by the checker under ``tmp_path``."""

    (tmp_path / "docs" / "architecture" / "refactoring").mkdir(parents=True)


def test_refactoring_links_pass_when_targets_exist(monkeypatch, tmp_path: Path) -> None:
    """Valid references should produce no errors."""

    _prepare_refactoring_environment(tmp_path)
    target_path = tmp_path / "docs" / "sample.md"
    target_path.parent.mkdir(exist_ok=True)
    target_path.write_text("example", encoding="utf-8")

    ref_doc = tmp_path / "docs" / "architecture" / "refactoring" / "notes.md"
    ref_doc.write_text(
        "See [ref: repo:docs/sample.md@test_refactoring_32] for context.",
        encoding="utf-8",
    )

    monkeypatch.setattr(checker, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.setattr(checker, "REQUIREMENTS", ())

    assert checker._collect_refactoring_link_errors() == []  # noqa: SLF001


def test_refactoring_links_fail_when_target_missing(monkeypatch, tmp_path: Path) -> None:
    """Missing referenced paths should be reported as errors."""

    _prepare_refactoring_environment(tmp_path)
    ref_doc = tmp_path / "docs" / "architecture" / "refactoring" / "notes.md"
    ref_doc.write_text(
        "Broken [ref: repo:docs/missing.md@test_refactoring_32] reference.",
        encoding="utf-8",
    )

    monkeypatch.setattr(checker, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.setattr(checker, "REQUIREMENTS", ())

    errors = checker._collect_refactoring_link_errors()  # noqa: SLF001
    assert errors == [
        "missing repository path 'docs/missing.md' referenced in docs/architecture/refactoring/notes.md"
    ]
