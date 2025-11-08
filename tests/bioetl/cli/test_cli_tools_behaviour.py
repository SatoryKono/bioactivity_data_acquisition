"""Поведение CLI инструментов с обработкой ошибок."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from bioetl.cli.tools import (
    build_vocab_store as build_vocab_store_cli,
    check_output_artifacts as check_output_artifacts_cli,
    determinism_check as determinism_check_cli,
    doctest_cli as doctest_cli_cli,
    inventory_docs as inventory_docs_cli,
    link_check as link_check_cli,
    schema_guard as schema_guard_cli,
    semantic_diff as semantic_diff_cli,
)
from bioetl.etl.vocab_store import VocabStoreError


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_build_vocab_store_cli_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner: CliRunner) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    output_path = tmp_path / "out.yaml"

    called: dict[str, Any] = {}

    def fake_build_vocab_store(*, src: Path, output: Path) -> Path:
        called["src"] = src
        called["output"] = output
        output.write_text("result", encoding="utf-8")
        return output

    monkeypatch.setattr(build_vocab_store_cli, "build_vocab_store", fake_build_vocab_store)

    result = runner.invoke(
        build_vocab_store_cli.app,
        ["--src", str(src_dir), "--output", str(output_path)],
        prog_name="bioetl-build-vocab-store",
    )

    assert result.exit_code == 0, result.stdout
    assert "Aggregated vocab store" in result.stdout
    assert called["src"] == src_dir
    assert called["output"] == output_path


def test_build_vocab_store_cli_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner: CliRunner) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    output_path = tmp_path / "out.yaml"

    def fake_build_vocab_store(*args: Any, **kwargs: Any) -> Path:
        raise VocabStoreError("boom")

    monkeypatch.setattr(build_vocab_store_cli, "build_vocab_store", fake_build_vocab_store)

    result = runner.invoke(
        build_vocab_store_cli.app,
        ["--src", str(src_dir), "--output", str(output_path)],
        prog_name="bioetl-build-vocab-store",
    )

    assert result.exit_code == 1
    assert "boom" in result.stderr


def test_check_output_artifacts_cli_reports_errors(monkeypatch: pytest.MonkeyPatch, runner: CliRunner) -> None:
    monkeypatch.setattr(
        check_output_artifacts_cli,
        "check_output_artifacts",
        lambda *, max_bytes: ["err1", "err2"],
    )

    result = runner.invoke(
        check_output_artifacts_cli.app,
        ["--max-bytes", "42"],
        prog_name="bioetl-check-output-artifacts",
    )

    assert result.exit_code == 1
    assert "err1" in result.stdout


def test_check_output_artifacts_cli_success(monkeypatch: pytest.MonkeyPatch, runner: CliRunner) -> None:
    monkeypatch.setattr(
        check_output_artifacts_cli,
        "check_output_artifacts",
        lambda *, max_bytes: [],
    )

    result = runner.invoke(
        check_output_artifacts_cli.app,
        ["--max-bytes", "99"],
        prog_name="bioetl-check-output-artifacts",
    )

    assert result.exit_code == 0
    assert "не обнаружено" in result.stdout


def test_determinism_check_cli_reports_failures(monkeypatch: pytest.MonkeyPatch, runner: CliRunner) -> None:
    results = {
        "pipe": SimpleNamespace(deterministic=False),
    }
    monkeypatch.setattr(determinism_check_cli, "run_determinism_check", lambda *, pipelines=None: results)

    result = runner.invoke(
        determinism_check_cli.app,
        [],
        prog_name="bioetl-determinism-check",
    )

    assert result.exit_code == 1
    assert "pipe" in result.stdout


def test_doctest_cli_reports_failures(monkeypatch: pytest.MonkeyPatch, runner: CliRunner, tmp_path: Path) -> None:
    report_path = tmp_path / "report.txt"
    report_path.write_text("report", encoding="utf-8")

    monkeypatch.setattr(doctest_cli_cli, "extract_cli_examples", lambda: ["cmd1", "cmd2"])
    monkeypatch.setattr(
        doctest_cli_cli,
        "run_examples",
        lambda examples: ([SimpleNamespace(exit_code=0), SimpleNamespace(exit_code=1)], report_path),
    )

    result = runner.invoke(
        doctest_cli_cli.app,
        [],
        prog_name="bioetl-doctest-cli",
    )

    assert result.exit_code == 1
    assert str(report_path) in result.stdout


def test_inventory_docs_cli_success(monkeypatch: pytest.MonkeyPatch, runner: CliRunner, tmp_path: Path) -> None:
    inventory_path = tmp_path / "inventory.txt"
    hashes_path = tmp_path / "hashes.txt"
    docs_root = tmp_path / "docs"
    docs_root.mkdir()

    files = (docs_root / "file.md",)

    monkeypatch.setattr(
        inventory_docs_cli,
        "collect_markdown_files",
        lambda *, docs_root=None: files,
    )

    def fake_write_inventory(*, inventory_path: Path, hashes_path: Path, files: tuple[Path, ...]) -> Any:
        inventory_path.write_text("inventory", encoding="utf-8")
        hashes_path.write_text("hashes", encoding="utf-8")
        return SimpleNamespace(files=files, inventory_path=inventory_path, hashes_path=hashes_path)

    monkeypatch.setattr(inventory_docs_cli, "write_inventory", fake_write_inventory)

    result = runner.invoke(
        inventory_docs_cli.app,
        [
            "--inventory",
            str(inventory_path),
            "--hashes",
            str(hashes_path),
            "--docs-root",
            str(docs_root),
        ],
        prog_name="bioetl-inventory-docs",
    )

    assert result.exit_code == 0
    assert "Инвентаризация завершена" in result.stdout


def test_link_check_cli_failure(monkeypatch: pytest.MonkeyPatch, runner: CliRunner) -> None:
    monkeypatch.setattr(link_check_cli, "run_link_check", lambda *, timeout_seconds: 5)

    result = runner.invoke(
        link_check_cli.app,
        ["--timeout", "10"],
        prog_name="bioetl-link-check",
    )

    assert result.exit_code == 5
    assert "Lychee" in result.stdout


def test_schema_guard_cli_failure(monkeypatch: pytest.MonkeyPatch, runner: CliRunner, tmp_path: Path) -> None:
    report_path = tmp_path / "schema-report.md"
    payload = {"conf": {"valid": False}}

    monkeypatch.setattr(
        schema_guard_cli,
        "run_schema_guard",
        lambda: (payload, ["registry-error"], report_path),
    )

    result = runner.invoke(
        schema_guard_cli.app,
        [],
        prog_name="bioetl-schema-guard",
    )

    assert result.exit_code == 1
    assert str(report_path) in result.stdout


def test_semantic_diff_cli_success(monkeypatch: pytest.MonkeyPatch, runner: CliRunner, tmp_path: Path) -> None:
    report_path = tmp_path / "diff.md"
    monkeypatch.setattr(semantic_diff_cli, "run_semantic_diff", lambda: report_path)

    result = runner.invoke(
        semantic_diff_cli.app,
        [],
        prog_name="bioetl-semantic-diff",
    )

    assert result.exit_code == 0
    assert str(report_path) in result.stdout

