from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest

from bioetl.devtools import cli_doctest_cli as doctest_cli


class DummyLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: Any, **kwargs: Any) -> None:
        self.records.append((event, kwargs))


class DummyUnifiedLogger:
    last: DummyLogger | None = None

    @staticmethod
    def configure() -> None:
        pass

    @staticmethod
    def get(_: str) -> DummyLogger:
        DummyUnifiedLogger.last = DummyLogger()
        return DummyUnifiedLogger.last


def test_extract_bash_commands() -> None:
    content = """
    ```bash
    python -m bioetl.cli.cli_app activity_chembl --config cfg.yaml
    ```
    """
    examples = doctest_cli.extract_bash_commands(content, Path("docs.md"))
    assert len(examples) == 1
    assert "--dry-run" in examples[0].command


def test_extract_bash_commands_handles_multiline() -> None:
    content = """
    ```bash
    python -m bioetl.cli.cli_app document --config cfg.yaml \\
    --limit 5
    ```
    """
    [example] = doctest_cli.extract_bash_commands(content, Path("docs.md"))
    assert "--limit 5" in example.command
    assert example.command.endswith("--output-dir data/output/doctest_test --dry-run")


def test_extract_bash_commands_skips_invalid_commands() -> None:
    content = """
    ```bash
    python -m bioetl.cli.cli_app activity < data.txt
    ```
    """
    examples = doctest_cli.extract_bash_commands(content, Path("docs.md"))
    assert not examples


def test_run_examples(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doctest_cli, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(doctest_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(doctest_cli, "DOCS_ROOT", tmp_path / "docs")
    monkeypatch.setattr(doctest_cli, "ARTIFACTS_DIR", tmp_path / "artifacts")

    doc_path = tmp_path / "example.md"
    doc_path.write_text(
        """
        ```bash
        python -m bioetl.cli.cli_app activity_chembl --config cfg.yaml --dry-run
        ```
        """,
        encoding="utf-8",
    )

    monkeypatch.setattr(doctest_cli, "_iter_markdown_files", lambda: [doc_path])

    def fake_run(cmd: str) -> tuple[int, str, str]:
        assert "activity_chembl" in cmd
        return 0, "ok", ""

    monkeypatch.setattr(doctest_cli, "_run_command", fake_run)
    results, report = doctest_cli.run_examples()
    assert report.exists()
    assert results[0].exit_code == 0
    report_content = report.read_text(encoding="utf-8")
    assert "CLI Doctest Report" in report_content
    assert "✅ Passed" in report_content


def test_run_examples_captures_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doctest_cli, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(doctest_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(doctest_cli, "ARTIFACTS_DIR", tmp_path / "artifacts")

    example = doctest_cli.CLIExample(
        source_file=tmp_path / "docs.md",
        line_number=12,
        command="python -m bioetl.cli.cli_app activity_chembl --dry-run",
    )

    def failing_run(_: str) -> tuple[int, str, str]:
        return 2, "oops", "boom"

    monkeypatch.setattr(doctest_cli, "_run_command", failing_run)

    results, report_path = doctest_cli.run_examples([example])
    assert results[0].exit_code == 2
    report_text = report_path.read_text(encoding="utf-8")
    assert "Failed Examples" in report_text
    assert "boom" in report_text


def test_iter_markdown_files_filters_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    docs_root = tmp_path / "docs"
    activity_dir = docs_root / "pipelines" / "activity-chembl"
    activity_dir.mkdir(parents=True)
    cli_dir = docs_root / "cli"
    cli_dir.mkdir(parents=True)

    existing_files = [
        tmp_path / "README.md",
        cli_dir / "01-cli-commands.md",
        activity_dir / "00-activity-chembl-overview.md",
    ]
    for file_path in existing_files:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(doctest_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(doctest_cli, "DOCS_ROOT", docs_root)

    collected = list(doctest_cli._iter_markdown_files())
    assert {path.name for path in collected} == {file_path.name for file_path in existing_files}


def test_run_command_handles_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        doctest_cli.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(subprocess.TimeoutExpired(cmd="cmd", timeout=1)),
    )
    exit_code, stdout, stderr = doctest_cli._run_command("bioetl.cli.cli_app --help")
    assert exit_code == -1
    assert "timed out" in stderr
    assert stdout == ""


def test_run_command_handles_generic_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        doctest_cli.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("boom")),
    )
    exit_code, stdout, stderr = doctest_cli._run_command("bioetl.cli.cli_app --help")
    assert exit_code == -1
    assert stderr == "boom"
    assert stdout == ""


def test_extract_bash_commands_skips_other_languages() -> None:
    content = """
    ```python
    print("hello")
    ```
    """
    assert not doctest_cli.extract_bash_commands(content, Path("docs.md"))


def test_run_examples_truncates_long_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doctest_cli, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(doctest_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(doctest_cli, "ARTIFACTS_DIR", tmp_path / "artifacts")

    long_command = "python -m bioetl.cli.cli_app activity_chembl --config " + "a" * 80
    example = doctest_cli.CLIExample(
        source_file=tmp_path / "docs.md",
        line_number=1,
        command=long_command + " --dry-run",
    )

    def fake_run(_: str) -> tuple[int, str, str]:
        return 0, "", ""

    monkeypatch.setattr(doctest_cli, "_run_command", fake_run)
    results, report_path = doctest_cli.run_examples([example])
    assert results[0].exit_code == 0
    table = report_path.read_text(encoding="utf-8")
    assert "`python -m bioetl.cli.cli_app activity_chembl --config" in table


def test_extract_cli_examples_handles_missing_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doctest_cli, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(doctest_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(doctest_cli, "DOCS_ROOT", tmp_path / "docs")
    examples = doctest_cli.extract_cli_examples()
    assert examples == []