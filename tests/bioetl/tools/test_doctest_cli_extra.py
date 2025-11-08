"""Тесты для инструмента doctest_cli."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bioetl.tools import doctest_cli as module


class _DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def bind(self, **_: Any) -> "_DummyLogger":
        return self


class _DummyUnifiedLogger:
    def __init__(self) -> None:
        self.logger = _DummyLogger()

    def configure(self) -> None:
        return None

    def get(self, _: str) -> _DummyLogger:
        return self.logger


@pytest.fixture()
def project_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path
    module.PROJECT_ROOT = root
    module.DOCS_ROOT = root / "docs"
    module.ARTIFACTS_DIR = root / "artifacts"
    module.DOCS_ROOT.mkdir(parents=True, exist_ok=True)
    module.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    return root


@pytest.fixture(autouse=True)
def patch_logger(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())


def test_extract_bash_commands_handles_multiline(tmp_path: Path, project_paths: Path) -> None:
    content = """
```bash
python -m bioetl.cli.main activity_chembl --dry-run \\
    --output-dir data/output/sample
```
"""
    file_path = tmp_path / "doc.md"
    examples = module.extract_bash_commands(content, file_path)

    assert len(examples) == 1
    example = examples[0]
    assert "--dry-run" in example.command


def test_extract_cli_examples_reads_files(monkeypatch: pytest.MonkeyPatch, project_paths: Path) -> None:
    doc_file = project_paths / "docs.md"
    doc_file.write_text(
        "```bash\npython -m bioetl.cli.main list\n```",
        encoding="utf-8",
    )

    monkeypatch.setattr(module, "_iter_markdown_files", lambda: [doc_file])

    examples = module.extract_cli_examples()

    assert len(examples) == 1
    assert examples[0].command.endswith("list")


def test_run_command_handles_exceptions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("missing")),
    )

    code, stdout, stderr = module._run_command("python -m bioetl.cli.main activity_chembl")
    assert code == -1
    assert "missing" in stderr


def test_write_report_creates_summary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(module, "ARTIFACTS_DIR", tmp_path)
    example = module.CLIExample(source_file=Path("doc.md"), line_number=10, command="cmd")
    result = module.CLIExampleResult(example=example, exit_code=1, stdout="", stderr="error")

    report_path = module._write_report([result])

    assert report_path.exists()
    content = report_path.read_text(encoding="utf-8")
    assert "Failed Examples" in content
    assert "doc.md" in content


def test_run_examples_uses_custom_examples(
    monkeypatch: pytest.MonkeyPatch,
    project_paths: Path,
) -> None:
    source_file = project_paths / "doc.md"
    source_file.write_text("", encoding="utf-8")
    example = module.CLIExample(source_file=source_file, line_number=1, command="cmd --dry-run")

    monkeypatch.setattr(module, "_run_command", lambda cmd: (0, "ok", ""))
    monkeypatch.setattr(module, "_write_report", lambda results: project_paths / "report.md")

    results, report = module.run_examples([example])

    assert len(results) == 1
    assert report == project_paths / "report.md"

