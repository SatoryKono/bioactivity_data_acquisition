"""Тесты для doctest CLI-примеров."""

from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from bioetl.tools import doctest_cli


class _LoggerStub:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **context: Any) -> None:
        self.events.append((event, context))


@pytest.fixture
def doctest_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> tuple[_LoggerStub, Path, Path]:
    """Изолированная среда и заглушка логгера."""

    logger = _LoggerStub()
    project_root = tmp_path
    docs_root = project_root / "docs"
    docs_root.mkdir()
    artifacts_dir = project_root / "artifacts"

    monkeypatch.setattr("bioetl.tools.doctest_cli.PROJECT_ROOT", project_root, raising=False)
    monkeypatch.setattr("bioetl.tools.doctest_cli.DOCS_ROOT", docs_root, raising=False)
    monkeypatch.setattr("bioetl.tools.doctest_cli.ARTIFACTS_DIR", artifacts_dir, raising=False)
    monkeypatch.setattr("bioetl.tools.doctest_cli.UnifiedLogger.configure", lambda: None)
    monkeypatch.setattr("bioetl.tools.doctest_cli.UnifiedLogger.bind", lambda **_: None)
    monkeypatch.setattr("bioetl.tools.doctest_cli.UnifiedLogger.get", lambda *_: logger)
    monkeypatch.setattr("bioetl.tools.doctest_cli.UnifiedLogger.stage", lambda *args, **kwargs: nullcontext())

    return logger, project_root, docs_root


@pytest.mark.unit
def test_extract_bash_commands_parses_multiline(tmp_path: Path) -> None:
    """Многострочный пример преобразуется в команду с флагами детерминизма."""

    md_content = """
```bash
python -m bioetl.cli.main run-activity \\
  --profile prod
--limit 10
```
"""
    file_path = tmp_path / "example.md"
    examples = doctest_cli.extract_bash_commands(md_content, file_path)

    assert len(examples) == 1
    example = examples[0]
    normalized = " ".join(example.command.replace("\\", "").split())
    assert normalized.startswith(
        "python -m bioetl.cli.main run-activity --profile prod --limit 10"
    )
    assert "--output-dir data/output/doctest_test --dry-run" in normalized


@pytest.mark.unit
def test_extract_cli_examples_reads_files(
    doctest_env: tuple[_LoggerStub, Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Чтение CLI-примеров из подменённых markdown-файлов."""

    logger, _project_root, docs_root = doctest_env
    doc = docs_root / "cli" / "01-cli-commands.md"
    doc.parent.mkdir(parents=True, exist_ok=True)
    doc.write_text(
        """
```bash
python -m bioetl.cli.main list
```
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "bioetl.tools.doctest_cli._iter_markdown_files",
        lambda: [doc],
    )

    examples = doctest_cli.extract_cli_examples()

    assert len(examples) == 1
    assert examples[0].source_file == doc
    assert any(event[0] == "cli_examples_extracted" for event in logger.events)


@pytest.mark.unit
def test_run_examples_uses_provided_examples(
    doctest_env: tuple[_LoggerStub, Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Проверяем обработку результатов запуска и отчёт."""

    logger, project_root, _ = doctest_env
    examples = [
        doctest_cli.CLIExample(
            source_file=project_root / "a.md",
            line_number=10,
            command="bioetl.cli.main list",
        ),
        doctest_cli.CLIExample(
            source_file=project_root / "b.md",
            line_number=20,
            command="python -m bioetl.cli.main run-assay --dry-run",
        ),
    ]

    results_queue = [
        (0, "ok", ""),
        (1, "", "failure"),
    ]

    def fake_run_command(cmd: str) -> tuple[int, str, str]:
        return results_queue.pop(0)

    report_path = Path("report.md")

    monkeypatch.setattr("bioetl.tools.doctest_cli._run_command", fake_run_command)
    monkeypatch.setattr(
        "bioetl.tools.doctest_cli._write_report",
        lambda payload: report_path,
    )

    results, path = doctest_cli.run_examples(examples)

    assert path == report_path
    assert [item.exit_code for item in results] == [0, 1]
    assert any(event[0] == "cli_example_result" for event in logger.events)


@pytest.mark.unit
def test_run_command_handles_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Таймаут процесса возвращает код -1 и сообщение об ошибке."""

    def raise_timeout(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd="cmd", timeout=1)

    import subprocess

    monkeypatch.setattr("bioetl.tools.doctest_cli.subprocess.run", raise_timeout)

    code, stdout, stderr = doctest_cli._run_command("bioetl.cli.main list")

    assert code == -1
    assert stdout == ""
    assert "timed out" in stderr


@pytest.mark.unit
def test_run_command_handles_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Произвольные исключения конвертируются в код -1."""

    import subprocess

    def raise_error(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("bioetl.tools.doctest_cli.subprocess.run", raise_error)

    code, stdout, stderr = doctest_cli._run_command("bioetl.cli.main list")

    assert code == -1
    assert stdout == ""
    assert stderr == "boom"


@pytest.mark.unit
def test_write_report_creates_summary(doctest_env: tuple[_LoggerStub, Path, Path]) -> None:
    """Формируем отчёт и проверяем содержимое."""

    _ = doctest_env
    results = [
        doctest_cli.CLIExampleResult(
            example=doctest_cli.CLIExample(Path("file.md"), 1, "bioetl.cli.main list"),
            exit_code=0,
            stdout="ok",
            stderr="",
        ),
        doctest_cli.CLIExampleResult(
            example=doctest_cli.CLIExample(Path("file.md"), 2, "bioetl.cli.main invalid"),
            exit_code=1,
            stdout="",
            stderr="error",
        ),
    ]

    report_path = doctest_cli._write_report(results)

    content = report_path.read_text(encoding="utf-8")
    assert "CLI Doctest Report" in content
    assert "Failed Examples" in content
    assert "❌ FAIL" in content

