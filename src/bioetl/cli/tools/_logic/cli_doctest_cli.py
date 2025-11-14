"""Run doctest-style checks for CLI examples documented in markdown."""

from __future__ import annotations

import re
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from bioetl.core.logging import UnifiedLogger
from bioetl.core.logging import LogEvents
from bioetl.tools import get_project_root

__all__ = [
    "CLIExample",
    "extract_cli_examples",
    "run_examples",
]


PROJECT_ROOT = get_project_root()
DOCS_ROOT = PROJECT_ROOT / "docs"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"


@dataclass(frozen=True)
class CLIExample:
    """Describe a CLI example extracted from documentation."""

    source_file: Path
    line_number: int
    command: str
    expected_exit_code: int = 0


def _iter_markdown_files() -> Iterable[Path]:
    """Yield markdown files that should be scanned for CLI examples."""
    sources = [
        PROJECT_ROOT / "README.md",
        DOCS_ROOT / "cli" / "01-cli-commands.md",
        DOCS_ROOT / "pipelines" / "activity-chembl" / "00-activity-chembl-overview.md",
        DOCS_ROOT / "pipelines" / "activity-chembl" / "16-activity-chembl-cli.md",
        DOCS_ROOT / "pipelines" / "assay-chembl" / "00-assay-chembl-overview.md",
        DOCS_ROOT / "pipelines" / "assay-chembl" / "16-assay-chembl-cli.md",
    ]
    for source in sources:
        if source.exists():
            yield source


def extract_bash_commands(content: str, file_path: Path) -> list[CLIExample]:
    """Extract bash commands from markdown content."""

    examples: list[CLIExample] = []
    lines = content.split("\n")
    in_code_block = False
    code_block_lang = ""

    for index, line in enumerate(lines, start=1):
        stripped = line.strip()
        if stripped.startswith("```"):
            if not in_code_block:
                lang_match = re.match(r"```(\w+)", stripped)
                code_block_lang = lang_match.group(1) if lang_match else ""
                in_code_block = True
            else:
                in_code_block = False
                code_block_lang = ""
            continue

        if not in_code_block or code_block_lang not in ("bash", "sh", ""):
            continue

        if "python -m bioetl.cli.cli_app" not in stripped and "bioetl.cli.cli_app" not in stripped:
            continue

        cmd_lines = [stripped]
        # Collect continuation lines for multi-line commands.
        next_index = index
        while next_index < len(lines):
            candidate = lines[next_index].strip()
            if not candidate:
                break
            if candidate.endswith("\\"):
                cmd_lines.append(candidate.rstrip("\\").strip())
                next_index += 1
                continue
            if candidate.startswith("--") or candidate.startswith("-"):
                cmd_lines.append(candidate)
                next_index += 1
                continue
            break

        full_cmd = " ".join(cmd_lines).strip()
        full_cmd = re.sub(r"\\\s*$", "", full_cmd)
        full_cmd = " ".join(full_cmd.split())

        if not full_cmd or full_cmd.startswith("#") or "<" in full_cmd:
            continue

        if "--dry-run" not in full_cmd and " list" not in full_cmd:
            if "--output-dir" not in full_cmd:
                output_dir = "data/output/doctest_test"
                full_cmd += f" --output-dir {output_dir}"
            full_cmd += " --dry-run"

        examples.append(
            CLIExample(
                source_file=file_path,
                line_number=index,
                command=full_cmd,
            )
        )

    return examples


def extract_cli_examples() -> list[CLIExample]:
    """Extract all CLI examples from documentation sources."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    examples: list[CLIExample] = []

    for file_path in _iter_markdown_files():
        content = file_path.read_text(encoding="utf-8")
        extracted = extract_bash_commands(content, file_path)
        log.info(LogEvents.CLI_EXAMPLES_EXTRACTED,
            file=str(file_path.relative_to(PROJECT_ROOT)),
            count=len(extracted),
        )
        examples.extend(extracted)

    return examples


@dataclass(frozen=True)
class CLIExampleResult:
    example: CLIExample
    exit_code: int
    stdout: str
    stderr: str


def _run_command(cmd: str) -> tuple[int, str, str]:
    """Execute the provided CLI command and capture outputs."""
    parts = cmd.split()
    try:
        result = subprocess.run(
            parts,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=PROJECT_ROOT,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out after 120 seconds"
    except Exception as exc:  # noqa: BLE001
        return -1, "", str(exc)


def _write_report(results: list[CLIExampleResult]) -> Path:
    """Persist CLI doctest results as a markdown report."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = ARTIFACTS_DIR / "cli_doctest_report.md"
    tmp = report_path.with_suffix(report_path.suffix + ".tmp")

    passed = sum(1 for item in results if item.exit_code == 0)
    failed = len(results) - passed

    with tmp.open("w", encoding="utf-8") as handle:
        handle.write("# CLI Doctest Report\n\n")
        handle.write("**Purpose**: Run CLI examples from documentation with --dry-run.\n\n")
        handle.write(f"**Total examples tested**: {len(results)}\n\n")
        handle.write(f"- ✅ Passed: {passed}\n")
        handle.write(f"- ❌ Failed: {failed}\n\n")

        if failed > 0:
            handle.write("## Failed Examples\n\n")
            for item in results:
                if item.exit_code == 0:
                    continue
                handle.write(f"### {item.example.source_file.name}:{item.example.line_number}\n\n")
                handle.write(f"**Command**:\n```bash\n{item.example.command}\n```\n\n")
                handle.write(f"**Exit Code**: {item.exit_code}\n\n")
                if item.stderr:
                    handle.write(f"**Stderr**:\n```\n{item.stderr[:500]}\n```\n\n")
                if item.stdout:
                    handle.write(f"**Stdout**:\n```\n{item.stdout[:500]}\n```\n\n")

        handle.write("## All Examples\n\n")
        handle.write("| Source | Line | Command | Status |\n")
        handle.write("|--------|------|---------|--------|\n")
        for item in results:
            status = "✅ PASS" if item.exit_code == 0 else f"❌ FAIL ({item.exit_code})"
            cmd_short = (
                item.example.command[:60] + "..."
                if len(item.example.command) > 60
                else item.example.command
            )
            handle.write(
                f"| {item.example.source_file.name} | {item.example.line_number} | `{cmd_short}` | {status} |\n"
            )

    tmp.replace(report_path)
    return report_path


def run_examples(examples: list[CLIExample] | None = None) -> tuple[list[CLIExampleResult], Path]:
    """Run CLI examples and return their results alongside the report path."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    targets = examples if examples is not None else extract_cli_examples()
    log.info(LogEvents.CLI_EXAMPLES_RUNNING, count=len(targets))

    results: list[CLIExampleResult] = []
    for example in targets:
        exit_code, stdout, stderr = _run_command(example.command)
        results.append(
            CLIExampleResult(
                example=example,
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
            )
        )
        log.info(LogEvents.CLI_EXAMPLE_RESULT,
            command=example.command,
            exit_code=exit_code,
            source=str(example.source_file.relative_to(PROJECT_ROOT)),
            line=example.line_number,
        )

    report_path = _write_report(results)
    log.info(LogEvents.CLI_DOCTEST_REPORT_WRITTEN, path=str(report_path))
    return results, report_path
