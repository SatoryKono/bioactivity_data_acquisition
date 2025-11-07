#!/usr/bin/env python3
"""Doctest CLI examples from documentation.

This script extracts CLI command examples from README.md and pipeline documentation,
runs them with --dry-run flag (to avoid side effects), and reports results.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

ROOT = Path(__file__).parent.parent
DOCS = ROOT / "docs"
AUDIT_RESULTS = ROOT / "audit_results"


class CLIExample(NamedTuple):
    """A CLI command example extracted from documentation."""

    source_file: Path
    line_number: int
    command: str
    expected_exit_code: int = 0


def extract_bash_commands(content: str, file_path: Path) -> list[CLIExample]:
    """Extract bash commands from markdown content."""
    examples: list[CLIExample] = []
    lines = content.split("\n")
    in_code_block = False
    code_block_lang = ""

    for i, line in enumerate(lines, start=1):
        # Check for code block start
        if line.strip().startswith("```"):
            if not in_code_block:
                # Starting a code block
                lang_match = re.match(r"```(\w+)", line)
                code_block_lang = lang_match.group(1) if lang_match else ""
                in_code_block = True
            else:
                # Ending a code block
                in_code_block = False
                code_block_lang = ""
            continue

        if in_code_block and code_block_lang in ("bash", "sh", ""):
            # Look for bioetl.cli.main commands
            if "python -m bioetl.cli.main" in line or "bioetl.cli.main" in line:
                # Extract the command (may span multiple lines)
                cmd_lines = [line]
                # Collect continuation lines
                j = i
                while j < len(lines) - 1:
                    next_line = lines[j]
                    if next_line.strip().endswith("\\"):
                        # Line continues on next line
                        cmd_lines.append(next_line.rstrip("\\").strip())
                        j += 1
                    elif next_line.strip().startswith("--") or next_line.strip().startswith("-"):
                        # Next line is part of the command
                        cmd_lines.append(next_line.strip())
                        j += 1
                    else:
                        # Command ends
                        break

                # Join and clean up the command
                full_cmd = " ".join(cmd_lines).strip()
                # Remove trailing backslashes and clean whitespace
                full_cmd = re.sub(r"\\\s*$", "", full_cmd)
                full_cmd = " ".join(full_cmd.split())

                # Skip if it's just a comment or placeholder
                if full_cmd and not full_cmd.startswith("#") and "<" not in full_cmd:
                    # Ensure --dry-run is present for safety
                    if "--dry-run" not in full_cmd and "list" not in full_cmd:
                        # Add --dry-run and --output-dir if missing
                        if "--output-dir" not in full_cmd:
                            # Try to infer output dir from command
                            output_dir = "data/output/doctest_test"
                            full_cmd += f" --output-dir {output_dir}"
                        full_cmd += " --dry-run"

                    examples.append(CLIExample(
                        source_file=file_path,
                        line_number=i,
                        command=full_cmd,
                    ))

    return examples


def run_command(cmd: str) -> tuple[int, str, str]:
    """Run a CLI command and return exit code, stdout, stderr."""
    try:
        # Parse command into parts
        parts = cmd.split()
        result = subprocess.run(
            parts,
            capture_output=True,
            text=True,
            timeout=120,  # 2 minute timeout
            cwd=ROOT,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out after 120 seconds"
    except Exception as e:
        return -1, "", str(e)


def main() -> int:
    """Main entry point."""
    print("Extracting CLI examples from documentation...")

    # Files to check
    files_to_check = [
        ROOT / "README.md",
        DOCS / "cli" / "01-cli-commands.md",
        DOCS / "pipelines" / "activity-chembl" / "00-activity-chembl-overview.md",
        DOCS / "pipelines" / "activity-chembl" / "16-activity-chembl-cli.md",
        DOCS / "pipelines" / "assay-chembl" / "00-assay-chembl-overview.md",
        DOCS / "pipelines" / "assay-chembl" / "16-assay-chembl-cli.md",
    ]

    all_examples: list[CLIExample] = []

    for file_path in files_to_check:
        if not file_path.exists():
            print(f"WARNING: {file_path} does not exist, skipping")
            continue

        content = file_path.read_text(encoding="utf-8")
        examples = extract_bash_commands(content, file_path)
        all_examples.extend(examples)
        print(f"  Found {len(examples)} examples in {file_path.relative_to(ROOT)}")

    print(f"\nTotal examples found: {len(all_examples)}")
    print("\nRunning examples with --dry-run...\n")

    results: list[tuple[CLIExample, int, str, str]] = []

    for example in all_examples:
        print(f"Running: {example.command[:80]}...")
        exit_code, stdout, stderr = run_command(example.command)
        results.append((example, exit_code, stdout, stderr))

        if exit_code == 0:
            print("  ✓ PASSED")
        else:
            print(f"  ✗ FAILED (exit code: {exit_code})")
            if stderr:
                print(f"    Error: {stderr[:200]}")

    # Generate report
    AUDIT_RESULTS.mkdir(exist_ok=True)
    report_path = AUDIT_RESULTS / "CLI_DOCTEST_REPORT.md"

    with report_path.open("w", encoding="utf-8") as f:
        f.write("# CLI Doctest Report\n\n")
        f.write(f"**Generated**: {Path(__file__).stat().st_mtime}\n\n")
        f.write(f"**Total examples tested**: {len(all_examples)}\n\n")

        passed = sum(1 for _, ec, _, _ in results if ec == 0)
        failed = len(results) - passed

        f.write(f"- ✅ Passed: {passed}\n")
        f.write(f"- ❌ Failed: {failed}\n\n")

        if failed > 0:
            f.write("## Failed Examples\n\n")
            for example, exit_code, stdout, stderr in results:
                if exit_code != 0:
                    f.write(f"### {example.source_file.name}:{example.line_number}\n\n")
                    f.write(f"**Command**:\n```bash\n{example.command}\n```\n\n")
                    f.write(f"**Exit Code**: {exit_code}\n\n")
                    if stderr:
                        f.write(f"**Stderr**:\n```\n{stderr[:500]}\n```\n\n")
                    if stdout:
                        f.write(f"**Stdout**:\n```\n{stdout[:500]}\n```\n\n")

        f.write("## All Examples\n\n")
        f.write("| Source | Line | Command | Status |\n")
        f.write("|--------|------|---------|--------|\n")
        for example, exit_code, stdout, stderr in results:
            status = "✅ PASS" if exit_code == 0 else f"❌ FAIL ({exit_code})"
            cmd_short = example.command[:60] + "..." if len(example.command) > 60 else example.command
            f.write(f"| {example.source_file.name} | {example.line_number} | `{cmd_short}` | {status} |\n")

    print(f"\nReport saved to {report_path}")

    # Return non-zero if any failures
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
