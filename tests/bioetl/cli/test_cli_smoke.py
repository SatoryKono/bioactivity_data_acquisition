from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _run_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parents[3]
    return subprocess.run(
        [sys.executable, "-m", "bioetl.cli.cli_app", *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )


def test_cli_help_subprocess() -> None:
    result = _run_cli(["--help"])
    assert result.returncode == 0, result.stderr
    assert "Commands" in result.stdout


def test_cli_list_subprocess() -> None:
    result = _run_cli(["list"])
    assert result.returncode == 0, result.stderr
    assert "activity_chembl" in result.stdout

