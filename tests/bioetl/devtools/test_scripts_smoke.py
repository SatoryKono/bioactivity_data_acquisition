from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _run_script(script: str, *extra: str) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / f"{script}.py"
    return subprocess.run(
        [sys.executable, str(script_path), *extra],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )


def test_determinism_check_help() -> None:
    result = _run_script("determinism_check", "--help")
    assert result.returncode == 0, result.stderr
    assert "determinism" in result.stdout.lower()

