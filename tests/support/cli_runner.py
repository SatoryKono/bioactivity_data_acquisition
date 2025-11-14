"""Helpers for invoking the BioETL CLI in tests."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _build_env(extra_env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Builder for CLI environment with project ``src`` on ``PYTHONPATH``."""

    env = dict(os.environ)
    src_dir = PROJECT_ROOT / "src"
    pythonpath_entries: list[str] = [str(src_dir)]
    existing_pythonpath = env.get("PYTHONPATH")
    if existing_pythonpath:
        pythonpath_entries.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)
    if extra_env:
        env.update(extra_env)
    return env


def run_cli_command(
    args: Sequence[str],
    *,
    cwd: Path | None = None,
    timeout: float = 60.0,
    extra_env: Mapping[str, str] | None = None,
    capture_output: bool = True,
    text: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Execute the BioETL CLI with the provided arguments."""
    env = _build_env(extra_env)
    command: list[str] = [sys.executable, "-m", "bioetl.cli.cli_app", *args]
    return subprocess.run(
        command,
        cwd=cwd or PROJECT_ROOT,
        env=env,
        capture_output=capture_output,
        text=text,
        timeout=timeout,
        check=False,
    )


def run_cli_script(
    args: Sequence[str],
    *,
    cwd: Path | None = None,
    timeout: float = 60.0,
    extra_env: Mapping[str, str] | None = None,
    capture_output: bool = True,
    text: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Execute the installed BioETL console script."""
    env = _build_env(extra_env)
    executable = env.get("BIOETL_CLI_EXECUTABLE")
    if executable:
        command: list[str] = [executable, *args]
    else:
        resolved = shutil.which("bioetl", path=env.get("PATH"))
        if resolved:
            command = [resolved, *args]
        else:
            command = [sys.executable, "-m", "bioetl.cli.cli_app", *args]
    return subprocess.run(
        command,
        cwd=cwd or PROJECT_ROOT,
        env=env,
        capture_output=capture_output,
        text=text,
        timeout=timeout,
        check=False,
    )
