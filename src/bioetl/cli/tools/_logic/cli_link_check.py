"""Run documentation link checks using the lychee CLI."""

from __future__ import annotations

import subprocess
from pathlib import Path

from bioetl.core.logging import UnifiedLogger
from bioetl.core.logging import LogEvents
from bioetl.tools import get_project_root

__all__ = ["run_link_check"]


PROJECT_ROOT = get_project_root()
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"


def _write_stub_report(output_file: Path) -> None:
    """Write a placeholder report when lychee is unavailable."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_file.with_suffix(output_file.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write("# Link Check Report\n\n")
        handle.write("## Status\n\n")
        handle.write(
            "**Warning:** lychee is not installed. Please install it to run link checks.\n\n"
        )
        handle.write("```bash\n")
        handle.write("# Install lychee:\n")
        handle.write("cargo install lychee\n")
        handle.write("# or\n")
        handle.write("brew install lychee\n")
        handle.write("```\n")
    tmp.replace(output_file)


def run_link_check(timeout_seconds: int = 300) -> int:
    """Execute lychee and return its exit code."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    output_file = ARTIFACTS_DIR / "link-check-report.md"
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    config_file = PROJECT_ROOT / ".lychee.toml"

    log.info(LogEvents.LINK_CHECK_START, config=str(config_file))

    try:
        result = subprocess.run(
            ["lychee", "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            _write_stub_report(output_file)
            log.warning(LogEvents.LYCHEE_NOT_AVAILABLE)
            return 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        _write_stub_report(output_file)
        log.warning(LogEvents.LYCHEE_NOT_AVAILABLE)
        return 0

    try:
        result = subprocess.run(
            ["lychee", "--config", str(config_file), "--output", str(output_file)],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        log.info(LogEvents.LYCHEE_FINISHED,
            returncode=result.returncode,
            stdout=result.stdout[:2000],
            stderr=result.stderr[:2000],
        )
        return result.returncode
    except FileNotFoundError:
        _write_stub_report(output_file)
        log.warning(LogEvents.LYCHEE_NOT_FOUND)
        return 0
    except subprocess.TimeoutExpired:
        log.error(LogEvents.LYCHEE_TIMEOUT, timeout_seconds=timeout_seconds)
        return 1
