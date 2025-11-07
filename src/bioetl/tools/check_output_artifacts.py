"""Проверка артефактов в каталоге data/output."""

from __future__ import annotations

import subprocess
from pathlib import Path

from bioetl.core.logger import UnifiedLogger
from bioetl.tools import get_project_root

__all__ = ["MAX_BYTES", "check_output_artifacts"]


MAX_BYTES = 1_000_000
IGNORED_NAMES = {".gitkeep"}


def _git_ls_files(path: str) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", path],
        check=True,
        capture_output=True,
        text=True,
    )
    files = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return [Path(line) for line in files]


def _git_diff_cached(path: str) -> list[Path]:
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=AM", "--", path],
        check=True,
        capture_output=True,
        text=True,
    )
    files = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return [Path(line) for line in files]


def check_output_artifacts(max_bytes: int = MAX_BYTES) -> list[str]:
    """Возвращает список ошибок, связанных с артефактами в data/output."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    repo_root = get_project_root()
    output_dir = repo_root / "data" / "output"

    log.info("checking_output_artifacts", output_dir=str(output_dir), max_bytes=max_bytes)

    tracked = [p for p in _git_ls_files("data/output") if p.name not in IGNORED_NAMES]
    staged = [p for p in _git_diff_cached("data/output") if p.name not in IGNORED_NAMES]

    errors: list[str] = []

    if tracked:
        tracked_list = "\n".join(f"  - {path}" for path in tracked)
        errors.append(
            "Tracked artifacts detected under data/output. Move long-term datasets to external storage:\n"
            f"{tracked_list}"
        )

    if staged:
        staged_list = "\n".join(f"  - {path}" for path in staged)
        errors.append(
            "New artifacts staged under data/output. Commit lightweight samples under data/samples instead:\n"
            f"{staged_list}"
        )

    oversized: list[tuple[Path, int]] = []
    if output_dir.exists():
        for file_path in output_dir.rglob("*"):
            if not file_path.is_file() or file_path.name in IGNORED_NAMES:
                continue
            size = file_path.stat().st_size
            if size > max_bytes:
                oversized.append((file_path.relative_to(repo_root), size))

    if oversized:
        formatted = "\n".join(f"  - {path} ({size / 1_000_000:.2f} MB)" for path, size in oversized)
        errors.append(
            "Large files found in data/output. Upload them to the external bucket and keep only small samples locally:\n"
            f"{formatted}"
        )

    log.info("output_artifact_check_completed", issues=len(errors))
    return errors
