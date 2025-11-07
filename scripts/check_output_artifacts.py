#!/usr/bin/env python3
"""Fail fast if large artifacts remain in the repository's data/output directory."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

MAX_BYTES = 1_000_000  # 1 MB threshold for local working tree files.
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


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    output_dir = repo_root / "data" / "output"

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
            if size > MAX_BYTES:
                oversized.append((file_path.relative_to(repo_root), size))

    if oversized:
        formatted = "\n".join(
            f"  - {path} ({size / 1_000_000:.2f} MB)" for path, size in oversized
        )
        errors.append(
            "Large files found in data/output. Upload them to the external bucket and keep only small samples locally:\n"
            f"{formatted}"
        )

    if errors:
        sys.stderr.write("\n".join(errors) + "\n")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
