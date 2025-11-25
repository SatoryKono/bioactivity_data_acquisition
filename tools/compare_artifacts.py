from __future__ import annotations

import argparse
from pathlib import Path
import sys


def compare_directories(new_dir: Path, golden_dir: Path) -> int:
    missing: list[str] = []
    mismatched: list[str] = []

    for new_file in new_dir.glob("*"):
        golden_file = golden_dir / new_file.name
        if not golden_file.exists():
            missing.append(new_file.name)
            continue
        if new_file.read_bytes() != golden_file.read_bytes():
            mismatched.append(new_file.name)

    if missing or mismatched:
        for name in missing:
            print(f"Missing in golden: {name}")
        for name in mismatched:
            print(f"File differs: {name}")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare pipeline artifacts with golden snapshots")
    parser.add_argument("--new-dir", type=Path, required=True)
    parser.add_argument("--golden-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    return compare_directories(args.new_dir, args.golden_dir)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
