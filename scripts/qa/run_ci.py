#!/usr/bin/env python3
"""Run repository quality gates locally or in CI."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[2]

COMMAND_GROUPS: Dict[str, List[List[str]]] = {
    "lint": [
        [sys.executable, "scripts/qa/check_docs.py"],
        [sys.executable, "scripts/qa/check_module_names.py"],
        [sys.executable, "-m", "compileall", "src", "tests", "scripts"],
    ],
    "tests": [
        ["pytest", "--no-cov", "tests/healthcheck"],
    ],
    "docs": [
        [sys.executable, "scripts/link_check.py"],
    ],
}
COMMAND_GROUPS["all"] = COMMAND_GROUPS["lint"] + COMMAND_GROUPS["tests"]


def run_commands(commands: List[List[str]]) -> int:
    for command in commands:
        print(f"→ {' '.join(command)}")
        result = subprocess.run(command, cwd=ROOT)
        if result.returncode != 0:
            return result.returncode
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage",
        choices=sorted(COMMAND_GROUPS.keys()),
        default="all",
        help="Which stage to run (default: all).",
    )
    args = parser.parse_args()

    commands = COMMAND_GROUPS[args.stage]
    return run_commands(commands)


if __name__ == "__main__":
    sys.exit(main())
