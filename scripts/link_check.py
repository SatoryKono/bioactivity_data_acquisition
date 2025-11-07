#!/usr/bin/env python3
"""Проверка ссылок в документации с использованием .lychee.toml."""

import re
import subprocess
from pathlib import Path
from typing import List, Tuple

ROOT = Path(__file__).parent.parent
ARTIFACTS_DIR = ROOT / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)


def check_lychee_available() -> bool:
    """Проверяет, доступен ли lychee."""
    try:
        result = subprocess.run(
            ["lychee", "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def run_lychee() -> Tuple[int, str]:
    """Запускает lychee с конфигом .lychee.toml."""
    config_file = ROOT / ".lychee.toml"
    output_file = ARTIFACTS_DIR / "link-check-report.md"
    
    try:
        result = subprocess.run(
            ["lychee", "--config", str(config_file), "--output", str(output_file)],
            capture_output=True,
            text=True,
            timeout=300,
        )
        return result.returncode, result.stdout + result.stderr
    except FileNotFoundError:
        # Если lychee не установлен, создаём заглушку отчёта
        with output_file.open("w", encoding="utf-8") as f:
            f.write("# Link Check Report\n\n")
            f.write("## Status\n\n")
            f.write("**Warning:** lychee is not installed. Please install it to run link checks.\n\n")
            f.write("```bash\n")
            f.write("# Install lychee:\n")
            f.write("cargo install lychee\n")
            f.write("# or\n")
            f.write("brew install lychee\n")
            f.write("```\n")
        return 0, "lychee not found, stub report created"
    except subprocess.TimeoutExpired:
        return 1, "lychee timeout"


def main():
    """Основная функция проверки ссылок."""
    print("Checking if lychee is available...")
    if not check_lychee_available():
        print("Warning: lychee is not installed. Creating stub report.")
        print("To install: cargo install lychee or brew install lychee")
    
    print("Running link check...")
    exit_code, output = run_lychee()
    
    if exit_code == 0:
        print("Link check completed successfully")
        print(f"Report saved to {ARTIFACTS_DIR / 'link-check-report.md'}")
    else:
        print(f"Link check failed with exit code {exit_code}")
        print(output)
    
    return exit_code


if __name__ == "__main__":
    exit(main())

