"""Утилита для очистки `type: ignore` директив из исходников."""

from __future__ import annotations

import argparse
import re
from collections.abc import Iterable
from pathlib import Path


TYPE_IGNORE_PATTERN = re.compile(r"\s+#\s*type:\s*ignore(?::[^\n]*)?")


def iter_python_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if "/.venv/" in path.as_posix():
            continue
        yield path


def cleanse_file(path: Path) -> int:
    content = path.read_text(encoding="utf-8")
    new_content, count = TYPE_IGNORE_PATTERN.subn("", content)
    if count:
        path.write_text(new_content, encoding="utf-8")
    return count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Удалить type: ignore из исходников.")
    parser.add_argument("root", nargs="?", default=Path(__file__).resolve().parents[1], type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = args.root
    total_removed = 0
    for file_path in iter_python_files(root):
        total_removed += cleanse_file(file_path)
    print(f"Removed {total_removed} type: ignore directives from {root}.")


if __name__ == "__main__":
    main()

