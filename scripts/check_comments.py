"""Заготовка для проверки комментариев в кодовой базе."""

from __future__ import annotations

import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Проверка комментариев и TODO в репозитории.")
    parser.add_argument("root", nargs="?", default=Path(__file__).resolve().parents[1], type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = args.root
    print(f"TODO: реализовать проверку комментариев для {root}.")


if __name__ == "__main__":
    main()

