#!/usr/bin/env python3
"""Инвентаризация документации: сбор списка файлов и хешей."""

import hashlib
from pathlib import Path

ROOT = Path(__file__).parent.parent
DOCS = ROOT / "docs"
AUDIT_RESULTS = ROOT / "audit_results"
AUDIT_RESULTS.mkdir(exist_ok=True)


def get_file_hash(filepath: Path) -> str:
    """Вычисляет SHA256 хеш файла."""
    sha256 = hashlib.sha256()
    with filepath.open("rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def main():
    """Основная функция инвентаризации."""
    # Сбор списка всех .md файлов
    md_files = sorted(DOCS.rglob("*.md"))
    
    # Запись списка файлов
    inventory_file = AUDIT_RESULTS / "docs_inventory.txt"
    with inventory_file.open("w", encoding="utf-8") as f:
        for filepath in md_files:
            f.write(f"{filepath.relative_to(ROOT)}\n")
    
    print(f"Found {len(md_files)} markdown files")
    print(f"Inventory saved to {inventory_file}")
    
    # Вычисление хешей
    hashes_file = AUDIT_RESULTS / "docs_hashes.txt"
    with hashes_file.open("w", encoding="utf-8") as f:
        for filepath in md_files:
            file_hash = get_file_hash(filepath)
            f.write(f"{file_hash}  {filepath.relative_to(ROOT)}\n")
    
    print(f"Hashes saved to {hashes_file}")


if __name__ == "__main__":
    main()

