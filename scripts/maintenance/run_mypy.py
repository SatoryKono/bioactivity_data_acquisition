#!/usr/bin/env python3
"""Скрипт для запуска mypy с правильными настройками."""

import subprocess
import sys
from pathlib import Path


def main():
    """Запустить mypy с правильными настройками."""
    # Получаем корневую директорию проекта
    project_root = Path(__file__).parent.parent
    src_dir = project_root / "src"
    
    # Проверяем, что мы находимся в правильной директории
    if not src_dir.exists():
        print("Ошибка: директория src не найдена", file=sys.stderr)
        sys.exit(1)
    
    # Строим команду mypy
    cmd = [
        sys.executable, "-m", "mypy",
        "--explicit-package-bases",
        "--config-file", str(project_root / "mypy.ini"),
    ]
    
    # Добавляем аргументы командной строки или проверяем всю библиотеку по умолчанию
    if len(sys.argv) > 1:
        cmd.extend(sys.argv[1:])
    else:
        cmd.append("library/")
    
    # Запускаем mypy из директории src
    try:
        result = subprocess.run(cmd, cwd=src_dir, check=False)
        sys.exit(result.returncode)
    except KeyboardInterrupt:
        print("\nПрервано пользователем", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка запуска mypy: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
