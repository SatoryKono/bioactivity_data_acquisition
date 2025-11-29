from __future__ import annotations

from importlib import import_module
from pathlib import Path
import sys

# Добавляем src в PYTHONPATH для запуска тестов из корня репозитория
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# Гарантируем, что используется локальный пакет bioetl из src
for name in list(sys.modules):
    if name == "bioetl" or name.startswith("bioetl."):
        del sys.modules[name]
import_module("bioetl")  # noqa: F401

