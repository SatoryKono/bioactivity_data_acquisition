from __future__ import annotations

import os
from importlib import import_module
from pathlib import Path
import sys

import pytest

from tests.utils.determinism import enforce_determinism

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


@pytest.fixture(scope="session", autouse=True)
def _setup_determinism() -> None:
    """Автоматически настраивает детерминизм для всех тестов."""
    enforce_determinism(seed=0)


@pytest.fixture
def project_root() -> Path:
    """Корневая директория проекта."""
    return PROJECT_ROOT


@pytest.fixture
def configs_dir(project_root: Path) -> Path:
    """Директория с конфигурационными файлами."""
    return project_root / "configs"


@pytest.fixture
def golden_dir(tmp_path: Path) -> Path:
    """Директория для golden тестовых файлов."""
    golden = tmp_path / "golden"
    golden.mkdir(parents=True, exist_ok=True)
    return golden


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    """Директория для выходных файлов тестов."""
    output = tmp_path / "output"
    output.mkdir(parents=True, exist_ok=True)
    return output


@pytest.fixture
def test_data_dir(project_root: Path) -> Path:
    """Директория с тестовыми данными."""
    return project_root / "data" / "input"

