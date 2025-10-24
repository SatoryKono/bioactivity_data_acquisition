from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    from library.clients.session import reset_shared_session as _reset_shared_session
except ImportError:  # pragma: no cover - optional when clients are not available
    def _reset_shared_session() -> None:  # type: ignore[return-type]
        """Fallback no-op when the shared session cannot be imported."""

        return None


def pytest_configure() -> None:
    src_str = str(SRC)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)


@pytest.fixture(autouse=True)
def reset_shared_session():
    _reset_shared_session()
    yield
    _reset_shared_session()


@pytest.fixture()
def sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "assay_id": [1, 2],
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL2"],
            "standard_value": [1.5, 2.5],
            "standard_units": ["nM", "uM"],
            "activity_comment": [None, "active"],
        }
    )


# Фикстуры для валидации пайплайнов

@pytest.fixture()
def pipeline_configs() -> dict[str, dict[str, Any]]:
    """Загружает все конфигурации пайплайнов."""
    config_files = {
        "activity": "configs/config_activity.yaml",
        "target": "configs/config_target.yaml", 
        "document": "configs/config_document.yaml",
        "testitem": "configs/config_testitem.yaml",
        "assay": "configs/config_assay.yaml"
    }
    
    configs = {}
    for name, path in config_files.items():
        config_path = Path(path)
        if config_path.exists():
            with open(config_path, encoding="utf-8") as f:
                configs[name] = yaml.safe_load(f)
    
    return configs


@pytest.fixture()
def pipeline_column_orders(pipeline_configs: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    """Извлекает column_order из всех конфигураций пайплайнов."""
    column_orders = {}
    
    for name, config in pipeline_configs.items():
        if "determinism" in config and "column_order" in config["determinism"]:
            column_orders[name] = config["determinism"]["column_order"]
    
    return column_orders


@pytest.fixture()
def pipeline_input_data() -> dict[str, pd.DataFrame]:
    """Загружает входные данные для всех пайплайнов."""
    input_files = {
        "activity": "data/input/activity.csv",
        "target": "data/input/target.csv",
        "document": "data/input/document.csv", 
        "testitem": "data/input/testitem.csv",
        "assay": "data/input/assay.csv"
    }
    
    input_data = {}
    for name, path in input_files.items():
        input_path = Path(path)
        if input_path.exists():
            input_data[name] = pd.read_csv(input_path)
    
    return input_data


@pytest.fixture()
def temp_output_directory() -> Path:
    """Создает временную директорию для выходных файлов."""
    import tempfile
    return Path(tempfile.mkdtemp())


@pytest.fixture()
def activity_config(pipeline_configs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Конфигурация Activity пайплайна."""
    return pipeline_configs.get("activity", {})


@pytest.fixture()
def target_config(pipeline_configs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Конфигурация Target пайплайна."""
    return pipeline_configs.get("target", {})


@pytest.fixture()
def document_config(pipeline_configs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Конфигурация Document пайплайна."""
    return pipeline_configs.get("document", {})


@pytest.fixture()
def testitem_config(pipeline_configs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Конфигурация Testitem пайплайна."""
    return pipeline_configs.get("testitem", {})


@pytest.fixture()
def assay_config(pipeline_configs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Конфигурация Assay пайплайна."""
    return pipeline_configs.get("assay", {})


@pytest.fixture()
def activity_input_data(pipeline_input_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Входные данные для Activity пайплайна."""
    return pipeline_input_data.get("activity", pd.DataFrame())


@pytest.fixture()
def target_input_data(pipeline_input_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Входные данные для Target пайплайна."""
    return pipeline_input_data.get("target", pd.DataFrame())


@pytest.fixture()
def document_input_data(pipeline_input_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Входные данные для Document пайплайна."""
    return pipeline_input_data.get("document", pd.DataFrame())


@pytest.fixture()
def testitem_input_data(pipeline_input_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Входные данные для Testitem пайплайна."""
    return pipeline_input_data.get("testitem", pd.DataFrame())


@pytest.fixture()
def assay_input_data(pipeline_input_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Входные данные для Assay пайплайна."""
    return pipeline_input_data.get("assay", pd.DataFrame())


@pytest.fixture()
def activity_column_order(pipeline_column_orders: dict[str, list[str]]) -> list[str]:
    """Порядок колонок для Activity пайплайна."""
    return pipeline_column_orders.get("activity", [])


@pytest.fixture()
def target_column_order(pipeline_column_orders: dict[str, list[str]]) -> list[str]:
    """Порядок колонок для Target пайплайна."""
    return pipeline_column_orders.get("target", [])


@pytest.fixture()
def document_column_order(pipeline_column_orders: dict[str, list[str]]) -> list[str]:
    """Порядок колонок для Document пайплайна."""
    return pipeline_column_orders.get("document", [])


@pytest.fixture()
def testitem_column_order(pipeline_column_orders: dict[str, list[str]]) -> list[str]:
    """Порядок колонок для Testitem пайплайна."""
    return pipeline_column_orders.get("testitem", [])


@pytest.fixture()
def assay_column_order(pipeline_column_orders: dict[str, list[str]]) -> list[str]:
    """Порядок колонок для Assay пайплайна."""
    return pipeline_column_orders.get("assay", [])


# Утилиты для валидации

def parse_column_order(column_order: list[str]) -> list[str]:
    """Извлекает имена колонок из column_order конфигурации."""
    column_names = []
    for col in column_order:
        if isinstance(col, str):
            # Извлекаем имя колонки до комментария
            col_name = col.split('#')[0].strip().strip('"').strip("'")
            if col_name and col_name != 'index':
                column_names.append(col_name)
    return column_names


def validate_pipeline_output(output_file: Path, column_order: list[str], schema_class: Any) -> None:
    """Валидирует выходной файл пайплайна."""
    if not output_file.exists():
        pytest.fail(f"Output file not found: {output_file}")
    
    # Читаем данные
    output_data = pd.read_csv(output_file)
    assert len(output_data) > 0, "Output data is empty"
    
    # Проверяем column_order
    from tests.schemas.test_column_order_validation import validate_column_order
    validate_column_order(output_data, column_order)
    
    # Проверяем Pandera схему
    from tests.schemas.test_column_order_validation import validate_pandera_schema
    try:
        validated_data = validate_pandera_schema(output_data, schema_class)
        assert len(validated_data) == len(output_data)
    except Exception as e:
        pytest.fail(f"Schema validation failed: {e}")


def run_pipeline_cli(pipeline_name: str, config_file: str, input_file: str, output_dir: str, limit: int = None, dry_run: bool = False) -> subprocess.CompletedProcess:
    """Запускает пайплайн через CLI."""
    import subprocess
    
    cmd = ["bioactivity-data-acquisition"]
    
    if pipeline_name == "activity":
        cmd.extend(["get-activity-data"])
    elif pipeline_name == "target":
        cmd.extend(["get-target-data"])
    elif pipeline_name == "document":
        cmd.extend(["get-document-data"])
    elif pipeline_name == "testitem":
        cmd.extend(["testitem-run"])
    elif pipeline_name == "assay":
        # Assay может быть частью activity пайплайна
        cmd.extend(["get-activity-data"])
    else:
        raise ValueError(f"Unknown pipeline: {pipeline_name}")
    
    cmd.extend(["--config", config_file])
    
    if pipeline_name == "document":
        cmd.extend(["--documents-csv", input_file])
    else:
        cmd.extend(["--input", input_file])
    
    cmd.extend(["--output-dir", output_dir])
    
    if limit:
        cmd.extend(["--limit", str(limit)])
    
    if dry_run:
        cmd.append("--dry-run")
    
    return subprocess.run(cmd, capture_output=True, text=True, timeout=600)
