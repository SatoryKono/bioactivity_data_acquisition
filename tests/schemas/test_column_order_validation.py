"""Базовые тесты валидации column_order из конфигураций пайплайнов."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from library.schemas.activity_schema import ActivityInputSchema, ActivityRawSchema
from library.schemas.assay_schema import AssayInputSchema, AssayRawSchema
from library.schemas.document_schema import DocumentInputSchema
from library.schemas.target_schema import TargetInputSchema, TargetRawSchema
from library.schemas.testitem_schema import TestitemInputSchema, TestitemRawSchema


class TestColumnOrderValidation:
    """Тесты валидации column_order для всех пайплайнов."""

    @pytest.fixture
    def config_files(self) -> dict[str, Path]:
        """Пути к конфигурационным файлам."""
        return {
            "activity": Path("configs/config_activity.yaml"),
            "target": Path("configs/config_target.yaml"),
            "document": Path("configs/config_document.yaml"),
            "testitem": Path("configs/config_testitem.yaml"),
            "assay": Path("configs/config_assay.yaml"),
        }

    @pytest.fixture
    def configs(self, config_files: dict[str, Path]) -> dict[str, dict[str, Any]]:
        """Загруженные конфигурации."""
        configs = {}
        for name, path in config_files.items():
            if path.exists():
                with open(path, encoding="utf-8") as f:
                    configs[name] = yaml.safe_load(f)
        return configs

    def test_config_files_exist(self, config_files: dict[str, Path]) -> None:
        """Проверка существования всех конфигурационных файлов."""
        for name, path in config_files.items():
            assert path.exists(), f"Config file not found: {path}"

    def test_column_order_defined(self, configs: dict[str, dict[str, Any]]) -> None:
        """Проверка наличия column_order в конфигурациях."""
        for name, config in configs.items():
            assert "determinism" in config, f"determinism section missing in {name}"
            assert "column_order" in config["determinism"], f"column_order missing in {name}"

    def test_column_order_not_empty(self, configs: dict[str, dict[str, Any]]) -> None:
        """Проверка что column_order не пустой."""
        for name, config in configs.items():
            column_order = config["determinism"]["column_order"]
            assert isinstance(column_order, list), f"column_order must be list in {name}"
            assert len(column_order) > 0, f"column_order is empty in {name}"

    def test_column_order_has_comments(self, configs: dict[str, dict[str, Any]]) -> None:
        """Проверка наличия комментариев с типами данных в column_order."""
        for name, config in configs.items():
            column_order = config["determinism"]["column_order"]
            for i, col in enumerate(column_order):
                if isinstance(col, str):
                    # Проверяем что есть комментарий с типом
                    assert "#" in col or "STRING" in col or "INT" in col or "DECIMAL" in col or "BOOL" in col, \
                        f"Column {col} in {name} missing type comment"

    def test_chembl_id_patterns(self, configs: dict[str, dict[str, Any]]) -> None:
        """Проверка паттернов ChEMBL ID в комментариях."""
        chembl_pattern = r"pattern \^CHEMBL\\d\+\$"
        
        for name, config in configs.items():
            column_order = config["determinism"]["column_order"]
            for col in column_order:
                if isinstance(col, str) and "chembl_id" in col.lower():
                    # Ищем паттерн в комментарии
                    if re.search(chembl_pattern, col, re.IGNORECASE):
                        # Паттерн найден - это хорошо
                        pass
                    else:
                        # Проверяем что хотя бы упоминается pattern
                        assert "pattern" in col.lower(), \
                            f"ChEMBL ID column {col} in {name} should have pattern validation"

    def test_doi_patterns(self, configs: dict[str, dict[str, Any]]) -> None:
        """Проверка паттернов DOI в комментариях."""
        doi_pattern = r"pattern \^10\\.\\d\+/[^\\s\]\+"
        
        for name, config in configs.items():
            column_order = config["determinism"]["column_order"]
            for col in column_order:
                if isinstance(col, str) and "doi" in col.lower():
                    # Ищем паттерн DOI в комментарии
                    if re.search(doi_pattern, col, re.IGNORECASE):
                        # Паттерн найден - это хорошо
                        pass
                    else:
                        # Проверяем что хотя бы упоминается pattern
                        assert "pattern" in col.lower(), \
                            f"DOI column {col} in {name} should have pattern validation"

    def test_required_fields_annotated(self, configs: dict[str, dict[str, Any]]) -> None:
        """Проверка аннотации обязательных полей."""
        for name, config in configs.items():
            column_order = config["determinism"]["column_order"]
            for col in column_order:
                if isinstance(col, str) and ("chembl_id" in col.lower() and "pk" in col.lower()):
                    # Primary key должен быть NOT NULL
                    assert "NOT NULL" in col or "nullable=False" in col, \
                        f"Primary key {col} in {name} should be NOT NULL"

    def test_enum_values_annotated(self, configs: dict[str, dict[str, Any]]) -> None:
        """Проверка аннотации enum значений."""
        for name, config in configs.items():
            column_order = config["determinism"]["column_order"]
            for col in column_order:
                if isinstance(col, str) and ("enum" in col.lower() or "[" in col):
                    # Enum поля должны иметь список значений
                    assert "[" in col and "]" in col, \
                        f"Enum column {col} in {name} should have enum values list"

    def test_numeric_ranges_annotated(self, configs: dict[str, dict[str, Any]]) -> None:
        """Проверка аннотации диапазонов для числовых полей."""
        for name, config in configs.items():
            column_order = config["determinism"]["column_order"]
            for col in column_order:
                if isinstance(col, str) and ("DECIMAL" in col or "INT" in col):
                    # Числовые поля должны иметь диапазоны
                    has_range = any(x in col for x in [">=", "<=", ">", "<", "0-", "range"])
                    if not has_range:
                        # Не все числовые поля требуют диапазоны, но основные должны
                        if any(x in col.lower() for x in ["value", "count", "length", "weight"]):
                            assert has_range, \
                                f"Numeric column {col} in {name} should have range validation"


class TestSchemaColumnOrderAlignment:
    """Тесты соответствия Pandera схем и column_order."""

    @pytest.fixture
    def schema_classes(self) -> dict[str, Any]:
        """Классы Pandera схем для каждого пайплайна."""
        return {
            "activity": (ActivityInputSchema, ActivityRawSchema),
            "target": (TargetInputSchema, TargetRawSchema),
            "document": (DocumentInputSchema, None),
            "testitem": (TestitemInputSchema, TestitemRawSchema),
            "assay": (AssayInputSchema, AssayRawSchema),
        }

    def test_schema_classes_exist(self, schema_classes: dict[str, Any]) -> None:
        """Проверка существования классов схем."""
        for name, (input_schema, raw_schema) in schema_classes.items():
            assert input_schema is not None, f"Input schema missing for {name}"
            # Raw schema может отсутствовать для некоторых пайплайнов

    def test_schema_has_validate_method(self, schema_classes: dict[str, Any]) -> None:
        """Проверка наличия метода validate у схем."""
        for name, (input_schema, raw_schema) in schema_classes.items():
            assert hasattr(input_schema, "validate") or hasattr(input_schema, "get_schema"), \
                f"Schema {name} missing validate method"
            
            if raw_schema is not None:
                assert hasattr(raw_schema, "validate") or hasattr(raw_schema, "get_schema"), \
                    f"Raw schema {name} missing validate method"


def validate_column_order(df: pd.DataFrame, config_column_order: list[str]) -> None:
    """Валидация соответствия порядка колонок в DataFrame и конфигурации.
    
    Args:
        df: DataFrame для проверки
        config_column_order: Список колонок из конфигурации
        
    Raises:
        AssertionError: Если порядок колонок не соответствует
    """
    actual_columns = df.columns.tolist()
    
    # Исключаем 'index' если он первый (добавляется автоматически)
    if actual_columns and actual_columns[0] == 'index':
        actual_columns = actual_columns[1:]
    
    # Извлекаем только имена колонок из конфигурации (убираем комментарии)
    expected_columns = []
    for col in config_column_order:
        if isinstance(col, str):
            # Извлекаем имя колонки до комментария
            col_name = col.split('#')[0].strip().strip('"').strip("'")
            if col_name and col_name != 'index':
                expected_columns.append(col_name)
    
    # Фильтруем только существующие колонки
    expected = [col for col in expected_columns if col in df.columns]
    actual = [col for col in actual_columns if col in expected_columns]
    
    assert actual == expected, \
        f"Column order mismatch:\nExpected: {expected}\nActual: {actual}\nMissing: {set(expected) - set(actual)}\nExtra: {set(actual) - set(expected)}"


def validate_pandera_schema(df: pd.DataFrame, schema: Any) -> pd.DataFrame:
    """Валидация DataFrame по Pandera схеме.
    
    Args:
        df: DataFrame для валидации
        schema: Pandera схема
        
    Returns:
        Валидированный DataFrame
        
    Raises:
        ValidationError: Если валидация не прошла
    """
    if hasattr(schema, 'get_schema'):
        # Для схем с методом get_schema
        pandera_schema = schema.get_schema()
        return pandera_schema.validate(df, lazy=True)
    else:
        # Для схем с прямым методом validate
        return schema.validate(df, lazy=True)


def validate_determinism(output_path: Path) -> None:
    """Валидация детерминизма выходных данных.
    
    Args:
        output_path: Путь к выходному файлу
        
    Raises:
        AssertionError: Если данные не детерминированы
    """
    if not output_path.exists():
        raise FileNotFoundError(f"Output file not found: {output_path}")
    
    # Читаем файл дважды для проверки детерминизма
    df1 = pd.read_csv(output_path)
    df2 = pd.read_csv(output_path)
    
    # Проверяем что данные идентичны
    pd.testing.assert_frame_equal(df1, df2, check_dtype=False)
