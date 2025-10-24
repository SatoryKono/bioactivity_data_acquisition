"""Модульные тесты валидации схем для Activity пайплайна."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from library.schemas.activity_schema import ActivityInputSchema, ActivityRawSchema
from tests.schemas.test_column_order_validation import validate_column_order, validate_pandera_schema


class TestActivitySchemaValidation:
    """Тесты валидации Activity схем."""

    @pytest.fixture
    def activity_config(self) -> dict[str, Any]:
        """Конфигурация Activity пайплайна."""
        config_path = Path("configs/config_activity.yaml")
        if not config_path.exists():
            pytest.skip("Activity config file not found")
        
        with open(config_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def activity_column_order(self, activity_config: dict[str, Any]) -> list[str]:
        """Порядок колонок из конфигурации Activity."""
        return activity_config["determinism"]["column_order"]

    @pytest.fixture
    def valid_activity_data(self) -> pd.DataFrame:
        """Валидные тестовые данные для Activity."""
        return pd.DataFrame({
            "activity_chembl_id": ["CHEMBL123456", "CHEMBL789012"],
            "assay_chembl_id": ["CHEMBL123", "CHEMBL456"],
            "document_chembl_id": ["CHEMBL101", "CHEMBL102"],
            "target_chembl_id": ["CHEMBL789", "CHEMBL012"],
            "molecule_chembl_id": ["CHEMBL345", "CHEMBL678"],
            "activity_type": ["IC50", "Ki"],
            "activity_value": [10.5, 25.0],
            "activity_unit": ["nM", "nM"],
            "pchembl_value": [8.0, 7.6],
            "data_validity_comment": [None, "Good data"],
            "activity_comment": [None, "High confidence"],
            "lower_bound": [None, 20.0],
            "upper_bound": [None, 30.0],
            "standard_value": [10.5, 25.0],
            "standard_relation": ["=", "="],
            "standard_units": ["nM", "nM"],
            "standard_type": ["IC50", "Ki"],
            "data_validity_comment": [None, "Good data"],
            "activity_comment": [None, "High confidence"],
            "source_system": ["ChEMBL", "ChEMBL"],
            "extracted_at": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
            "chembl_release": ["33.0", "33.0"]
        })

    @pytest.fixture
    def invalid_activity_data(self) -> pd.DataFrame:
        """Невалидные тестовые данные для Activity."""
        return pd.DataFrame({
            "activity_chembl_id": ["INVALID_ID", "CHEMBL789012"],
            "assay_chembl_id": ["CHEMBL123", "INVALID"],
            "activity_type": ["INVALID_TYPE", "Ki"],
            "activity_value": [-10.5, 25.0],  # Отрицательное значение
            "activity_unit": ["invalid_unit", "nM"],
            "pchembl_value": [15.0, 7.6],  # Вне диапазона 3.0-12.0
        })

    def test_activity_input_schema_validation(self, valid_activity_data: pd.DataFrame) -> None:
        """Тест валидации ActivityInputSchema с валидными данными."""
        try:
            validated = validate_pandera_schema(valid_activity_data, ActivityInputSchema)
            assert len(validated) == len(valid_activity_data)
        except Exception as e:
            pytest.fail(f"ActivityInputSchema validation failed with valid data: {e}")

    def test_activity_raw_schema_validation(self, valid_activity_data: pd.DataFrame) -> None:
        """Тест валидации ActivityRawSchema с валидными данными."""
        try:
            validated = validate_pandera_schema(valid_activity_data, ActivityRawSchema)
            assert len(validated) == len(valid_activity_data)
        except Exception as e:
            pytest.fail(f"ActivityRawSchema validation failed with valid data: {e}")

    def test_activity_schema_invalid_data(self, invalid_activity_data: pd.DataFrame) -> None:
        """Тест что схемы отклоняют невалидные данные."""
        with pytest.raises(Exception):  # ValidationError или подобное
            validate_pandera_schema(invalid_activity_data, ActivityInputSchema)

    def test_activity_column_order_validation(self, valid_activity_data: pd.DataFrame, activity_column_order: list[str]) -> None:
        """Тест соответствия порядка колонок в Activity данных."""
        # Создаем DataFrame с правильным порядком колонок
        ordered_data = valid_activity_data.copy()
        
        # Извлекаем имена колонок из конфигурации
        expected_columns = []
        for col in activity_column_order:
            if isinstance(col, str):
                col_name = col.split('#')[0].strip().strip('"').strip("'")
                if col_name and col_name in ordered_data.columns:
                    expected_columns.append(col_name)
        
        # Переупорядочиваем колонки
        ordered_data = ordered_data[expected_columns]
        
        # Валидируем порядок
        validate_column_order(ordered_data, activity_column_order)

    def test_activity_chembl_id_patterns(self, valid_activity_data: pd.DataFrame) -> None:
        """Тест паттернов ChEMBL ID в Activity данных."""
        chembl_pattern = re.compile(r'^CHEMBL\d+$')
        
        for col in valid_activity_data.columns:
            if 'chembl_id' in col.lower():
                for value in valid_activity_data[col].dropna():
                    assert chembl_pattern.match(str(value)), \
                        f"Invalid ChEMBL ID pattern in {col}: {value}"

    def test_activity_enum_values(self, valid_activity_data: pd.DataFrame) -> None:
        """Тест enum значений в Activity данных."""
        # Проверяем activity_type
        valid_types = ["IC50", "EC50", "Ki", "Kd", "AC50"]
        for value in valid_activity_data["activity_type"].dropna():
            assert value in valid_types, f"Invalid activity_type: {value}"
        
        # Проверяем activity_unit
        valid_units = ["nM", "uM", "mM", "M", "%", "mg/ml", "ug/ml"]
        for value in valid_activity_data["activity_unit"].dropna():
            assert value in valid_units, f"Invalid activity_unit: {value}"

    def test_activity_numeric_ranges(self, valid_activity_data: pd.DataFrame) -> None:
        """Тест диапазонов числовых значений в Activity данных."""
        # Проверяем activity_value >= 0
        for value in valid_activity_data["activity_value"].dropna():
            assert value >= 0, f"activity_value should be >= 0, got {value}"
        
        # Проверяем pchembl_value в диапазоне 3.0-12.0
        for value in valid_activity_data["pchembl_value"].dropna():
            assert 3.0 <= value <= 12.0, f"pchembl_value should be 3.0-12.0, got {value}"

    def test_activity_required_fields(self, valid_activity_data: pd.DataFrame) -> None:
        """Тест обязательных полей в Activity данных."""
        required_fields = [
            "activity_chembl_id",
            "assay_chembl_id", 
            "document_chembl_id",
            "target_chembl_id",
            "molecule_chembl_id"
        ]
        
        for field in required_fields:
            assert field in valid_activity_data.columns, f"Required field {field} missing"
            # Проверяем что нет NULL значений в обязательных полях
            assert not valid_activity_data[field].isna().any(), \
                f"Required field {field} contains NULL values"

    def test_activity_data_types(self, valid_activity_data: pd.DataFrame) -> None:
        """Тест типов данных в Activity."""
        # STRING поля
        string_fields = ["activity_chembl_id", "assay_chembl_id", "activity_type", "activity_unit"]
        for field in string_fields:
            if field in valid_activity_data.columns:
                assert valid_activity_data[field].dtype == 'object', \
                    f"Field {field} should be string/object type"
        
        # DECIMAL поля
        decimal_fields = ["activity_value", "pchembl_value", "lower_bound", "upper_bound"]
        for field in decimal_fields:
            if field in valid_activity_data.columns:
                assert pd.api.types.is_numeric_dtype(valid_activity_data[field]), \
                    f"Field {field} should be numeric type"

    def test_activity_config_column_order_structure(self, activity_column_order: list[str]) -> None:
        """Тест структуры column_order в конфигурации Activity."""
        assert isinstance(activity_column_order, list), "column_order should be a list"
        assert len(activity_column_order) > 0, "column_order should not be empty"
        
        # Проверяем что есть основные поля
        column_names = []
        for col in activity_column_order:
            if isinstance(col, str):
                col_name = col.split('#')[0].strip().strip('"').strip("'")
                column_names.append(col_name)
        
        required_columns = [
            "activity_chembl_id",
            "assay_chembl_id",
            "document_chembl_id", 
            "target_chembl_id",
            "molecule_chembl_id"
        ]
        
        for required in required_columns:
            assert required in column_names, f"Required column {required} missing from column_order"

    def test_activity_config_validation_rules(self, activity_config: dict[str, Any]) -> None:
        """Тест правил валидации в конфигурации Activity."""
        column_order = activity_config["determinism"]["column_order"]
        
        # Проверяем что есть комментарии с типами
        for col in column_order:
            if isinstance(col, str):
                # Проверяем наличие типов в комментариях
                has_type = any(x in col for x in ["STRING", "INT", "DECIMAL", "BOOL", "TEXT"])
                assert has_type, f"Column {col} missing type annotation"
                
                # Проверяем валидационные правила
                if "chembl_id" in col.lower():
                    assert "NOT NULL" in col or "nullable=False" in col, \
                        f"ChEMBL ID column {col} should be NOT NULL"
                
                if "activity_type" in col.lower():
                    assert "enum" in col.lower() or "[" in col, \
                        f"activity_type column {col} should have enum values"
