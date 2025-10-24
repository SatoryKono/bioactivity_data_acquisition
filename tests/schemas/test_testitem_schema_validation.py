"""Модульные тесты валидации схем для Testitem пайплайна."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from library.schemas.testitem_schema import TestitemInputSchema, TestitemRawSchema
from tests.schemas.test_column_order_validation import validate_column_order, validate_pandera_schema


class TestTestitemSchemaValidation:
    """Тесты валидации Testitem схем."""

    @pytest.fixture
    def testitem_config(self) -> dict[str, Any]:
        """Конфигурация Testitem пайплайна."""
        config_path = Path("configs/config_testitem.yaml")
        if not config_path.exists():
            pytest.skip("Testitem config file not found")
        
        with open(config_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def testitem_column_order(self, testitem_config: dict[str, Any]) -> list[str]:
        """Порядок колонок из конфигурации Testitem."""
        return testitem_config["determinism"]["column_order"]

    @pytest.fixture
    def valid_testitem_data(self) -> pd.DataFrame:
        """Валидные тестовые данные для Testitem."""
        return pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL25", "CHEMBL153"],
            "molregno": [1, 2],
            "pref_name": ["Aspirin", "Ibuprofen"],
            "pref_name_key": ["aspirin", "ibuprofen"],
            "parent_chembl_id": ["CHEMBL25", "CHEMBL153"],
            "parent_molregno": [1, 2],
            "max_phase": [4.0, 4.0],
            "therapeutic_flag": [True, True],
            "dosed_ingredient": [True, True],
            "first_approval": ["1899-01-01", "1969-01-01"],
            "structure_type": ["MOLECULE", "MOLECULE"],
            "molecule_type": ["Small molecule", "Small molecule"],
            "mw_freebase": [180.16, 206.28],
            "alogp": [1.19, 3.97],
            "hba": [4, 1],
            "hbd": [1, 1],
            "psa": [63.60, 37.30],
            "rtb": [3, 1],
            "ro3_pass": [True, True],
            "num_ro5_violations": [0, 0],
            "acd_most_apka": [3.49, 4.91],
            "acd_most_bpka": [None, None],
            "acd_logp": [1.19, 3.97],
            "acd_logd": [1.19, 3.97],
            "molecular_species": ["NEUTRAL", "NEUTRAL"],
            "full_mwt": [180.16, 206.28],
            "aromatic_rings": [1, 1],
            "heavy_atoms": [13, 15],
            "qed_weighted": [0.67, 0.58],
            "mw_monoisotopic": [180.042, 206.130],
            "full_molformula": ["C9H8O4", "C13H18O2"],
            "hba_lipinski": [4, 1],
            "hbd_lipinski": [1, 1],
            "num_lipinski_ro5_violations": [0, 0],
            "oral": [True, True],
            "parenteral": [True, True],
            "topical": [False, False],
            "black_box_warning": [False, False],
            "natural_product": [False, False],
            "first_in_class": [True, False],
            "chirality": [0, 0],
            "prodrug": [False, False],
            "inorganic_flag": [False, False],
            "polymer_flag": [False, False],
            "usan_year": [1899, 1969],
            "availability_type": ["Prescription", "OTC"],
            "usan_stem": ["-profen", "-profen"],
            "usan_substem": ["ibu", "ibu"],
            "usan_stem_definition": ["Anti-inflammatory", "Anti-inflammatory"],
            "indication_class": ["Analgesic", "Analgesic"],
            "withdrawn_flag": [False, False],
            "withdrawn_year": [None, None],
            "withdrawn_country": [None, None],
            "withdrawn_reason": [None, None],
            "mechanism_of_action": ["COX inhibitor", "COX inhibitor"],
            "direct_interaction": [True, True],
            "molecular_mechanism": ["Enzyme inhibition", "Enzyme inhibition"],
            "drug_chembl_id": ["CHEMBL25", "CHEMBL153"],
            "drug_name": ["Aspirin", "Ibuprofen"],
            "drug_type": ["Small molecule", "Small molecule"],
            "drug_substance_flag": [True, True],
            "source_system": ["ChEMBL", "ChEMBL"],
            "extracted_at": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
            "chembl_release": ["33.0", "33.0"]
        })

    @pytest.fixture
    def invalid_testitem_data(self) -> pd.DataFrame:
        """Невалидные тестовые данные для Testitem."""
        return pd.DataFrame({
            "molecule_chembl_id": ["INVALID_ID", "CHEMBL153"],
            "molregno": [-1, 2],  # Отрицательный molregno
            "max_phase": [5.0, 4.0],  # Вне диапазона 0-4
            "mw_freebase": [-100.0, 206.28],  # Отрицательная молекулярная масса
            "hba": [-1, 1],  # Отрицательное количество HBA
            "hbd": [-1, 1],  # Отрицательное количество HBD
            "num_ro5_violations": [6, 0],  # Вне диапазона 0-5
            "chirality": [2, 0],  # Вне диапазона -1, 0, 1
        })

    def test_testitem_input_schema_validation(self, valid_testitem_data: pd.DataFrame) -> None:
        """Тест валидации TestitemInputSchema с валидными данными."""
        try:
            validated = validate_pandera_schema(valid_testitem_data, TestitemInputSchema)
            assert len(validated) == len(valid_testitem_data)
        except Exception as e:
            pytest.fail(f"TestitemInputSchema validation failed with valid data: {e}")

    def test_testitem_raw_schema_validation(self, valid_testitem_data: pd.DataFrame) -> None:
        """Тест валидации TestitemRawSchema с валидными данными."""
        try:
            validated = validate_pandera_schema(valid_testitem_data, TestitemRawSchema)
            assert len(validated) == len(valid_testitem_data)
        except Exception as e:
            pytest.fail(f"TestitemRawSchema validation failed with valid data: {e}")

    def test_testitem_schema_invalid_data(self, invalid_testitem_data: pd.DataFrame) -> None:
        """Тест что схемы отклоняют невалидные данные."""
        with pytest.raises(Exception):  # ValidationError или подобное
            validate_pandera_schema(invalid_testitem_data, TestitemInputSchema)

    def test_testitem_column_order_validation(self, valid_testitem_data: pd.DataFrame, testitem_column_order: list[str]) -> None:
        """Тест соответствия порядка колонок в Testitem данных."""
        # Создаем DataFrame с правильным порядком колонок
        ordered_data = valid_testitem_data.copy()
        
        # Извлекаем имена колонок из конфигурации
        expected_columns = []
        for col in testitem_column_order:
            if isinstance(col, str):
                col_name = col.split('#')[0].strip().strip('"').strip("'")
                if col_name and col_name in ordered_data.columns:
                    expected_columns.append(col_name)
        
        # Переупорядочиваем колонки
        ordered_data = ordered_data[expected_columns]
        
        # Валидируем порядок
        validate_column_order(ordered_data, testitem_column_order)

    def test_testitem_chembl_id_patterns(self, valid_testitem_data: pd.DataFrame) -> None:
        """Тест паттернов ChEMBL ID в Testitem данных."""
        chembl_pattern = re.compile(r'^CHEMBL\d+$')
        
        for col in valid_testitem_data.columns:
            if 'chembl_id' in col.lower():
                for value in valid_testitem_data[col].dropna():
                    assert chembl_pattern.match(str(value)), \
                        f"Invalid ChEMBL ID pattern in {col}: {value}"

    def test_testitem_numeric_ranges(self, valid_testitem_data: pd.DataFrame) -> None:
        """Тест диапазонов числовых значений в Testitem данных."""
        # Проверяем molregno > 0
        for value in valid_testitem_data["molregno"].dropna():
            assert value > 0, f"molregno should be > 0, got {value}"
        
        # Проверяем max_phase в диапазоне 0-4
        for value in valid_testitem_data["max_phase"].dropna():
            assert 0 <= value <= 4, f"max_phase should be 0-4, got {value}"
        
        # Проверяем mw_freebase в диапазоне 50-2000
        for value in valid_testitem_data["mw_freebase"].dropna():
            assert 50 <= value <= 2000, f"mw_freebase should be 50-2000, got {value}"
        
        # Проверяем hba >= 0
        for value in valid_testitem_data["hba"].dropna():
            assert value >= 0, f"hba should be >= 0, got {value}"
        
        # Проверяем hbd >= 0
        for value in valid_testitem_data["hbd"].dropna():
            assert value >= 0, f"hbd should be >= 0, got {value}"
        
        # Проверяем psa >= 0
        for value in valid_testitem_data["psa"].dropna():
            assert value >= 0, f"psa should be >= 0, got {value}"
        
        # Проверяем rtb >= 0
        for value in valid_testitem_data["rtb"].dropna():
            assert value >= 0, f"rtb should be >= 0, got {value}"
        
        # Проверяем num_ro5_violations в диапазоне 0-5
        for value in valid_testitem_data["num_ro5_violations"].dropna():
            assert 0 <= value <= 5, f"num_ro5_violations should be 0-5, got {value}"
        
        # Проверяем qed_weighted в диапазоне 0-1
        for value in valid_testitem_data["qed_weighted"].dropna():
            assert 0 <= value <= 1, f"qed_weighted should be 0-1, got {value}"

    def test_testitem_boolean_fields(self, valid_testitem_data: pd.DataFrame) -> None:
        """Тест boolean полей в Testitem данных."""
        boolean_fields = [
            "therapeutic_flag", "dosed_ingredient", "ro3_pass", "oral", 
            "parenteral", "topical", "black_box_warning", "natural_product",
            "first_in_class", "prodrug", "inorganic_flag", "polymer_flag",
            "withdrawn_flag", "direct_interaction", "drug_substance_flag"
        ]
        
        for field in boolean_fields:
            if field in valid_testitem_data.columns:
                for value in valid_testitem_data[field].dropna():
                    assert isinstance(value, bool), \
                        f"Field {field} should be boolean, got {type(value)}"

    def test_testitem_enum_values(self, valid_testitem_data: pd.DataFrame) -> None:
        """Тест enum значений в Testitem данных."""
        # Проверяем structure_type
        valid_structure_types = ["MOLECULE", "UNKNOWN", "INORGANIC", "POLYMER"]
        for value in valid_testitem_data["structure_type"].dropna():
            assert value in valid_structure_types, f"Invalid structure_type: {value}"
        
        # Проверяем molecule_type
        valid_molecule_types = ["Small molecule", "Protein", "Nucleic acid", "Unknown"]
        for value in valid_testitem_data["molecule_type"].dropna():
            assert value in valid_molecule_types, f"Invalid molecule_type: {value}"
        
        # Проверяем molecular_species
        valid_species = ["NEUTRAL", "ACID", "BASE", "ZWITTERION"]
        for value in valid_testitem_data["molecular_species"].dropna():
            assert value in valid_species, f"Invalid molecular_species: {value}"
        
        # Проверяем chirality
        valid_chirality = [-1, 0, 1]
        for value in valid_testitem_data["chirality"].dropna():
            assert value in valid_chirality, f"Invalid chirality: {value}"

    def test_testitem_date_formats(self, valid_testitem_data: pd.DataFrame) -> None:
        """Тест форматов дат в Testitem данных."""
        # Проверяем first_approval в формате ISO 8601
        for value in valid_testitem_data["first_approval"].dropna():
            assert re.match(r'^\d{4}-\d{2}-\d{2}$', str(value)), \
                f"first_approval should be in YYYY-MM-DD format: {value}"
        
        # Проверяем usan_year в диапазоне 1900-текущий год
        current_year = 2024
        for value in valid_testitem_data["usan_year"].dropna():
            assert 1900 <= value <= current_year, \
                f"usan_year should be 1900-{current_year}, got {value}"

    def test_testitem_required_fields(self, valid_testitem_data: pd.DataFrame) -> None:
        """Тест обязательных полей в Testitem данных."""
        required_fields = ["molecule_chembl_id"]
        
        for field in required_fields:
            assert field in valid_testitem_data.columns, f"Required field {field} missing"
            # Проверяем что нет NULL значений в обязательных полях
            assert not valid_testitem_data[field].isna().any(), \
                f"Required field {field} contains NULL values"

    def test_testitem_data_types(self, valid_testitem_data: pd.DataFrame) -> None:
        """Тест типов данных в Testitem."""
        # STRING поля
        string_fields = ["molecule_chembl_id", "pref_name", "structure_type", "molecule_type"]
        for field in string_fields:
            if field in valid_testitem_data.columns:
                assert valid_testitem_data[field].dtype == 'object', \
                    f"Field {field} should be string/object type"
        
        # INT поля
        int_fields = ["molregno", "hba", "hbd", "rtb", "num_ro5_violations", "chirality"]
        for field in int_fields:
            if field in valid_testitem_data.columns:
                assert pd.api.types.is_integer_dtype(valid_testitem_data[field]), \
                    f"Field {field} should be integer type"
        
        # DECIMAL поля
        decimal_fields = ["max_phase", "mw_freebase", "alogp", "psa", "qed_weighted"]
        for field in decimal_fields:
            if field in valid_testitem_data.columns:
                assert pd.api.types.is_numeric_dtype(valid_testitem_data[field]), \
                    f"Field {field} should be numeric type"

    def test_testitem_config_column_order_structure(self, testitem_column_order: list[str]) -> None:
        """Тест структуры column_order в конфигурации Testitem."""
        assert isinstance(testitem_column_order, list), "column_order should be a list"
        assert len(testitem_column_order) > 0, "column_order should not be empty"
        
        # Проверяем что есть основные поля
        column_names = []
        for col in testitem_column_order:
            if isinstance(col, str):
                col_name = col.split('#')[0].strip().strip('"').strip("'")
                column_names.append(col_name)
        
        required_columns = ["molecule_chembl_id"]
        
        for required in required_columns:
            assert required in column_names, f"Required column {required} missing from column_order"

    def test_testitem_config_validation_rules(self, testitem_config: dict[str, Any]) -> None:
        """Тест правил валидации в конфигурации Testitem."""
        column_order = testitem_config["determinism"]["column_order"]
        
        # Проверяем что есть комментарии с типами
        for col in column_order:
            if isinstance(col, str):
                # Проверяем наличие типов в комментариях
                has_type = any(x in col for x in ["STRING", "INT", "DECIMAL", "BOOL", "TEXT"])
                assert has_type, f"Column {col} missing type annotation"
                
                # Проверяем валидационные правила
                if "molecule_chembl_id" in col.lower():
                    assert "NOT NULL" in col or "nullable=False" in col, \
                        f"Molecule ChEMBL ID column {col} should be NOT NULL"
                
                if "max_phase" in col.lower():
                    assert "0-4" in col, \
                        f"max_phase column {col} should have 0-4 range validation"
                
                if "mw_freebase" in col.lower():
                    assert "50-2000" in col, \
                        f"mw_freebase column {col} should have 50-2000 range validation"

    def test_testitem_physicochemical_properties(self, valid_testitem_data: pd.DataFrame) -> None:
        """Тест валидации физико-химических свойств."""
        # Проверяем что молекулярная масса и моноизотопная масса близки
        if "mw_freebase" in valid_testitem_data.columns and "mw_monoisotopic" in valid_testitem_data.columns:
            for i, row in valid_testitem_data.iterrows():
                if not pd.isna(row["mw_freebase"]) and not pd.isna(row["mw_monoisotopic"]):
                    diff = abs(row["mw_freebase"] - row["mw_monoisotopic"])
                    assert diff < 10, \
                        f"Molecular weight difference too large: {row['mw_freebase']} vs {row['mw_monoisotopic']}"
        
        # Проверяем что HBA и HBD разумны
        if "hba" in valid_testitem_data.columns and "hbd" in valid_testitem_data.columns:
            for i, row in valid_testitem_data.iterrows():
                if not pd.isna(row["hba"]) and not pd.isna(row["hbd"]):
                    assert row["hba"] >= 0 and row["hbd"] >= 0, \
                        f"HBA and HBD should be non-negative: HBA={row['hba']}, HBD={row['hbd']}"

    def test_testitem_drug_properties(self, valid_testitem_data: pd.DataFrame) -> None:
        """Тест валидации свойств лекарств."""
        # Проверяем что drug_chembl_id совпадает с molecule_chembl_id
        if "drug_chembl_id" in valid_testitem_data.columns and "molecule_chembl_id" in valid_testitem_data.columns:
            for i, row in valid_testitem_data.iterrows():
                if not pd.isna(row["drug_chembl_id"]) and not pd.isna(row["molecule_chembl_id"]):
                    assert row["drug_chembl_id"] == row["molecule_chembl_id"], \
                        f"drug_chembl_id should match molecule_chembl_id: {row['drug_chembl_id']} != {row['molecule_chembl_id']}"
        
        # Проверяем что drug_name совпадает с pref_name
        if "drug_name" in valid_testitem_data.columns and "pref_name" in valid_testitem_data.columns:
            for i, row in valid_testitem_data.iterrows():
                if not pd.isna(row["drug_name"]) and not pd.isna(row["pref_name"]):
                    assert row["drug_name"] == row["pref_name"], \
                        f"drug_name should match pref_name: {row['drug_name']} != {row['pref_name']}"
