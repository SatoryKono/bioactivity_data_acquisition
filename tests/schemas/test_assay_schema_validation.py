"""Модульные тесты валидации схем для Assay пайплайна."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from library.schemas.assay_schema import AssayInputSchema, AssayRawSchema
from tests.schemas.test_column_order_validation import validate_column_order, validate_pandera_schema


class TestAssaySchemaValidation:
    """Тесты валидации Assay схем."""

    @pytest.fixture
    def assay_config(self) -> dict[str, Any]:
        """Конфигурация Assay пайплайна."""
        config_path = Path("configs/config_assay.yaml")
        if not config_path.exists():
            pytest.skip("Assay config file not found")
        
        with open(config_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def assay_column_order(self, assay_config: dict[str, Any]) -> list[str]:
        """Порядок колонок из конфигурации Assay."""
        return assay_config["determinism"]["column_order"]

    @pytest.fixture
    def valid_assay_data(self) -> pd.DataFrame:
        """Валидные тестовые данные для Assay."""
        return pd.DataFrame({
            "assay_chembl_id": ["CHEMBL123456", "CHEMBL789012"],
            "assay_type": ["B", "F"],
            "assay_category": ["Binding", "Functional"],
            "target_chembl_id": ["CHEMBL789", "CHEMBL012"],
            "target_organism": ["Homo sapiens", "Homo sapiens"],
            "target_tax_id": [9606, 9606],
            "bao_format": ["BAO_0000019", "BAO_0000004"],
            "bao_label": ["cell-based format", "biochemical format"],
            "bao_endpoint": ["BAO_0000179", "BAO_0000180"],
            "bao_assay_format": ["BAO_0000019", "BAO_0000004"],
            "bao_assay_type": ["BAO_0000006", "BAO_0000007"],
            "bao_assay_type_label": ["binding assay", "functional assay"],
            "bao_assay_description": ["Description 1", "Description 2"],
            "bao_assay_method": ["BAO_0000008", "BAO_0000009"],
            "bao_assay_method_label": ["method 1", "method 2"],
            "bao_assay_tissue": ["BAO_0000001", "BAO_0000002"],
            "bao_assay_tissue_label": ["tissue 1", "tissue 2"],
            "bao_assay_cell_type": ["BAO_0000003", "BAO_0000004"],
            "bao_assay_cell_type_label": ["cell type 1", "cell type 2"],
            "bao_assay_subcellular_fraction": ["BAO_0000005", "BAO_0000006"],
            "bao_assay_subcellular_fraction_label": ["fraction 1", "fraction 2"],
            "bao_assay_organism": ["BAO_0000007", "BAO_0000008"],
            "bao_assay_organism_label": ["organism 1", "organism 2"],
            "bao_assay_strain": ["BAO_0000009", "BAO_0000010"],
            "bao_assay_strain_label": ["strain 1", "strain 2"],
            "bao_assay_tissue_chembl_id": ["CHEMBL123", "CHEMBL456"],
            "bao_assay_tissue_pref_name": ["Brain", "Liver"],
            "bao_assay_cell_chembl_id": ["CHEMBL789", "CHEMBL012"],
            "bao_assay_cell_pref_name": ["Neuron", "Hepatocyte"],
            "bao_assay_subcellular_fraction_chembl_id": ["CHEMBL345", "CHEMBL678"],
            "bao_assay_subcellular_fraction_pref_name": ["Membrane", "Cytoplasm"],
            "bao_assay_organism_chembl_id": ["CHEMBL901", "CHEMBL234"],
            "bao_assay_organism_pref_name": ["Human", "Mouse"],
            "bao_assay_strain_chembl_id": ["CHEMBL567", "CHEMBL890"],
            "bao_assay_strain_pref_name": ["Wild type", "Knockout"],
            "assay_description": ["Assay description 1", "Assay description 2"],
            "assay_test_type": ["Single concentration", "Dose response"],
            "assay_organism": ["Homo sapiens", "Mus musculus"],
            "assay_tissue": ["Brain", "Liver"],
            "assay_cell_type": ["Neuron", "Hepatocyte"],
            "assay_subcellular_fraction": ["Membrane", "Cytoplasm"],
            "assay_strain": ["Wild type", "Knockout"],
            "assay_variant": ["WT", "KO"],
            "assay_relationship_type": ["D", "D"],
            "assay_relationship_description": ["Description 1", "Description 2"],
            "confidence_score": [9, 8],
            "curated_by": ["Curator 1", "Curator 2"],
            "src_id": [1, 2],
            "type": ["assay", "assay"],
            "chembl_assay_id": ["CHEMBL123456", "CHEMBL789012"],
            "source_system": ["ChEMBL", "ChEMBL"],
            "extracted_at": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
            "chembl_release": ["33.0", "33.0"]
        })

    @pytest.fixture
    def invalid_assay_data(self) -> pd.DataFrame:
        """Невалидные тестовые данные для Assay."""
        return pd.DataFrame({
            "assay_chembl_id": ["INVALID_ID", "CHEMBL789012"],
            "assay_type": ["INVALID_TYPE", "F"],  # Неправильный тип assay
            "target_tax_id": [-1, 9606],  # Отрицательный tax_id
            "confidence_score": [11, 8],  # Вне диапазона 0-9
        })

    def test_assay_input_schema_validation(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест валидации AssayInputSchema с валидными данными."""
        try:
            validated = validate_pandera_schema(valid_assay_data, AssayInputSchema)
            assert len(validated) == len(valid_assay_data)
        except Exception as e:
            pytest.fail(f"AssayInputSchema validation failed with valid data: {e}")

    def test_assay_raw_schema_validation(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест валидации AssayRawSchema с валидными данными."""
        try:
            validated = validate_pandera_schema(valid_assay_data, AssayRawSchema)
            assert len(validated) == len(valid_assay_data)
        except Exception as e:
            pytest.fail(f"AssayRawSchema validation failed with valid data: {e}")

    def test_assay_schema_invalid_data(self, invalid_assay_data: pd.DataFrame) -> None:
        """Тест что схемы отклоняют невалидные данные."""
        with pytest.raises(Exception):  # ValidationError или подобное
            validate_pandera_schema(invalid_assay_data, AssayInputSchema)

    def test_assay_column_order_validation(self, valid_assay_data: pd.DataFrame, assay_column_order: list[str]) -> None:
        """Тест соответствия порядка колонок в Assay данных."""
        # Создаем DataFrame с правильным порядком колонок
        ordered_data = valid_assay_data.copy()
        
        # Извлекаем имена колонок из конфигурации
        expected_columns = []
        for col in assay_column_order:
            if isinstance(col, str):
                col_name = col.split('#')[0].strip().strip('"').strip("'")
                if col_name and col_name in ordered_data.columns:
                    expected_columns.append(col_name)
        
        # Переупорядочиваем колонки
        ordered_data = ordered_data[expected_columns]
        
        # Валидируем порядок
        validate_column_order(ordered_data, assay_column_order)

    def test_assay_chembl_id_patterns(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест паттернов ChEMBL ID в Assay данных."""
        chembl_pattern = re.compile(r'^CHEMBL\d+$')
        
        for col in valid_assay_data.columns:
            if 'chembl_id' in col.lower():
                for value in valid_assay_data[col].dropna():
                    assert chembl_pattern.match(str(value)), \
                        f"Invalid ChEMBL ID pattern in {col}: {value}"

    def test_assay_bao_patterns(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест паттернов BAO ID в Assay данных."""
        bao_pattern = re.compile(r'^BAO_\d+$')
        
        for col in valid_assay_data.columns:
            if 'bao_' in col.lower() and col.endswith('_format') or col.endswith('_endpoint'):
                for value in valid_assay_data[col].dropna():
                    assert bao_pattern.match(str(value)), \
                        f"Invalid BAO pattern in {col}: {value}"

    def test_assay_enum_values(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест enum значений в Assay данных."""
        # Проверяем assay_type
        valid_types = ["B", "F", "A", "P", "T", "U"]
        for value in valid_assay_data["assay_type"].dropna():
            assert value in valid_types, f"Invalid assay_type: {value}"
        
        # Проверяем assay_category
        valid_categories = ["Binding", "Functional", "ADMET", "Toxicity", "Unknown"]
        for value in valid_assay_data["assay_category"].dropna():
            assert value in valid_categories, f"Invalid assay_category: {value}"
        
        # Проверяем assay_relationship_type
        valid_relationships = ["D", "H", "M", "N", "R", "S", "T", "U", "X"]
        for value in valid_assay_data["assay_relationship_type"].dropna():
            assert value in valid_relationships, f"Invalid assay_relationship_type: {value}"

    def test_assay_numeric_ranges(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест диапазонов числовых значений в Assay данных."""
        # Проверяем target_tax_id > 0
        for value in valid_assay_data["target_tax_id"].dropna():
            assert value > 0, f"target_tax_id should be > 0, got {value}"
        
        # Проверяем confidence_score в диапазоне 0-9
        for value in valid_assay_data["confidence_score"].dropna():
            assert 0 <= value <= 9, f"confidence_score should be 0-9, got {value}"
        
        # Проверяем src_id > 0
        for value in valid_assay_data["src_id"].dropna():
            assert value > 0, f"src_id should be > 0, got {value}"

    def test_assay_required_fields(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест обязательных полей в Assay данных."""
        required_fields = ["assay_chembl_id"]
        
        for field in required_fields:
            assert field in valid_assay_data.columns, f"Required field {field} missing"
            # Проверяем что нет NULL значений в обязательных полях
            assert not valid_assay_data[field].isna().any(), \
                f"Required field {field} contains NULL values"

    def test_assay_data_types(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест типов данных в Assay."""
        # STRING поля
        string_fields = ["assay_chembl_id", "assay_type", "assay_category", "target_organism"]
        for field in string_fields:
            if field in valid_assay_data.columns:
                assert valid_assay_data[field].dtype == 'object', \
                    f"Field {field} should be string/object type"
        
        # INT поля
        int_fields = ["target_tax_id", "confidence_score", "src_id"]
        for field in int_fields:
            if field in valid_assay_data.columns:
                assert pd.api.types.is_integer_dtype(valid_assay_data[field]), \
                    f"Field {field} should be integer type"

    def test_assay_config_column_order_structure(self, assay_column_order: list[str]) -> None:
        """Тест структуры column_order в конфигурации Assay."""
        assert isinstance(assay_column_order, list), "column_order should be a list"
        assert len(assay_column_order) > 0, "column_order should not be empty"
        
        # Проверяем что есть основные поля
        column_names = []
        for col in assay_column_order:
            if isinstance(col, str):
                col_name = col.split('#')[0].strip().strip('"').strip("'")
                column_names.append(col_name)
        
        required_columns = ["assay_chembl_id"]
        
        for required in required_columns:
            assert required in column_names, f"Required column {required} missing from column_order"

    def test_assay_config_validation_rules(self, assay_config: dict[str, Any]) -> None:
        """Тест правил валидации в конфигурации Assay."""
        column_order = assay_config["determinism"]["column_order"]
        
        # Проверяем что есть комментарии с типами
        for col in column_order:
            if isinstance(col, str):
                # Проверяем наличие типов в комментариях
                has_type = any(x in col for x in ["STRING", "INT", "DECIMAL", "BOOL", "TEXT"])
                assert has_type, f"Column {col} missing type annotation"
                
                # Проверяем валидационные правила
                if "assay_chembl_id" in col.lower():
                    assert "NOT NULL" in col or "nullable=False" in col, \
                        f"Assay ChEMBL ID column {col} should be NOT NULL"
                
                if "assay_type" in col.lower():
                    assert "enum" in col.lower() or "[" in col, \
                        f"assay_type column {col} should have enum values"
                
                if "confidence_score" in col.lower():
                    assert "0-9" in col, \
                        f"confidence_score column {col} should have 0-9 range validation"

    def test_assay_bao_classification_consistency(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест консистентности BAO классификации."""
        # Проверяем что BAO format и label соответствуют
        if "bao_format" in valid_assay_data.columns and "bao_label" in valid_assay_data.columns:
            for i, row in valid_assay_data.iterrows():
                if not pd.isna(row["bao_format"]) and not pd.isna(row["bao_label"]):
                    # Проверяем что BAO ID имеет правильный формат
                    assert row["bao_format"].startswith("BAO_"), \
                        f"BAO format should start with BAO_: {row['bao_format']}"
        
        # Проверяем что assay_type соответствует BAO классификации
        if "assay_type" in valid_assay_data.columns and "bao_assay_type_label" in valid_assay_data.columns:
            for i, row in valid_assay_data.iterrows():
                if not pd.isna(row["assay_type"]) and not pd.isna(row["bao_assay_type_label"]):
                    # B (Binding) должно соответствовать binding assay
                    if row["assay_type"] == "B":
                        assert "binding" in row["bao_assay_type_label"].lower(), \
                            f"Binding assay should have binding in BAO label: {row['bao_assay_type_label']}"
                    # F (Functional) должно соответствовать functional assay
                    elif row["assay_type"] == "F":
                        assert "functional" in row["bao_assay_type_label"].lower(), \
                            f"Functional assay should have functional in BAO label: {row['bao_assay_type_label']}"

    def test_assay_target_consistency(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест консистентности данных о мишени."""
        # Проверяем что target_chembl_id имеет правильный формат
        if "target_chembl_id" in valid_assay_data.columns:
            for value in valid_assay_data["target_chembl_id"].dropna():
                assert re.match(r'^CHEMBL\d+$', str(value)), \
                    f"target_chembl_id should be ChEMBL ID format: {value}"
        
        # Проверяем что target_organism и target_tax_id соответствуют
        if "target_organism" in valid_assay_data.columns and "target_tax_id" in valid_assay_data.columns:
            for i, row in valid_assay_data.iterrows():
                if not pd.isna(row["target_organism"]) and not pd.isna(row["target_tax_id"]):
                    # Homo sapiens должен иметь tax_id 9606
                    if "Homo sapiens" in row["target_organism"]:
                        assert row["target_tax_id"] == 9606, \
                            f"Homo sapiens should have tax_id 9606, got {row['target_tax_id']}"

    def test_assay_relationship_validation(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест валидации отношений между assay."""
        # Проверяем что assay_relationship_type имеет валидные значения
        if "assay_relationship_type" in valid_assay_data.columns:
            valid_relationships = ["D", "H", "M", "N", "R", "S", "T", "U", "X"]
            for value in valid_assay_data["assay_relationship_type"].dropna():
                assert value in valid_relationships, f"Invalid assay_relationship_type: {value}"
        
        # Проверяем что есть описание для отношений
        if "assay_relationship_description" in valid_assay_data.columns:
            for value in valid_assay_data["assay_relationship_description"].dropna():
                assert isinstance(value, str) and len(value.strip()) > 0, \
                    f"assay_relationship_description should be non-empty string: {value}"

    def test_assay_metadata_consistency(self, valid_assay_data: pd.DataFrame) -> None:
        """Тест консистентности метаданных assay."""
        # Проверяем что extracted_at имеет правильный формат
        if "extracted_at" in valid_assay_data.columns:
            for value in valid_assay_data["extracted_at"].dropna():
                # Проверяем ISO 8601 формат
                assert "T" in str(value) and "Z" in str(value), \
                    f"extracted_at should be in ISO 8601 format: {value}"
        
        # Проверяем что chembl_release имеет правильный формат
        if "chembl_release" in valid_assay_data.columns:
            for value in valid_assay_data["chembl_release"].dropna():
                # Проверяем формат версии (например, "33.0")
                assert re.match(r'^\d+\.\d+$', str(value)), \
                    f"chembl_release should be in format X.Y: {value}"
        
        # Проверяем что source_system имеет валидные значения
        if "source_system" in valid_assay_data.columns:
            valid_sources = ["ChEMBL", "PubChem", "Other"]
            for value in valid_assay_data["source_system"].dropna():
                assert value in valid_sources, f"Invalid source_system: {value}"
