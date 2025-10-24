"""Модульные тесты валидации схем для Target пайплайна."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from library.schemas.target_schema import TargetInputSchema, TargetRawSchema
from tests.schemas.test_column_order_validation import validate_column_order, validate_pandera_schema


class TestTargetSchemaValidation:
    """Тесты валидации Target схем."""

    @pytest.fixture
    def target_config(self) -> dict[str, Any]:
        """Конфигурация Target пайплайна."""
        config_path = Path("configs/config_target.yaml")
        if not config_path.exists():
            pytest.skip("Target config file not found")
        
        with open(config_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def target_column_order(self, target_config: dict[str, Any]) -> list[str]:
        """Порядок колонок из конфигурации Target."""
        return target_config["determinism"]["column_order"]

    @pytest.fixture
    def valid_target_data(self) -> pd.DataFrame:
        """Валидные тестовые данные для Target."""
        return pd.DataFrame({
            "target_chembl_id": ["CHEMBL123456", "CHEMBL789012"],
            "pref_name": ["Adenosine A1 receptor", "Dopamine D2 receptor"],
            "component_description": ["G protein-coupled receptor", "Dopamine receptor"],
            "component_id": [1, 2],
            "relationship": ["TARGET", "TARGET"],
            "gene": ["ADORA1", "DRD2"],
            "uniprot_id": ["P30542", "P14416"],
            "mapping_uniprot_id": ["P30542", "P14416"],
            "chembl_alternative_name": ["A1R", "D2R"],
            "ec_code": ["3.6.5.1", "3.6.5.1"],
            "hgnc_name": ["ADORA1", "DRD2"],
            "hgnc_id": ["HGNC:263", "HGNC:3023"],
            "target_type": ["SINGLE PROTEIN", "SINGLE PROTEIN"],
            "tax_id": [9606, 9606],
            "species_group_flag": [True, True],
            "target_components": ["Component 1", "Component 2"],
            "protein_classifications": ["GPCR", "GPCR"],
            "cross_references": ["Ref1", "Ref2"],
            "reaction_ec_numbers": ["3.6.5.1", "3.6.5.1"],
            "uniprot_id_primary": ["P30542", "P14416"],
            "uniprot_ids_all": ["P30542;P30543", "P14416;P14417"],
            "uniProtkbId": ["P30542", "P14416"],
            "secondaryAccessions": ["P30543", "P14417"],
            "secondaryAccessionNames": ["Isoform 2", "Isoform 2"],
            "isoform_ids": ["P30542-1", "P14416-1"],
            "isoform_names": ["Isoform 1", "Isoform 1"],
            "isoform_synonyms": ["Synonym 1", "Synonym 1"],
            "recommendedName": ["Adenosine A1 receptor", "Dopamine D2 receptor"],
            "geneName": ["ADORA1", "DRD2"],
            "protein_name_canonical": ["A1R", "D2R"],
            "protein_name_alt": ["Alternative name 1", "Alternative name 2"],
            "protein_synonym_list": ["Synonym list 1", "Synonym list 2"],
            "taxon_id": [9606, 9606],
            "lineage_superkingdom": ["Eukaryota", "Eukaryota"],
            "lineage_phylum": ["Chordata", "Chordata"],
            "lineage_class": ["Mammalia", "Mammalia"],
            "sequence_length": [326, 443],
            "features_signal_peptide": [False, False],
            "features_transmembrane": [True, True],
            "features_topology": ["Topology 1", "Topology 2"],
            "source_system": ["ChEMBL", "ChEMBL"],
            "extracted_at": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
            "chembl_release": ["33.0", "33.0"]
        })

    @pytest.fixture
    def invalid_target_data(self) -> pd.DataFrame:
        """Невалидные тестовые данные для Target."""
        return pd.DataFrame({
            "target_chembl_id": ["INVALID_ID", "CHEMBL789012"],
            "hgnc_id": ["INVALID_HGNC", "HGNC:3023"],  # Неправильный формат HGNC
            "tax_id": [-1, 9606],  # Отрицательный tax_id
            "sequence_length": [-100, 443],  # Отрицательная длина последовательности
        })

    def test_target_input_schema_validation(self, valid_target_data: pd.DataFrame) -> None:
        """Тест валидации TargetInputSchema с валидными данными."""
        try:
            validated = validate_pandera_schema(valid_target_data, TargetInputSchema)
            assert len(validated) == len(valid_target_data)
        except Exception as e:
            pytest.fail(f"TargetInputSchema validation failed with valid data: {e}")

    def test_target_raw_schema_validation(self, valid_target_data: pd.DataFrame) -> None:
        """Тест валидации TargetRawSchema с валидными данными."""
        try:
            validated = validate_pandera_schema(valid_target_data, TargetRawSchema)
            assert len(validated) == len(valid_target_data)
        except Exception as e:
            pytest.fail(f"TargetRawSchema validation failed with valid data: {e}")

    def test_target_schema_invalid_data(self, invalid_target_data: pd.DataFrame) -> None:
        """Тест что схемы отклоняют невалидные данные."""
        with pytest.raises(Exception):  # ValidationError или подобное
            validate_pandera_schema(invalid_target_data, TargetInputSchema)

    def test_target_column_order_validation(self, valid_target_data: pd.DataFrame, target_column_order: list[str]) -> None:
        """Тест соответствия порядка колонок в Target данных."""
        # Создаем DataFrame с правильным порядком колонок
        ordered_data = valid_target_data.copy()
        
        # Извлекаем имена колонок из конфигурации
        expected_columns = []
        for col in target_column_order:
            if isinstance(col, str):
                col_name = col.split('#')[0].strip().strip('"').strip("'")
                if col_name and col_name in ordered_data.columns:
                    expected_columns.append(col_name)
        
        # Переупорядочиваем колонки
        ordered_data = ordered_data[expected_columns]
        
        # Валидируем порядок
        validate_column_order(ordered_data, target_column_order)

    def test_target_chembl_id_patterns(self, valid_target_data: pd.DataFrame) -> None:
        """Тест паттернов ChEMBL ID в Target данных."""
        chembl_pattern = re.compile(r'^CHEMBL\d+$')
        
        for col in valid_target_data.columns:
            if 'chembl_id' in col.lower():
                for value in valid_target_data[col].dropna():
                    assert chembl_pattern.match(str(value)), \
                        f"Invalid ChEMBL ID pattern in {col}: {value}"

    def test_target_hgnc_id_patterns(self, valid_target_data: pd.DataFrame) -> None:
        """Тест паттернов HGNC ID в Target данных."""
        hgnc_pattern = re.compile(r'^HGNC:\d+$')
        
        for value in valid_target_data["hgnc_id"].dropna():
            assert hgnc_pattern.match(str(value)), \
                f"Invalid HGNC ID pattern: {value}"

    def test_target_uniprot_id_patterns(self, valid_target_data: pd.DataFrame) -> None:
        """Тест паттернов UniProt ID в Target данных."""
        uniprot_pattern = re.compile(r'^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$')
        
        for col in valid_target_data.columns:
            if 'uniprot_id' in col.lower() and col != 'uniprot_ids_all':
                for value in valid_target_data[col].dropna():
                    assert uniprot_pattern.match(str(value)), \
                        f"Invalid UniProt ID pattern in {col}: {value}"

    def test_target_numeric_ranges(self, valid_target_data: pd.DataFrame) -> None:
        """Тест диапазонов числовых значений в Target данных."""
        # Проверяем tax_id > 0
        for value in valid_target_data["tax_id"].dropna():
            assert value > 0, f"tax_id should be > 0, got {value}"
        
        # Проверяем sequence_length > 0
        for value in valid_target_data["sequence_length"].dropna():
            assert value > 0, f"sequence_length should be > 0, got {value}"
        
        # Проверяем component_id > 0
        for value in valid_target_data["component_id"].dropna():
            assert value > 0, f"component_id should be > 0, got {value}"

    def test_target_boolean_fields(self, valid_target_data: pd.DataFrame) -> None:
        """Тест boolean полей в Target данных."""
        boolean_fields = ["species_group_flag", "features_signal_peptide", "features_transmembrane"]
        
        for field in boolean_fields:
            if field in valid_target_data.columns:
                for value in valid_target_data[field].dropna():
                    assert isinstance(value, bool), \
                        f"Field {field} should be boolean, got {type(value)}"

    def test_target_required_fields(self, valid_target_data: pd.DataFrame) -> None:
        """Тест обязательных полей в Target данных."""
        required_fields = ["target_chembl_id"]
        
        for field in required_fields:
            assert field in valid_target_data.columns, f"Required field {field} missing"
            # Проверяем что нет NULL значений в обязательных полях
            assert not valid_target_data[field].isna().any(), \
                f"Required field {field} contains NULL values"

    def test_target_data_types(self, valid_target_data: pd.DataFrame) -> None:
        """Тест типов данных в Target."""
        # STRING поля
        string_fields = ["target_chembl_id", "pref_name", "gene", "uniprot_id"]
        for field in string_fields:
            if field in valid_target_data.columns:
                assert valid_target_data[field].dtype == 'object', \
                    f"Field {field} should be string/object type"
        
        # INT поля
        int_fields = ["component_id", "tax_id", "sequence_length"]
        for field in int_fields:
            if field in valid_target_data.columns:
                assert pd.api.types.is_integer_dtype(valid_target_data[field]), \
                    f"Field {field} should be integer type"
        
        # BOOL поля
        bool_fields = ["species_group_flag", "features_signal_peptide", "features_transmembrane"]
        for field in bool_fields:
            if field in valid_target_data.columns:
                assert pd.api.types.is_bool_dtype(valid_target_data[field]), \
                    f"Field {field} should be boolean type"

    def test_target_config_column_order_structure(self, target_column_order: list[str]) -> None:
        """Тест структуры column_order в конфигурации Target."""
        assert isinstance(target_column_order, list), "column_order should be a list"
        assert len(target_column_order) > 0, "column_order should not be empty"
        
        # Проверяем что есть основные поля
        column_names = []
        for col in target_column_order:
            if isinstance(col, str):
                col_name = col.split('#')[0].strip().strip('"').strip("'")
                column_names.append(col_name)
        
        required_columns = ["target_chembl_id"]
        
        for required in required_columns:
            assert required in column_names, f"Required column {required} missing from column_order"

    def test_target_config_validation_rules(self, target_config: dict[str, Any]) -> None:
        """Тест правил валидации в конфигурации Target."""
        column_order = target_config["determinism"]["column_order"]
        
        # Проверяем что есть комментарии с типами
        for col in column_order:
            if isinstance(col, str):
                # Проверяем наличие типов в комментариях
                has_type = any(x in col for x in ["STRING", "INT", "DECIMAL", "BOOL", "TEXT"])
                assert has_type, f"Column {col} missing type annotation"
                
                # Проверяем валидационные правила
                if "target_chembl_id" in col.lower():
                    assert "NOT NULL" in col or "nullable=False" in col, \
                        f"Target ChEMBL ID column {col} should be NOT NULL"
                
                if "hgnc_id" in col.lower():
                    assert "pattern" in col.lower(), \
                        f"HGNC ID column {col} should have pattern validation"
                
                if "tax_id" in col.lower():
                    assert "> 0" in col, \
                        f"tax_id column {col} should have > 0 validation"

    def test_target_enum_values(self, valid_target_data: pd.DataFrame) -> None:
        """Тест enum значений в Target данных."""
        # Проверяем target_type
        valid_types = ["SINGLE PROTEIN", "PROTEIN COMPLEX", "PROTEIN FAMILY", "PROTEIN GROUP"]
        for value in valid_target_data["target_type"].dropna():
            assert value in valid_types, f"Invalid target_type: {value}"
        
        # Проверяем relationship
        valid_relationships = ["TARGET", "NON-TARGET", "UNKNOWN"]
        for value in valid_target_data["relationship"].dropna():
            assert value in valid_relationships, f"Invalid relationship: {value}"

    def test_target_taxonomy_validation(self, valid_target_data: pd.DataFrame) -> None:
        """Тест валидации таксономических данных."""
        # Проверяем что tax_id и taxon_id совпадают
        if "tax_id" in valid_target_data.columns and "taxon_id" in valid_target_data.columns:
            for i, row in valid_target_data.iterrows():
                if not pd.isna(row["tax_id"]) and not pd.isna(row["taxon_id"]):
                    assert row["tax_id"] == row["taxon_id"], \
                        f"tax_id and taxon_id should match: {row['tax_id']} != {row['taxon_id']}"
        
        # Проверяем lineage поля
        lineage_fields = ["lineage_superkingdom", "lineage_phylum", "lineage_class"]
        for field in lineage_fields:
            if field in valid_target_data.columns:
                for value in valid_target_data[field].dropna():
                    assert isinstance(value, str) and len(value) > 0, \
                        f"Lineage field {field} should be non-empty string"

    def test_target_sequence_validation(self, valid_target_data: pd.DataFrame) -> None:
        """Тест валидации данных последовательности."""
        if "sequence_length" in valid_target_data.columns:
            for value in valid_target_data["sequence_length"].dropna():
                assert isinstance(value, (int, float)) and value > 0, \
                    f"sequence_length should be positive number, got {value}"
        
        # Проверяем что features поля являются boolean
        features_fields = ["features_signal_peptide", "features_transmembrane"]
        for field in features_fields:
            if field in valid_target_data.columns:
                for value in valid_target_data[field].dropna():
                    assert isinstance(value, bool), \
                        f"Feature field {field} should be boolean, got {type(value)}"
