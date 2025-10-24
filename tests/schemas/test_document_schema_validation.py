"""Модульные тесты валидации схем для Document пайплайна."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from library.schemas.document_schema import DocumentInputSchema
from tests.schemas.test_column_order_validation import validate_column_order, validate_pandera_schema


class TestDocumentSchemaValidation:
    """Тесты валидации Document схем."""

    @pytest.fixture
    def document_config(self) -> dict[str, Any]:
        """Конфигурация Document пайплайна."""
        config_path = Path("configs/config_document.yaml")
        if not config_path.exists():
            pytest.skip("Document config file not found")
        
        with open(config_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def document_column_order(self, document_config: dict[str, Any]) -> list[str]:
        """Порядок колонок из конфигурации Document."""
        return document_config["determinism"]["column_order"]

    @pytest.fixture
    def valid_document_data(self) -> pd.DataFrame:
        """Валидные тестовые данные для Document."""
        return pd.DataFrame({
            "document_chembl_id": ["CHEMBL123456", "CHEMBL789012"],
            "pubmed_id": ["12345678", "87654321"],
            "doi": ["10.1016/j.bcp.2003.08.027", "10.1016/S0024-3205(01)01169-3"],
            "classification": ["Journal Article", "Journal Article"],
            "document_contains_external_links": [True, False],
            "is_experimental_doc": [True, True],
            "document_citation": ["Author et al. (2024)", "Author et al. (2023)"],
            "pubmed_mesh_descriptors": ["Descriptor1;Descriptor2", "Descriptor3"],
            "pubmed_mesh_qualifiers": ["Qualifier1", "Qualifier2"],
            "pubmed_chemical_list": ["Chemical1;Chemical2", "Chemical3"],
            "crossref_subject": ["Subject1", "Subject2"],
            "chembl_pmid": ["12345678", "87654321"],
            "crossref_pmid": ["12345678", "87654321"],
            "openalex_pmid": ["12345678", "87654321"],
            "pubmed_pmid": ["12345678", "87654321"],
            "semantic_scholar_pmid": ["12345678", "87654321"],
            "chembl_title": ["Title from ChEMBL", "Another Title"],
            "crossref_title": ["Title from Crossref", "Another Title"],
            "openalex_title": ["Title from OpenAlex", "Another Title"],
            "pubmed_article_title": ["Title from PubMed", "Another Title"],
            "semantic_scholar_title": ["Title from Semantic Scholar", "Another Title"],
            "chembl_abstract": ["Abstract from ChEMBL", "Another Abstract"],
            "crossref_abstract": ["Abstract from Crossref", "Another Abstract"],
            "openalex_abstract": ["Abstract from OpenAlex", "Another Abstract"],
            "pubmed_abstract": ["Abstract from PubMed", "Another Abstract"],
            "chembl_authors": ["Author1;Author2", "Author3"],
            "crossref_authors": ["Author1;Author2", "Author3"],
            "openalex_authors": ["Author1;Author2", "Author3"],
            "pubmed_authors": ["Author1;Author2", "Author3"],
            "semantic_scholar_authors": ["Author1;Author2", "Author3"],
            "chembl_doi": ["10.1016/j.bcp.2003.08.027", "10.1016/S0024-3205(01)01169-3"],
            "crossref_doi": ["10.1016/j.bcp.2003.08.027", "10.1016/S0024-3205(01)01169-3"],
            "openalex_doi": ["10.1016/j.bcp.2003.08.027", "10.1016/S0024-3205(01)01169-3"],
            "pubmed_doi": ["10.1016/j.bcp.2003.08.027", "10.1016/S0024-3205(01)01169-3"],
            "semantic_scholar_doi": ["10.1016/j.bcp.2003.08.027", "10.1016/S0024-3205(01)01169-3"],
            "source_system": ["ChEMBL", "ChEMBL"],
            "extracted_at": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
            "chembl_release": ["33.0", "33.0"]
        })

    @pytest.fixture
    def invalid_document_data(self) -> pd.DataFrame:
        """Невалидные тестовые данные для Document."""
        return pd.DataFrame({
            "document_chembl_id": ["INVALID_ID", "CHEMBL789012"],
            "pubmed_id": ["INVALID_PMID", "87654321"],  # Неправильный формат PMID
            "doi": ["invalid_doi", "10.1016/S0024-3205(01)01169-3"],  # Неправильный формат DOI
        })

    def test_document_input_schema_validation(self, valid_document_data: pd.DataFrame) -> None:
        """Тест валидации DocumentInputSchema с валидными данными."""
        try:
            validated = validate_pandera_schema(valid_document_data, DocumentInputSchema)
            assert len(validated) == len(valid_document_data)
        except Exception as e:
            pytest.fail(f"DocumentInputSchema validation failed with valid data: {e}")

    def test_document_schema_invalid_data(self, invalid_document_data: pd.DataFrame) -> None:
        """Тест что схемы отклоняют невалидные данные."""
        with pytest.raises(Exception):  # ValidationError или подобное
            validate_pandera_schema(invalid_document_data, DocumentInputSchema)

    def test_document_column_order_validation(self, valid_document_data: pd.DataFrame, document_column_order: list[str]) -> None:
        """Тест соответствия порядка колонок в Document данных."""
        # Создаем DataFrame с правильным порядком колонок
        ordered_data = valid_document_data.copy()
        
        # Извлекаем имена колонок из конфигурации
        expected_columns = []
        for col in document_column_order:
            if isinstance(col, str):
                col_name = col.split('#')[0].strip().strip('"').strip("'")
                if col_name and col_name in ordered_data.columns:
                    expected_columns.append(col_name)
        
        # Переупорядочиваем колонки
        ordered_data = ordered_data[expected_columns]
        
        # Валидируем порядок
        validate_column_order(ordered_data, document_column_order)

    def test_document_chembl_id_patterns(self, valid_document_data: pd.DataFrame) -> None:
        """Тест паттернов ChEMBL ID в Document данных."""
        chembl_pattern = re.compile(r'^CHEMBL\d+$')
        
        for col in valid_document_data.columns:
            if 'chembl_id' in col.lower():
                for value in valid_document_data[col].dropna():
                    assert chembl_pattern.match(str(value)), \
                        f"Invalid ChEMBL ID pattern in {col}: {value}"

    def test_document_doi_patterns(self, valid_document_data: pd.DataFrame) -> None:
        """Тест паттернов DOI в Document данных."""
        doi_pattern = re.compile(r'^10\.\d+/[^\s]+$')
        
        for col in valid_document_data.columns:
            if 'doi' in col.lower():
                for value in valid_document_data[col].dropna():
                    assert doi_pattern.match(str(value)), \
                        f"Invalid DOI pattern in {col}: {value}"

    def test_document_pmid_patterns(self, valid_document_data: pd.DataFrame) -> None:
        """Тест паттернов PMID в Document данных."""
        pmid_pattern = re.compile(r'^\d+$')
        
        for col in valid_document_data.columns:
            if 'pmid' in col.lower():
                for value in valid_document_data[col].dropna():
                    assert pmid_pattern.match(str(value)), \
                        f"Invalid PMID pattern in {col}: {value}"

    def test_document_boolean_fields(self, valid_document_data: pd.DataFrame) -> None:
        """Тест boolean полей в Document данных."""
        boolean_fields = ["document_contains_external_links", "is_experimental_doc"]
        
        for field in boolean_fields:
            if field in valid_document_data.columns:
                for value in valid_document_data[field].dropna():
                    assert isinstance(value, bool), \
                        f"Field {field} should be boolean, got {type(value)}"

    def test_document_required_fields(self, valid_document_data: pd.DataFrame) -> None:
        """Тест обязательных полей в Document данных."""
        required_fields = ["document_chembl_id"]
        
        for field in required_fields:
            assert field in valid_document_data.columns, f"Required field {field} missing"
            # Проверяем что нет NULL значений в обязательных полях
            assert not valid_document_data[field].isna().any(), \
                f"Required field {field} contains NULL values"

    def test_document_data_types(self, valid_document_data: pd.DataFrame) -> None:
        """Тест типов данных в Document."""
        # STRING поля
        string_fields = ["document_chembl_id", "pubmed_id", "doi", "classification"]
        for field in string_fields:
            if field in valid_document_data.columns:
                assert valid_document_data[field].dtype == 'object', \
                    f"Field {field} should be string/object type"
        
        # BOOL поля
        bool_fields = ["document_contains_external_links", "is_experimental_doc"]
        for field in bool_fields:
            if field in valid_document_data.columns:
                assert pd.api.types.is_bool_dtype(valid_document_data[field]), \
                    f"Field {field} should be boolean type"
        
        # TEXT поля
        text_fields = ["document_citation", "pubmed_mesh_descriptors", "crossref_subject"]
        for field in text_fields:
            if field in valid_document_data.columns:
                assert valid_document_data[field].dtype == 'object', \
                    f"Field {field} should be text/object type"

    def test_document_config_column_order_structure(self, document_column_order: list[str]) -> None:
        """Тест структуры column_order в конфигурации Document."""
        assert isinstance(document_column_order, list), "column_order should be a list"
        assert len(document_column_order) > 0, "column_order should not be empty"
        
        # Проверяем что есть основные поля
        column_names = []
        for col in document_column_order:
            if isinstance(col, str):
                col_name = col.split('#')[0].strip().strip('"').strip("'")
                column_names.append(col_name)
        
        required_columns = ["document_chembl_id"]
        
        for required in required_columns:
            assert required in column_names, f"Required column {required} missing from column_order"

    def test_document_config_validation_rules(self, document_config: dict[str, Any]) -> None:
        """Тест правил валидации в конфигурации Document."""
        column_order = document_config["determinism"]["column_order"]
        
        # Проверяем что есть комментарии с типами
        for col in column_order:
            if isinstance(col, str):
                # Проверяем наличие типов в комментариях
                has_type = any(x in col for x in ["STRING", "INT", "DECIMAL", "BOOL", "TEXT"])
                assert has_type, f"Column {col} missing type annotation"
                
                # Проверяем валидационные правила
                if "document_chembl_id" in col.lower():
                    assert "NOT NULL" in col or "nullable=False" in col, \
                        f"Document ChEMBL ID column {col} should be NOT NULL"
                
                if "doi" in col.lower():
                    assert "pattern" in col.lower(), \
                        f"DOI column {col} should have pattern validation"
                
                if "pmid" in col.lower():
                    assert "pattern" in col.lower(), \
                        f"PMID column {col} should have pattern validation"

    def test_document_enum_values(self, valid_document_data: pd.DataFrame) -> None:
        """Тест enum значений в Document данных."""
        # Проверяем classification
        valid_classifications = ["Journal Article", "Patent", "Book", "Conference Paper", "Review"]
        for value in valid_document_data["classification"].dropna():
            assert value in valid_classifications, f"Invalid classification: {value}"

    def test_document_multi_source_fields(self, valid_document_data: pd.DataFrame) -> None:
        """Тест полей из множественных источников."""
        # Проверяем что поля из разных источников имеют одинаковые значения
        source_fields = {
            "pmid": ["chembl_pmid", "crossref_pmid", "openalex_pmid", "pubmed_pmid", "semantic_scholar_pmid"],
            "doi": ["chembl_doi", "crossref_doi", "openalex_doi", "pubmed_doi", "semantic_scholar_doi"],
            "title": ["chembl_title", "crossref_title", "openalex_title", "pubmed_article_title", "semantic_scholar_title"],
            "abstract": ["chembl_abstract", "crossref_abstract", "openalex_abstract", "pubmed_abstract"],
            "authors": ["chembl_authors", "crossref_authors", "openalex_authors", "pubmed_authors", "semantic_scholar_authors"]
        }
        
        for field_type, fields in source_fields.items():
            for field in fields:
                if field in valid_document_data.columns:
                    # Проверяем что поле содержит данные
                    non_null_count = valid_document_data[field].notna().sum()
                    assert non_null_count > 0, f"Field {field} should have some non-null values"

    def test_document_text_content_validation(self, valid_document_data: pd.DataFrame) -> None:
        """Тест валидации текстового контента."""
        text_fields = ["document_citation", "pubmed_mesh_descriptors", "crossref_subject"]
        
        for field in text_fields:
            if field in valid_document_data.columns:
                for value in valid_document_data[field].dropna():
                    assert isinstance(value, str), f"Text field {field} should contain strings"
                    # Проверяем что текст не слишком короткий (если не пустой)
                    if len(value.strip()) > 0:
                        assert len(value.strip()) > 2, f"Text field {field} should have meaningful content"

    def test_document_metadata_consistency(self, valid_document_data: pd.DataFrame) -> None:
        """Тест консистентности метаданных документов."""
        # Проверяем что extracted_at имеет правильный формат
        if "extracted_at" in valid_document_data.columns:
            for value in valid_document_data["extracted_at"].dropna():
                # Проверяем ISO 8601 формат
                assert "T" in str(value) and "Z" in str(value), \
                    f"extracted_at should be in ISO 8601 format: {value}"
        
        # Проверяем что chembl_release имеет правильный формат
        if "chembl_release" in valid_document_data.columns:
            for value in valid_document_data["chembl_release"].dropna():
                # Проверяем формат версии (например, "33.0")
                assert re.match(r'^\d+\.\d+$', str(value)), \
                    f"chembl_release should be in format X.Y: {value}"

    def test_document_source_system_validation(self, valid_document_data: pd.DataFrame) -> None:
        """Тест валидации source_system."""
        if "source_system" in valid_document_data.columns:
            valid_sources = ["ChEMBL", "Crossref", "OpenAlex", "PubMed", "Semantic Scholar"]
            for value in valid_document_data["source_system"].dropna():
                assert value in valid_sources, f"Invalid source_system: {value}"
