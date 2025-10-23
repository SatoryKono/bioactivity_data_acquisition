"""Unit тесты для модуля объединения данных документов."""

import pandas as pd

from library.documents.merge import (
    normalize_doi,
    resolve_conflicts,
    merge_metadata_records,
    merge_source_data,
    compute_publication_date,
    add_document_sortorder
)


class TestNormalizeDOI:
    """Тесты для нормализации DOI."""
    
    def test_normalize_doi_none(self):
        """Тест с None значением."""
        assert normalize_doi(None) == ""
    
    def test_normalize_doi_empty_string(self):
        """Тест с пустой строкой."""
        assert normalize_doi("") == ""
        assert normalize_doi("   ") == ""
    
    def test_normalize_doi_with_prefixes(self):
        """Тест с различными префиксами DOI."""
        assert normalize_doi("doi:10.1000/test") == "10.1000/test"
        assert normalize_doi("https://doi.org/10.1000/test") == "10.1000/test"
        assert normalize_doi("http://doi.org/10.1000/test") == "10.1000/test"
        assert normalize_doi("doi.org/10.1000/test") == "10.1000/test"
    
    def test_normalize_doi_clean(self):
        """Тест с чистым DOI."""
        assert normalize_doi("10.1000/test") == "10.1000/test"


class TestResolveConflicts:
    """Тесты для разрешения конфликтов."""
    
    def test_resolve_conflicts_both_empty(self):
        """Тест с пустыми значениями."""
        assert resolve_conflicts("", "", "source") == ""
        assert resolve_conflicts(None, None, "source") == ""
    
    def test_resolve_conflicts_base_empty(self):
        """Тест с пустым базовым значением."""
        assert resolve_conflicts("", "source_value", "source") == "source_value"
        assert resolve_conflicts(None, "source_value", "source") == "source_value"
    
    def test_resolve_conflicts_source_empty(self):
        """Тест с пустым значением источника."""
        assert resolve_conflicts("base_value", "", "source") == "base_value"
        assert resolve_conflicts("base_value", None, "source") == "base_value"
    
    def test_resolve_conflicts_priority_base(self):
        """Тест с приоритетом базового значения."""
        assert resolve_conflicts("base", "source", "base") == "base"
    
    def test_resolve_conflicts_priority_source(self):
        """Тест с приоритетом источника."""
        assert resolve_conflicts("base", "source", "source") == "source"
    
    def test_resolve_conflicts_priority_non_empty(self):
        """Тест с приоритетом непустого значения."""
        assert resolve_conflicts("short", "longer_value", "non_empty") == "longer_value"
        assert resolve_conflicts("longer_value", "short", "non_empty") == "longer_value"


class TestMergeMetadataRecords:
    """Тесты для объединения метаданных."""
    
    def test_merge_metadata_empty(self):
        """Тест с пустыми записями."""
        result = merge_metadata_records()
        assert result == {}
    
    def test_merge_metadata_single_record(self):
        """Тест с одной записью."""
        record = {"field1": "value1", "field2": "value2"}
        result = merge_metadata_records(record)
        # Функция добавляет doi_normalised, поэтому проверяем основные поля
        assert result["field1"] == "value1"
        assert result["field2"] == "value2"
        assert "doi_normalised" in result
    
    def test_merge_metadata_multiple_records(self):
        """Тест с несколькими записями."""
        record1 = {"field1": "value1", "field2": "value2"}
        record2 = {"field2": "value2_updated", "field3": "value3"}
        
        result = merge_metadata_records(record1, record2)
        
        assert result["field1"] == "value1"
        assert result["field2"] == "value2_updated"  # Последняя запись перезаписывает
        assert result["field3"] == "value3"
    
    def test_merge_metadata_doi_normalization(self):
        """Тест нормализации DOI."""
        record1 = {"doi": "https://doi.org/10.1000/test"}
        record2 = {"PubMed.DOI": "doi:10.1000/test"}
        
        result = merge_metadata_records(record1, record2)
        
        assert result["doi"] == "10.1000/test"
        assert result["doi_normalised"] == "10.1000/test"


class TestMergeSourceData:
    """Тесты для объединения данных источников."""
    
    def test_merge_source_data_empty_source(self):
        """Тест с пустым источником."""
        base_df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        source_df = pd.DataFrame()
        
        result = merge_source_data(base_df, source_df, "test", "id")
        
        pd.testing.assert_frame_equal(result, base_df)
    
    def test_merge_source_data_success(self):
        """Тест успешного объединения."""
        base_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["A", "B"]
        })
        source_df = pd.DataFrame({
            "id": [1, 2],
            "source_field": ["X", "Y"]
        })
        
        result = merge_source_data(base_df, source_df, "test", "id")
        
        assert len(result) == 2
        assert "source_field" in result.columns
        assert result.iloc[0]["source_field"] == "X"
        assert result.iloc[1]["source_field"] == "Y"
    
    def test_merge_source_data_missing_join_key(self):
        """Тест с отсутствующим ключом объединения."""
        base_df = pd.DataFrame({"id": [1, 2]})
        source_df = pd.DataFrame({"other_id": [1, 2]})
        
        result = merge_source_data(base_df, source_df, "test", "id")
        
        pd.testing.assert_frame_equal(result, base_df)


class TestComputePublicationDate:
    """Тесты для вычисления даты публикации."""
    
    def test_compute_publication_date_full_date(self):
        """Тест с полной датой."""
        df = pd.DataFrame({
            "pubmed_year": [2023],
            "pubmed_month": [6],
            "pubmed_day": [15]
        })
        
        result = compute_publication_date(df)
        
        assert result.iloc[0]["publication_date"] == "2023-06-15"
    
    def test_compute_publication_date_year_month(self):
        """Тест с годом и месяцем."""
        df = pd.DataFrame({
            "pubmed_year": [2023],
            "pubmed_month": [6]
        })
        
        result = compute_publication_date(df)
        
        assert result.iloc[0]["publication_date"] == "2023-06-01"
    
    def test_compute_publication_date_year_only(self):
        """Тест только с годом."""
        df = pd.DataFrame({
            "pubmed_year": [2023]
        })
        
        result = compute_publication_date(df)
        
        assert result.iloc[0]["publication_date"] == "2023-01-01"
    
    def test_compute_publication_date_no_data(self):
        """Тест без данных о дате."""
        df = pd.DataFrame({
            "other_field": ["value"]
        })
        
        result = compute_publication_date(df)
        
        assert result.iloc[0]["publication_date"] == ""
    
    def test_compute_publication_date_priority_sources(self):
        """Тест приоритета источников."""
        df = pd.DataFrame({
            "pubmed_year": [2022],
            "crossref_year": [2023],
            "openalex_year": [2021]
        })
        
        result = compute_publication_date(df)
        
        # Должен использовать первый найденный год (pubmed)
        assert result.iloc[0]["publication_date"] == "2022-01-01"


class TestAddDocumentSortorder:
    """Тесты для добавления порядка сортировки."""
    
    def test_add_document_sortorder(self):
        """Тест добавления порядка сортировки."""
        df = pd.DataFrame({
            "document_chembl_id": ["CHEMBL3", "CHEMBL1", "CHEMBL2"],
            "name": ["C", "A", "B"]
        })
        
        result = add_document_sortorder(df)
        
        # Проверяем, что данные отсортированы по document_chembl_id
        assert result.iloc[0]["document_chembl_id"] == "CHEMBL1"
        assert result.iloc[1]["document_chembl_id"] == "CHEMBL2"
        assert result.iloc[2]["document_chembl_id"] == "CHEMBL3"
        
        # Проверяем порядок сортировки
        assert result.iloc[0]["document_sortorder"] == 0
        assert result.iloc[1]["document_sortorder"] == 1
        assert result.iloc[2]["document_sortorder"] == 2
    
    def test_add_document_sortorder_empty(self):
        """Тест с пустым DataFrame."""
        df = pd.DataFrame()
        
        # Для пустого DataFrame функция должна обработать случай отсутствия document_chembl_id
        result = add_document_sortorder(df)
        
        assert "document_sortorder" in result.columns
        assert len(result) == 0
