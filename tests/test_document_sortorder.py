"""Тесты для функции определения document_sortorder."""

import pandas as pd

from library.documents.pipeline import _add_document_sortorder_column, _determine_document_sortorder


class TestDocumentSortorder:
    """Тесты для функции определения порядка сортировки документов."""

    def test_complete_data(self):
        """Тест с полными данными."""
        row = pd.Series({
            "pubmed_issn":"1234-5678",
            "publication_date":"2023-05-15",
            "index":123
        })
        
        result = _determine_document_sortorder(row)
        assert result == "1234-5678:2023-05-15:000123"

    def test_string_index(self):
        """Тест со строковым index."""
        row = pd.Series({
            "pubmed_issn":"9876-5432",
            "publication_date":"2022-12-01",
            "index":"456"
        })
        
        result = _determine_document_sortorder(row)
        assert result == "9876-5432:2022-12-01:000456"

    def test_single_digit_index(self):
        """Тест с однозначным index."""
        row = pd.Series({
            "pubmed_issn":"1111-2222",
            "publication_date":"2021-01-01",
            "index":5
        })
        
        result = _determine_document_sortorder(row)
        assert result == "1111-2222:2021-01-01:000005"

    def test_large_index(self):
        """Тест с большим index."""
        row = pd.Series({
            "pubmed_issn":"3333-4444",
            "publication_date":"2020-06-30",
            "index":999999
        })
        
        result = _determine_document_sortorder(row)
        assert result == "3333-4444:2020-06-30:999999"

    def test_empty_index(self):
        """Тест с пустым index."""
        row = pd.Series({
            "pubmed_issn":"5555-6666",
            "publication_date":"2019-03-15",
            "index":""
        })
        
        result = _determine_document_sortorder(row)
        assert result == "5555-6666:2019-03-15:000000"

    def test_none_index(self):
        """Тест с None index."""
        row = pd.Series({
            "pubmed_issn":"7777-8888",
            "publication_date":"2018-09-20",
            "index":None
        })
        
        result = _determine_document_sortorder(row)
        assert result == "7777-8888:2018-09-20:000000"

    def test_nan_index(self):
        """Тест с NaN index."""
        row = pd.Series({
            "pubmed_issn":"9999-0000",
            "publication_date":"2017-11-05",
            "index":pd.NaT
        })
        
        result = _determine_document_sortorder(row)
        assert result == "9999-0000:2017-11-05:000000"

    def test_empty_issn(self):
        """Тест с пустым ISSN."""
        row = pd.Series({
            "pubmed_issn":"",
            "publication_date":"2016-07-12",
            "index":789
        })
        
        result = _determine_document_sortorder(row)
        assert result == ":2016-07-12:000789"

    def test_empty_publication_date(self):
        """Тест с пустой датой публикации."""
        row = pd.Series({
            "pubmed_issn":"1111-3333",
            "publication_date":"",
            "index":321
        })
        
        result = _determine_document_sortorder(row)
        assert result == "1111-3333::000321"

    def test_all_empty_fields(self):
        """Тест со всеми пустыми полями."""
        row = pd.Series({
            "pubmed_issn":"",
            "publication_date":"",
            "index":""
        })
        
        result = _determine_document_sortorder(row)
        assert result == "::000000"

    def test_missing_fields(self):
        """Тест с отсутствующими полями."""
        row = pd.Series({
            "title":"Test Title"
        })
        
        result = _determine_document_sortorder(row)
        assert result == "::000000"

    def test_float_index(self):
        """Тест с float index."""
        row = pd.Series({
            "pubmed_issn":"4444-5555",
            "publication_date":"2015-04-18",
            "index":123.0
        })
        
        result = _determine_document_sortorder(row)
        assert result == "4444-5555:2015-04-18:000123"

    def test_whitespace_values(self):
        """Тест со значениями, содержащими пробелы."""
        row = pd.Series({
            "pubmed_issn":" 6666-7777 ",
            "publication_date":" 2014-08-25 ",
            "index":" 456 "
        })
        
        result = _determine_document_sortorder(row)
        assert result == "6666-7777:2014-08-25:000456"

    def test_negative_index(self):
        """Тест с отрицательным index."""
        row = pd.Series({
            "pubmed_issn":"8888-9999",
            "publication_date":"2013-02-14",
            "index":-123
        })
        
        result = _determine_document_sortorder(row)
        assert result == "8888-9999:2013-02-14:-00123"

    def test_zero_index(self):
        """Тест с нулевым index."""
        row = pd.Series({
            "pubmed_issn":"0000-1111",
            "publication_date":"2012-10-31",
            "index":0
        })
        
        result = _determine_document_sortorder(row)
        assert result == "0000-1111:2012-10-31:000000"


class TestAddDocumentSortorderColumn:
    """Тесты для функции добавления колонки document_sortorder."""

    def test_add_document_sortorder_column(self):
        """Тест добавления колонки document_sortorder в DataFrame."""
        df = pd.DataFrame({
            "document_chembl_id":["CHEMBL123", "CHEMBL456", "CHEMBL789"],
            "pubmed_issn":["1234-5678", "9876-5432", "1111-2222"],
            "publication_date":["2023-05-15", "2022-12-01", "2021-01-01"],
            "index":[123, 456, 5]
        })
        
        result = _add_document_sortorder_column(df)
        
        assert "document_sortorder" in result.columns
        assert result["document_sortorder"].iloc[0] == "1234-5678:2023-05-15:000123"
        assert result["document_sortorder"].iloc[1] == "9876-5432:2022-12-01:000456"
        assert result["document_sortorder"].iloc[2] == "1111-2222:2021-01-01:000005"

    def test_add_document_sortorder_column_empty_dataframe(self):
        """Тест с пустым DataFrame."""
        df = pd.DataFrame()
        
        result = _add_document_sortorder_column(df)
        
        assert "document_sortorder" in result.columns
        assert len(result) == 0

    def test_add_document_sortorder_column_missing_fields(self):
        """Тест с DataFrame без необходимых полей."""
        df = pd.DataFrame({
            "document_chembl_id":["CHEMBL123", "CHEMBL456"],
            "title":["Test Title 1", "Test Title 2"]
        })
        
        result = _add_document_sortorder_column(df)
        
        assert "document_sortorder" in result.columns
        assert result["document_sortorder"].iloc[0] == "::000000"
        assert result["document_sortorder"].iloc[1] == "::000000"

    def test_add_document_sortorder_column_preserves_original_data(self):
        """Тест, что функция сохраняет оригинальные данные."""
        df = pd.DataFrame({
            "document_chembl_id":["CHEMBL123"],
            "title":["Test Title"],
            "pubmed_issn":["1234-5678"],
            "publication_date":["2023-05-15"],
            "index":[123]
        })
        
        result = _add_document_sortorder_column(df)
        
        # Проверяем, что оригинальные данные сохранены
        assert result["document_chembl_id"].iloc[0] == "CHEMBL123"
        assert result["title"].iloc[0] == "Test Title"
        assert result["pubmed_issn"].iloc[0] == "1234-5678"
        assert result["publication_date"].iloc[0] == "2023-05-15"
        assert result["index"].iloc[0] == 123
        
        # Проверяем, что добавлена новая колонка
        assert result["document_sortorder"].iloc[0] == "1234-5678:2023-05-15:000123"

    def test_add_document_sortorder_column_mixed_data_types(self):
        """Тест с различными типами данных."""
        df = pd.DataFrame({
            "document_chembl_id":["CHEMBL123", "CHEMBL456", "CHEMBL789"],
            "pubmed_issn":["1234-5678", None, ""],
            "publication_date":["2023-05-15", "", "2021-01-01"],
            "index":[123, "456", None]
        })
        
        result = _add_document_sortorder_column(df)
        
        assert "document_sortorder" in result.columns
        assert result["document_sortorder"].iloc[0] == "1234-5678:2023-05-15:000123"
        assert result["document_sortorder"].iloc[1] == "::000456"
        assert result["document_sortorder"].iloc[2] == ":2021-01-01:000000"
