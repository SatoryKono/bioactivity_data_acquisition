"""Тесты для функции определения publication_date."""

import pandas as pd

from library.documents.pipeline import _add_publication_date_column, _determine_publication_date


class TestPublicationDate:
    """Тесты для функции определения даты публикации."""

    def test_complete_completed_date(self):
        """Тест с полной датой завершения."""
        row = pd.Series({
            "pubmed_year_completed": 2023,
            "pubmed_month_completed": 5,
            "pubmed_day_completed": 15,
            "pubmed_year_revised": 2022,
            "pubmed_month_revised": 3,
            "pubmed_day_revised": 10
        })
        
        result = _determine_publication_date(row)
        assert result == "2023-05-15"

    def test_complete_revised_date(self):
        """Тест с полной датой пересмотра (когда completed неполная)."""
        row = pd.Series({
            "pubmed_year_completed": 2023,
            "pubmed_month_completed": None,
            "pubmed_day_completed": None,
            "pubmed_year_revised": 2022,
            "pubmed_month_revised": 3,
            "pubmed_day_revised": 10
        })
        
        result = _determine_publication_date(row)
        assert result == "2022-03-10"

    def test_year_only_completed(self):
        """Тест с только годом завершения."""
        row = pd.Series({
            "pubmed_year_completed": 2023,
            "pubmed_month_completed": None,
            "pubmed_day_completed": None,
            "pubmed_year_revised": None,
            "pubmed_month_revised": None,
            "pubmed_day_revised": None
        })
        
        result = _determine_publication_date(row)
        assert result == "2023-01-01"

    def test_year_only_revised(self):
        """Тест с только годом пересмотра."""
        row = pd.Series({
            "pubmed_year_completed": None,
            "pubmed_month_completed": None,
            "pubmed_day_completed": None,
            "pubmed_year_revised": 2022,
            "pubmed_month_revised": None,
            "pubmed_day_revised": None
        })
        
        result = _determine_publication_date(row)
        assert result == "2022-01-01"

    def test_no_date_data(self):
        """Тест без данных о дате."""
        row = pd.Series({
            "pubmed_year_completed": None,
            "pubmed_month_completed": None,
            "pubmed_day_completed": None,
            "pubmed_year_revised": None,
            "pubmed_month_revised": None,
            "pubmed_day_revised": None
        })
        
        result = _determine_publication_date(row)
        assert result == "0000-01-01"

    def test_empty_strings(self):
        """Тест с пустыми строками."""
        row = pd.Series({
            "pubmed_year_completed": "",
            "pubmed_month_completed": "",
            "pubmed_day_completed": "",
            "pubmed_year_revised": "",
            "pubmed_month_revised": "",
            "pubmed_day_revised": ""
        })
        
        result = _determine_publication_date(row)
        assert result == "0000-01-01"

    def test_string_values(self):
        """Тест со строковыми значениями."""
        row = pd.Series({
            "pubmed_year_completed": "2023",
            "pubmed_month_completed": "5",
            "pubmed_day_completed": "15",
            "pubmed_year_revised": "2022",
            "pubmed_month_revised": "3",
            "pubmed_day_revised": "10"
        })
        
        result = _determine_publication_date(row)
        assert result == "2023-05-15"

    def test_float_values(self):
        """Тест с float значениями."""
        row = pd.Series({
            "pubmed_year_completed": 2023.0,
            "pubmed_month_completed": 5.0,
            "pubmed_day_completed": 15.0,
            "pubmed_year_revised": 2022.0,
            "pubmed_month_revised": 3.0,
            "pubmed_day_revised": 10.0
        })
        
        result = _determine_publication_date(row)
        assert result == "2023-05-15"

    def test_invalid_values(self):
        """Тест с невалидными значениями."""
        row = pd.Series({
            "pubmed_year_completed": "invalid",
            "pubmed_month_completed": "invalid",
            "pubmed_day_completed": "invalid",
            "pubmed_year_revised": "invalid",
            "pubmed_month_revised": "invalid",
            "pubmed_day_revised": "invalid"
        })
        
        result = _determine_publication_date(row)
        assert result == "0000-01-01"

    def test_partial_completed_date(self):
        """Тест с частичной датой завершения (только год и месяц)."""
        row = pd.Series({
            "pubmed_year_completed": 2023,
            "pubmed_month_completed": 5,
            "pubmed_day_completed": None,
            "pubmed_year_revised": 2022,
            "pubmed_month_revised": 3,
            "pubmed_day_revised": 10
        })
        
        result = _determine_publication_date(row)
        assert result == "2022-03-10"  # Должна использоваться полная дата пересмотра

    def test_whitespace_values(self):
        """Тест со значениями, содержащими пробелы."""
        row = pd.Series({
            "pubmed_year_completed": " 2023 ",
            "pubmed_month_completed": " 5 ",
            "pubmed_day_completed": " 15 ",
            "pubmed_year_revised": " 2022 ",
            "pubmed_month_revised": " 3 ",
            "pubmed_day_revised": " 10 "
        })
        
        result = _determine_publication_date(row)
        assert result == "2023-05-15"


class TestAddPublicationDateColumn:
    """Тесты для функции добавления колонки publication_date."""

    def test_add_publication_date_column(self):
        """Тест добавления колонки publication_date в DataFrame."""
        df = pd.DataFrame({
            "document_chembl_id": ["CHEMBL123", "CHEMBL456"],
            "pubmed_year_completed": [2023, None],
            "pubmed_month_completed": [5, None],
            "pubmed_day_completed": [15, None],
            "pubmed_year_revised": [2022, 2021],
            "pubmed_month_revised": [3, None],
            "pubmed_day_revised": [10, None]
        })
        
        result = _add_publication_date_column(df)
        
        assert "publication_date" in result.columns
        assert result["publication_date"].iloc[0] == "2023-05-15"
        assert result["publication_date"].iloc[1] == "2021-01-01"

    def test_add_publication_date_column_empty_dataframe(self):
        """Тест с пустым DataFrame."""
        df = pd.DataFrame()
        
        result = _add_publication_date_column(df)
        
        assert "publication_date" in result.columns
        assert len(result) == 0

    def test_add_publication_date_column_no_pubmed_fields(self):
        """Тест с DataFrame без полей PubMed."""
        df = pd.DataFrame({
            "document_chembl_id": ["CHEMBL123", "CHEMBL456"],
            "title": ["Test Title 1", "Test Title 2"]
        })
        
        result = _add_publication_date_column(df)
        
        assert "publication_date" in result.columns
        assert result["publication_date"].iloc[0] == "0000-01-01"
        assert result["publication_date"].iloc[1] == "0000-01-01"

    def test_add_publication_date_column_preserves_original_data(self):
        """Тест, что функция сохраняет оригинальные данные."""
        df = pd.DataFrame({
            "document_chembl_id": ["CHEMBL123"],
            "title": ["Test Title"],
            "pubmed_year_completed": [2023],
            "pubmed_month_completed": [5],
            "pubmed_day_completed": [15]
        })
        
        result = _add_publication_date_column(df)
        
        # Проверяем, что оригинальные данные сохранены
        assert result["document_chembl_id"].iloc[0] == "CHEMBL123"
        assert result["title"].iloc[0] == "Test Title"
        assert result["pubmed_year_completed"].iloc[0] == 2023
        assert result["pubmed_month_completed"].iloc[0] == 5
        assert result["pubmed_day_completed"].iloc[0] == 15
        
        # Проверяем, что добавлена новая колонка
        assert result["publication_date"].iloc[0] == "2023-05-15"
