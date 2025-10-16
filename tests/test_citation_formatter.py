"""Тесты для функции формирования литературных ссылок."""

import pandas as pd
import pytest

from library.tools.citation_formatter import format_citation, add_citation_column


class TestFormatCitation:
    """Тесты для функции format_citation."""

    def test_basic_citation_with_issue(self):
        """Тест базовой ссылки с issue."""
        result = format_citation(
            journal="Nature",
            volume="612",
            issue="7940",
            first_page="100",
            last_page="105"
        )
        assert result == "Nature, 612 (7940). p. 100-105"

    def test_basic_citation_without_issue(self):
        """Тест базовой ссылки без issue."""
        result = format_citation(
            journal="Nature",
            volume="612",
            issue="",
            first_page="100",
            last_page="100"
        )
        assert result == "Nature, 612. 100"

    def test_single_page_non_numeric_last_page(self):
        """Тест одной страницы с нечисловым last_page."""
        result = format_citation(
            journal="JAMA",
            volume="327",
            issue="12",
            first_page="e221234",
            last_page="e221234"
        )
        assert result == "JAMA, 327 (12). e221234"

    def test_single_page_large_last_page(self):
        """Тест одной страницы с большим last_page."""
        result = format_citation(
            journal="Cell",
            volume="",
            issue="",
            first_page="50",
            last_page="200000"
        )
        assert result == "Cell. 50"

    def test_no_pages(self):
        """Тест без страниц."""
        result = format_citation(
            journal="Science",
            volume="380",
            issue="*",
            first_page="",
            last_page=""
        )
        assert result == "Science, 380 (*)."

    def test_no_volume(self):
        """Тест без volume."""
        result = format_citation(
            journal="PLOS ONE",
            volume="",
            issue="",
            first_page="123",
            last_page="456"
        )
        assert result == "PLOS ONE. p. 123-456"

    def test_no_journal(self):
        """Тест без journal."""
        result = format_citation(
            journal="",
            volume="123",
            issue="4",
            first_page="10",
            last_page="20"
        )
        assert result == ""

    def test_nan_values(self):
        """Тест с NaN значениями."""
        result = format_citation(
            journal="Test Journal",
            volume=pd.NA,
            issue=pd.NA,
            first_page=pd.NA,
            last_page=pd.NA
        )
        assert result == "Test Journal."

    def test_whitespace_handling(self):
        """Тест обработки пробелов."""
        result = format_citation(
            journal="  Nature  ",
            volume=" 612 ",
            issue=" 7940 ",
            first_page=" 100 ",
            last_page=" 105 "
        )
        assert result == "Nature, 612 (7940). p. 100-105"

    def test_string_numbers(self):
        """Тест со строковыми числами."""
        result = format_citation(
            journal="Journal",
            volume="123",
            issue="4",
            first_page="10",
            last_page="20"
        )
        assert result == "Journal, 123 (4). p. 10-20"

    def test_first_page_missing_last_page_present(self):
        """Тест когда first_page отсутствует, а last_page есть."""
        result = format_citation(
            journal="Journal",
            volume="1",
            issue="",
            first_page="",
            last_page="50"
        )
        assert result == "Journal, 1. 50"

    def test_both_pages_missing(self):
        """Тест когда обе страницы отсутствуют."""
        result = format_citation(
            journal="Journal",
            volume="1",
            issue="",
            first_page="",
            last_page=""
        )
        assert result == "Journal, 1."

    def test_first_page_non_numeric_last_page_numeric(self):
        """Тест когда first_page нечисловой, а last_page числовой."""
        result = format_citation(
            journal="Journal",
            volume="1",
            issue="",
            first_page="abc",
            last_page="50"
        )
        assert result == "Journal, 1. abc"

    def test_edge_case_equal_pages(self):
        """Тест граничного случая - равные страницы."""
        result = format_citation(
            journal="Journal",
            volume="1",
            issue="",
            first_page="100",
            last_page="100"
        )
        assert result == "Journal, 1. 100"

    def test_edge_case_last_page_99999(self):
        """Тест граничного случая - last_page = 99999."""
        result = format_citation(
            journal="Journal",
            volume="1",
            issue="",
            first_page="100",
            last_page="99999"
        )
        assert result == "Journal, 1. p. 100-99999"

    def test_edge_case_last_page_100000(self):
        """Тест граничного случая - last_page = 100000."""
        result = format_citation(
            journal="Journal",
            volume="1",
            issue="",
            first_page="100",
            last_page="100000"
        )
        assert result == "Journal, 1. 100"

    def test_double_spaces_removal(self):
        """Тест удаления двойных пробелов."""
        result = format_citation(
            journal="Journal  Name",
            volume="1",
            issue="",
            first_page="100",
            last_page="200"
        )
        assert result == "Journal Name, 1. p. 100-200"

    def test_trailing_punctuation_removal(self):
        """Тест удаления висячих знаков препинания."""
        result = format_citation(
            journal="Journal.",
            volume="1",
            issue="",
            first_page="100",
            last_page="200"
        )
        assert result == "Journal., 1. p. 100-200"


class TestAddCitationColumn:
    """Тесты для функции add_citation_column."""

    def test_add_citation_column_basic(self):
        """Тест добавления колонки с цитатами."""
        df = pd.DataFrame({
            'journal': ['Nature', 'Science', 'Cell'],
            'volume': ['612', '380', ''],
            'issue': ['7940', '', ''],
            'first_page': ['100', '50', ''],
            'last_page': ['105', '200000', '']
        })
        
        result = add_citation_column(df)
        
        assert 'document_citation' in result.columns
        assert result['document_citation'].iloc[0] == "Nature, 612 (7940). p. 100-105"
        assert result['document_citation'].iloc[1] == "Science, 380. 50"
        assert result['document_citation'].iloc[2] == "Cell."

    def test_add_citation_column_preserves_original_data(self):
        """Тест что оригинальные данные сохраняются."""
        df = pd.DataFrame({
            'journal': ['Nature'],
            'volume': ['612'],
            'issue': ['7940'],
            'first_page': ['100'],
            'last_page': ['105'],
            'other_column': ['test']
        })
        
        result = add_citation_column(df)
        
        assert len(result.columns) == 6  # 5 оригинальных + 1 новая
        assert result['other_column'].iloc[0] == 'test'
        assert result['journal'].iloc[0] == 'Nature'

    def test_add_citation_column_empty_dataframe(self):
        """Тест с пустым DataFrame."""
        df = pd.DataFrame(columns=['journal', 'volume', 'issue', 'first_page', 'last_page'])
        
        result = add_citation_column(df)
        
        assert 'document_citation' in result.columns
        assert len(result) == 0

    def test_add_citation_column_missing_columns(self):
        """Тест с отсутствующими колонками."""
        df = pd.DataFrame({
            'journal': ['Nature'],
            'volume': ['612']
        })
        
        result = add_citation_column(df)
        
        assert 'document_citation' in result.columns
        assert result['document_citation'].iloc[0] == "Nature, 612."


if __name__ == "__main__":
    pytest.main([__file__])
