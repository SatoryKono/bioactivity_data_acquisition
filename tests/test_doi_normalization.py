"""Тесты для нормализации DOI."""

import pandas as pd
from library.io_.normalize import normalize_doi_advanced


class TestDOINormalization:
    """Тесты для функции normalize_doi_advanced."""

    def test_basic_normalization(self):
        """Тест базовой нормализации DOI."""
        # Тестовые случаи из требований
        test_cases = [
            (" DOI:10.1000/XYZ-123 ", "10.1000/xyz-123"),
            ("https://doi.org/10.1000/xyz-123 ", "10.1000/xyz-123"),
            ("10.1000/xyz%2D123", "10.1000/xyz-123"),
            ("10.1000/xyz-123.", "10.1000/xyz-123"),
            ("URN:DOI:10.5555/  A B C ", "10.5555/abc"),
            ("https://dx.doi.org/10.1038/ABC.1", "10.1038/abc.1"),
        ]
        
        for input_doi, expected in test_cases:
            result = normalize_doi_advanced(input_doi)
            assert result == expected, f"Failed for input: {input_doi}"

    def test_edge_cases(self):
        """Тест граничных случаев."""
        # None и пустые значения
        assert normalize_doi_advanced(None) is None
        assert normalize_doi_advanced("") is None
        assert normalize_doi_advanced("   ") is None
        
        # Невалидные DOI
        assert normalize_doi_advanced("invalid-doi") is None
        assert normalize_doi_advanced("not-a-doi") is None
        assert normalize_doi_advanced("10.1000") is None  # Нет слэша
        assert normalize_doi_advanced("10.1000/") is None  # Пустой суффикс

    def test_prefix_removal(self):
        """Тест удаления различных префиксов."""
        test_cases = [
            ("doi:10.1000/test", "10.1000/test"),
            ("DOI:10.1000/test", "10.1000/test"),
            ("urn:doi:10.1000/test", "10.1000/test"),
            ("URN:DOI:10.1000/test", "10.1000/test"),
            ("info:doi/10.1000/test", "10.1000/test"),
            ("http://doi.org/10.1000/test", "10.1000/test"),
            ("https://doi.org/10.1000/test", "10.1000/test"),
            ("http://dx.doi.org/10.1000/test", "10.1000/test"),
            ("https://dx.doi.org/10.1000/test", "10.1000/test"),
        ]
        
        for input_doi, expected in test_cases:
            result = normalize_doi_advanced(input_doi)
            assert result == expected, f"Failed for input: {input_doi}"

    def test_percent_encoding(self):
        """Тест декодирования percent-encoding."""
        test_cases = [
            ("10.1000/xyz%2D123", "10.1000/xyz-123"),
            ("10.1000/xyz%2E123", "10.1000/xyz.123"),
            ("10.1000/xyz%20test", "10.1000/xyztest"),
            ("10.1000/xyz%2Ftest", "10.1000/xyz/test"),
        ]
        
        for input_doi, expected in test_cases:
            result = normalize_doi_advanced(input_doi)
            assert result == expected, f"Failed for input: {input_doi}"

    def test_whitespace_handling(self):
        """Тест обработки пробелов."""
        test_cases = [
            ("10.1000/  xyz  ", "10.1000/xyz"),
            ("10.1000 / xyz", "10.1000/xyz"),
            ("10.1000/  A B C  ", "10.1000/abc"),
            ("10.1000/  test  123  ", "10.1000/test123"),
        ]
        
        for input_doi, expected in test_cases:
            result = normalize_doi_advanced(input_doi)
            assert result == expected, f"Failed for input: {input_doi}"

    def test_trailing_punctuation(self):
        """Тест удаления хвостовой пунктуации."""
        test_cases = [
            ("10.1000/test.", "10.1000/test"),
            ("10.1000/test,", "10.1000/test"),
            ("10.1000/test;", "10.1000/test"),
            ("10.1000/test)", "10.1000/test"),
            ("10.1000/test]", "10.1000/test"),
            ("10.1000/test}", "10.1000/test"),
            ("10.1000/test'", "10.1000/test"),
            ('10.1000/test"', "10.1000/test"),
            ("10.1000/test.,;)]}", "10.1000/test"),
        ]
        
        for input_doi, expected in test_cases:
            result = normalize_doi_advanced(input_doi)
            assert result == expected, f"Failed for input: {input_doi}"

    def test_multiple_slashes(self):
        """Тест обработки множественных слэшей."""
        test_cases = [
            ("10.1000//test", "10.1000/test"),
            ("10.1000///test", "10.1000/test"),
            ("10.1000////test", "10.1000/test"),
        ]
        
        for input_doi, expected in test_cases:
            result = normalize_doi_advanced(input_doi)
            assert result == expected, f"Failed for input: {input_doi}"

    def test_case_normalization(self):
        """Тест приведения к нижнему регистру."""
        test_cases = [
            ("10.1000/XYZ-123", "10.1000/xyz-123"),
            ("10.1000/TestDOI", "10.1000/testdoi"),
            ("DOI:10.1000/ABC", "10.1000/abc"),
        ]
        
        for input_doi, expected in test_cases:
            result = normalize_doi_advanced(input_doi)
            assert result == expected, f"Failed for input: {input_doi}"

    def test_unicode_normalization(self):
        """Тест нормализации Unicode."""
        # Тест NFC нормализации
        result = normalize_doi_advanced("10.1000/test")
        assert result == "10.1000/test"

    def test_complex_combinations(self):
        """Тест сложных комбинаций различных преобразований."""
        test_cases = [
            (" DOI: https://dx.doi.org/10.1000/  XYZ-123%2D456 .", "10.1000/xyz-123-456"),
            ("URN:DOI:10.5555/  A B C %20test  ,", "10.5555/abctest"),
            ("info:doi/10.1038/ABC.1///", "10.1038/abc.1"),
        ]
        
        for input_doi, expected in test_cases:
            result = normalize_doi_advanced(input_doi)
            assert result == expected, f"Failed for input: {input_doi}"

    def test_idempotency(self):
        """Тест идемпотентности - повторное применение не должно изменять результат."""
        test_cases = [
            "10.1000/xyz-123",
            "10.1000/test",
            "10.1038/abc.1",
        ]
        
        for doi in test_cases:
            # Первое применение
            first_result = normalize_doi_advanced(doi)
            # Второе применение
            second_result = normalize_doi_advanced(first_result)
            assert first_result == second_result, f"Failed idempotency for: {doi}"

    def test_different_dois_remain_different(self):
        """Тест, что разные DOI остаются разными после нормализации."""
        test_pairs = [
            ("10.1000/xyz-123", "10.1000/xyz-456"),
            ("10.1000/test", "10.1001/test"),
            ("10.1038/abc.1", "10.1038/abc.2"),
        ]
        
        for doi1, doi2 in test_pairs:
            norm1 = normalize_doi_advanced(doi1)
            norm2 = normalize_doi_advanced(doi2)
            assert norm1 != norm2, f"DOIs should remain different: {doi1} vs {doi2}"


class TestDOINormalizationIntegration:
    """Тесты интеграции нормализации DOI в DataFrame."""

    def test_dataframe_doi_normalization(self):
        """Тест нормализации DOI-столбцов в DataFrame."""
        from library.etl.load import _normalize_dataframe
        
        # Создаем тестовый DataFrame с DOI-столбцами
        test_data = {
            'doi': [
                " DOI:10.1000/XYZ-123 ",
                "https://doi.org/10.1000/xyz-456",
                "10.1000/test%2D789",
                None,
                ""
            ],
            'document_doi': [
                "URN:DOI:10.5555/ABC",
                "https://dx.doi.org/10.1038/DEF.1",
                "10.1038/ghi.2.",
                "invalid-doi",
                "10.1000/valid"
            ],
            'other_column': [
                "regular text",
                "another text",
                "third text",
                "fourth text",
                "fifth text"
            ]
        }
        
        df = pd.DataFrame(test_data)
        normalized_df = _normalize_dataframe(df)
        
        # Проверяем, что DOI-столбцы нормализованы
        expected_doi = [
            "10.1000/xyz-123",
            "10.1000/xyz-456", 
            "10.1000/test-789",
            pd.NA,  # None становится pd.NA после замены
            pd.NA   # пустая строка становится pd.NA после замены
        ]
        
        expected_document_doi = [
            "10.5555/abc",
            "10.1038/def.1",
            "10.1038/ghi.2",
            pd.NA,  # невалидный DOI должен стать pd.NA
            "10.1000/valid"
        ]
        
        assert normalized_df['doi'].tolist() == expected_doi
        assert normalized_df['document_doi'].tolist() == expected_document_doi
        
        # Проверяем, что обычные столбцы нормализованы как обычно (в нижний регистр)
        assert normalized_df['other_column'].tolist() == [
            "regular text",
            "another text", 
            "third text",
            "fourth text",
            "fifth text"
        ]

    def test_case_insensitive_doi_column_detection(self):
        """Тест определения DOI-столбцов без учета регистра."""
        from library.etl.load import _normalize_dataframe
        
        test_data = {
            'DOI': ["10.1000/test1"],
            'Document_DOI': ["10.1000/test2"], 
            'doi_key': ["10.1000/test3"],
            'some_doi_column': ["10.1000/test4"],
            'regular_column': ["not a doi column"]
        }
        
        df = pd.DataFrame(test_data)
        normalized_df = _normalize_dataframe(df)
        
        # Все DOI-столбцы должны быть нормализованы
        assert normalized_df['DOI'].iloc[0] == "10.1000/test1"
        assert normalized_df['Document_DOI'].iloc[0] == "10.1000/test2"
        assert normalized_df['doi_key'].iloc[0] == "10.1000/test3"
        assert normalized_df['some_doi_column'].iloc[0] == "10.1000/test4"
        
        # Обычный столбец должен быть нормализован как обычно (в нижний регистр)
        assert normalized_df['regular_column'].iloc[0] == "not a doi column"

    def test_index_column_preservation(self):
        """Тест, что индексный столбец не нормализуется."""
        from library.etl.load import _normalize_dataframe
        
        test_data = {
            'index': [0, 1, 2, 3, 4],
            'doi': ["10.1000/test1", "10.1000/test2", "10.1000/test3", "10.1000/test4", "10.1000/test5"]
        }
        
        df = pd.DataFrame(test_data)
        normalized_df = _normalize_dataframe(df)
        
        # Индексный столбец должен остаться неизменным
        assert normalized_df['index'].tolist() == [0, 1, 2, 3, 4]
        # DOI-столбцы должны быть нормализованы
        assert normalized_df['doi'].tolist() == ["10.1000/test1", "10.1000/test2", "10.1000/test3", "10.1000/test4", "10.1000/test5"]
