#!/usr/bin/env python3
"""Демонстрационный скрипт для тестирования нормализации DOI."""

import pandas as pd
from library.io_.normalize import normalize_doi_advanced
from library.etl.load import _normalize_dataframe


def demo_doi_normalization():
    """Демонстрация нормализации DOI."""
    
    print("=== Демонстрация нормализации DOI ===\n")
    
    # Тестовые примеры DOI из требований
    test_cases = [
        (" DOI:10.1000/XYZ-123 ", "10.1000/xyz-123"),
        ("https://doi.org/10.1000/xyz-123 ", "10.1000/xyz-123"),
        ("10.1000/xyz%2D123", "10.1000/xyz-123"),
        ("10.1000/xyz-123.", "10.1000/xyz-123"),
        ("URN:DOI:10.5555/  A B C ", "10.5555/abc"),
        ("https://dx.doi.org/10.1038/ABC.1", "10.1038/abc.1"),
        ("info:doi/10.1038/ABC.1///", "10.1038/abc.1"),
    ]
    
    print("1. Нормализация отдельных DOI:")
    print("-" * 60)
    for input_doi, expected in test_cases:
        result = normalize_doi_advanced(input_doi)
        status = "OK" if result == expected else "FAIL"
        print(f"{status} '{input_doi}' -> '{result}'")
        if result != expected:
            print(f"   Ожидалось: '{expected}'")
    print()
    
    # Демонстрация работы с DataFrame
    print("2. Нормализация DOI-столбцов в DataFrame:")
    print("-" * 60)
    
    test_data = {
        'index': [0, 1, 2, 3, 4],
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
        'regular_column': [
            "Regular text",
            "Another text",
            "Third text",
            "Fourth text",
            "Fifth text"
        ]
    }
    
    df = pd.DataFrame(test_data)
    print("Исходные данные:")
    print(df.to_string(index=False))
    print()
    
    # Нормализуем данные
    normalized_df = _normalize_dataframe(df)
    
    print("После нормализации:")
    print(normalized_df.to_string(index=False))
    print()
    
    # Проверяем результаты
    print("3. Проверка результатов:")
    print("-" * 60)
    
    expected_doi = ["10.1000/xyz-123", "10.1000/xyz-456", "10.1000/test-789", pd.NA, pd.NA]
    expected_document_doi = ["10.5555/abc", "10.1038/def.1", "10.1038/ghi.2", pd.NA, "10.1000/valid"]
    
    doi_correct = normalized_df['doi'].tolist() == expected_doi
    document_doi_correct = normalized_df['document_doi'].tolist() == expected_document_doi
    
    print(f"DOI-столбец нормализован корректно: {'OK' if doi_correct else 'FAIL'}")
    print(f"Document_DOI-столбец нормализован корректно: {'OK' if document_doi_correct else 'FAIL'}")
    print(f"Индексный столбец не изменился: {'OK' if normalized_df['index'].tolist() == [0, 1, 2, 3, 4] else 'FAIL'}")
    print(f"Обычный столбец приведен к нижнему регистру: {'OK' if normalized_df['regular_column'].iloc[0] == 'regular text' else 'FAIL'}")
    
    print("\n=== Демонстрация завершена ===")


if __name__ == "__main__":
    demo_doi_normalization()
