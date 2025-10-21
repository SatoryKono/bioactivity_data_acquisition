#!/usr/bin/env python3
"""Тесты для функции подготовки данных к корреляционному анализу."""

import pandas as pd
import pytest

from library.etl.enhanced_correlation import prepare_data_for_correlation_analysis


def test_prepare_data_for_documents():
    """Тест подготовки данных для документов."""
    # Создаем тестовые данные для документов
    test_data = pd.DataFrame({
        'document_chembl_id': ['CHEMBL123', 'CHEMBL456'],
        'pubmed_pmid': [12345, 67890],
        'pubmed_year_completed': [2023, 2022],
        'pubmed_month_completed': [5, 6],
        'crossref_error': ['Error 1', 'Error 2'],  # Должна быть удалена
        'retrieved_at': ['2023-01-01', '2023-01-02'],  # Должна быть удалена
        'publication_date': ['2023-05-01', '2022-06-01'],
        'document_sortorder': ['123:2023-05-01:000000', '456:2022-06-01:000001']
    })
    
    result = prepare_data_for_correlation_analysis(test_data, data_type="documents")
    
    # Проверяем, что колонки с ошибками и технические колонки удалены
    assert 'crossref_error' not in result.columns
    assert 'retrieved_at' not in result.columns
    
    # Проверяем, что основные колонки остались
    assert 'document_chembl_id' in result.columns
    assert 'pubmed_pmid' in result.columns
    assert 'pubmed_year_completed' in result.columns
    assert 'publication_date' in result.columns
    
    # Проверяем количество строк
    assert len(result) == 2


def test_prepare_data_for_assays():
    """Тест подготовки данных для ассеев."""
    # Создаем тестовые данные для ассеев
    test_data = pd.DataFrame({
        'assay_chembl_id': ['CHEMBL123', 'CHEMBL456'],
        'assay_type': ['B', 'F'],
        'relationship_type': ['D', 'E'],  # Разные значения для создания _numeric колонки
        'assay_organism': ['Homo sapiens', 'Mus musculus'],
        'description': ['Test assay 1', 'Test assay 2 with different length'],
        'confidence_score': [8, 7],
        'hash_row': ['abc12345', 'def67890'],
        'assay_error': ['Error 1', 'Error 2']  # Должна быть удалена
    })
    
    result = prepare_data_for_correlation_analysis(test_data, data_type="assays")
    
    # Проверяем, что колонки с ошибками удалены
    assert 'assay_error' not in result.columns
    
    # Проверяем, что категориальные колонки конвертированы в числовые
    assert 'assay_type_numeric' in result.columns
    assert 'relationship_type_numeric' in result.columns
    assert 'assay_organism_numeric' in result.columns
    
    # Проверяем, что добавлены дополнительные признаки
    assert 'description_length' in result.columns
    assert 'hash_numeric' in result.columns
    
    # Проверяем, что остались только числовые колонки
    assert all(result.dtypes.apply(lambda x: pd.api.types.is_numeric_dtype(x)))
    
    # Проверяем количество строк
    assert len(result) == 2


def test_prepare_data_for_general():
    """Тест подготовки данных для общего случая."""
    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'id': [1, 2, 3],
        'value1': [10, 20, 30],
        'value2': [100, 200, 300],
        'error_col': ['Error 1', 'Error 2', 'Error 3'],  # НЕ удаляется в общем случае
        'constant_col': [1, 1, 1],  # Должна быть удалена (константная)
        'missing_col': [1, None, None]  # Должна быть удалена (>50% пропущенных)
    })
    
    result = prepare_data_for_correlation_analysis(test_data, data_type="general")
    
    # Проверяем, что проблемные колонки удалены
    assert 'constant_col' not in result.columns
    assert 'missing_col' not in result.columns
    
    # Проверяем, что основные колонки остались
    assert 'id' in result.columns
    assert 'value1' in result.columns
    assert 'value2' in result.columns
    assert 'error_col' in result.columns  # Остается в общем случае
    
    # Проверяем количество строк
    assert len(result) == 3


def test_prepare_data_with_logger():
    """Тест подготовки данных с логгером."""
    import logging
    
    # Создаем простой логгер
    logger = logging.getLogger('test')
    logger.setLevel(logging.INFO)
    
    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'id': [1, 2],
        'value': [10, 20],
        'error_col': ['Error 1', 'Error 2']
    })
    
    # Функция должна работать без ошибок с логгером
    result = prepare_data_for_correlation_analysis(test_data, data_type="general", logger=logger)
    
    # Проверяем результат (в общем случае error_col остается)
    assert 'error_col' in result.columns
    assert 'id' in result.columns
    assert 'value' in result.columns
    assert len(result) == 2


def test_prepare_data_empty_dataframe():
    """Тест подготовки пустого DataFrame."""
    test_data = pd.DataFrame()
    
    result = prepare_data_for_correlation_analysis(test_data, data_type="general")
    
    # Результат должен быть пустым DataFrame
    assert len(result) == 0
    assert len(result.columns) == 0


def test_prepare_data_single_row():
    """Тест подготовки DataFrame с одной строкой."""
    test_data = pd.DataFrame({
        'id': [1],
        'value': [10],
        'constant_col': [1]  # Должна быть удалена (константная)
    })
    
    result = prepare_data_for_correlation_analysis(test_data, data_type="general")
    
    # Проверяем, что константная колонка удалена
    assert 'constant_col' not in result.columns
    
    # Проверяем, что основные колонки остались (если они не константные)
    # В данном случае все колонки с одной строкой считаются константными
    # Поэтому результат может быть пустым
    if len(result.columns) > 0:
        assert 'id' in result.columns or 'value' in result.columns
    
    # Проверяем количество строк
    assert len(result) == 1


if __name__ == "__main__":
    pytest.main([__file__])
