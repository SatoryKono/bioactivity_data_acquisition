"""Простой демонстрационный скрипт для нормализации данных."""

import sys
from pathlib import Path

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import numpy as np
from library.etl.load import _normalize_dataframe


def main():
    """Простая демонстрация нормализации данных."""
    
    print("=== Простая демонстрация нормализации данных ===\n")
    
    # Создаем тестовые данные
    df = pd.DataFrame({
        'compound_id': ['  CHEMBL1  ', 'CHEMBL2', 'chembl3', '', '   ', None],
        'target': ['TARGET1', '  target2  ', 'TARGET3', 'nan', 'NONE', ''],
        'activity_value': [5.2, np.nan, 7.1, 9.4, np.inf, -np.inf],
        'is_active': [True, False, np.nan, True, False, True],
        'reference': ['PMID123', '  PMID456  ', 'PMID789', '', None, 'PMID012']
    })
    
    print("Исходные данные:")
    print(df)
    
    # Добавляем столбец index
    df_with_index = df.copy()
    df_with_index.insert(0, 'index', range(len(df_with_index)))
    
    print("\nДанные с добавленным index:")
    print(df_with_index)
    
    # Нормализуем данные
    normalized_df = _normalize_dataframe(df_with_index)
    
    print("\nРезультат после нормализации:")
    print(normalized_df)
    
    print("\n=== Анализ нормализации ===")
    
    # Анализируем строковые колонки
    for col in ['compound_id', 'target', 'reference']:
        print(f"\n{col.upper()}:")
        for i in range(len(df)):
            original = df[col].iloc[i]
            normalized = normalized_df[col].iloc[i]
            print(f"  '{original}' -> '{normalized}'")
    
    # Анализируем числовые данные
    print(f"\nACTIVITY_VALUE:")
    for i in range(len(df)):
        original = df['activity_value'].iloc[i]
        normalized = normalized_df['activity_value'].iloc[i]
        print(f"  {original} -> {normalized}")
    
    # Анализируем логические данные
    print(f"\nIS_ACTIVE:")
    for i in range(len(df)):
        original = df['is_active'].iloc[i]
        normalized = normalized_df['is_active'].iloc[i]
        print(f"  {original} -> {normalized}")
    
    print("\n=== Ключевые особенности ===")
    print("- Строковые данные приведены к нижнему регистру")
    print("- Концевые пробелы обрезаны")
    print("- Пустые строки заменены на NA")
    print("- NaN, inf, -inf заменены на NA")
    print("- Столбец index не нормализуется")
    
    print("\nДемонстрация завершена успешно!")


if __name__ == "__main__":
    main()
