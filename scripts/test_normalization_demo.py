"""Демонстрационный скрипт для проверки нормализации данных."""

import sys
from pathlib import Path

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import numpy as np
from library.etl.load import write_deterministic_csv


def main():
    """Демонстрация нормализации данных."""
    
    print("=== Демонстрация нормализации данных ===\n")
    
    # Создаем тестовые данные с различными проблемами
    df = pd.DataFrame({
        'compound_id': ['  CHEMBL1  ', 'CHEMBL2', 'chembl3', '', '   ', None],
        'target': ['TARGET1', '  target2  ', 'TARGET3', 'nan', 'NONE', ''],
        'activity_value': [5.2, np.nan, 7.1, 9.4, np.inf, -np.inf],
        'is_active': [True, False, np.nan, True, False, True],
        'reference': ['PMID123', '  PMID456  ', 'PMID789', '', None, 'PMID012'],
        'notes': ['  Important compound  ', 'Test data', '  NORMAL  ', '', '   ', 'Regular text']
    })
    
    print("Исходные данные (до нормализации):")
    print(df)
    print(f"\nТипы данных:")
    print(df.dtypes)
    
    # Настройки для сохранения
    output_path = Path("data/output") / "demo_normalized_data.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Записываем данные с нормализацией
    write_deterministic_csv(
        df,
        output_path,
        determinism=None,
        output=None
    )
    
    # Читаем обратно и показываем результат
    result_df = pd.read_csv(output_path)
    
    print("\n" + "="*60)
    print("Результат после нормализации:")
    print("="*60)
    print(result_df)
    print(f"\nТипы данных после нормализации:")
    print(result_df.dtypes)
    
    print("\n" + "="*60)
    print("Детальный анализ нормализации:")
    print("="*60)
    
    # Анализируем каждую колонку
    for col in ['compound_id', 'target', 'reference', 'notes']:
        print(f"\n{col.upper()}:")
        for i in range(len(result_df)):
            original = df[col].iloc[i]
            normalized = result_df[col].iloc[i]
            print(f"  Строка {i}: '{original}' -> '{normalized}'")
    
    print(f"\nЧисловые данные (activity_value):")
    for i in range(len(result_df)):
        original = df['activity_value'].iloc[i]
        normalized = result_df['activity_value'].iloc[i]
        print(f"  Строка {i}: {original} -> {normalized}")
    
    print(f"\nЛогические данные (is_active):")
    for i in range(len(result_df)):
        original = df['is_active'].iloc[i]
        normalized = result_df['is_active'].iloc[i]
        print(f"  Строка {i}: {original} -> {normalized}")
    
    print("\n" + "="*60)
    print("Ключевые особенности нормализации:")
    print("="*60)
    print("Строковые данные приведены к нижнему регистру")
    print("Концевые пробелы обрезаны")
    print("Пустые строки и специальные значения заменены на NA")
    print("NaN, inf, -inf в числовых данных заменены на NA")
    print("NaN в логических данных заменены на NA")
    print("Столбец index добавлен и не нормализуется")
    print("Столбец index размещен первым")
    
    print(f"\nФайл сохранен: {output_path}")
    print("\nДемонстрация завершена успешно!")


if __name__ == "__main__":
    main()
