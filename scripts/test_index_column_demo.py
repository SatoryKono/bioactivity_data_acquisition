"""Демонстрационный скрипт для проверки добавления столбца index."""

import sys
from pathlib import Path

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
from library.etl.load import write_deterministic_csv
from library.config import DeterminismSettings, OutputSettings, CsvFormatSettings


def main():
    """Демонстрация добавления столбца index."""
    
    # Создаем тестовые данные
    df = pd.DataFrame({
        'compound_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3', 'CHEMBL4'],
        'target': ['TARGET1', 'TARGET2', 'TARGET1', 'TARGET3'],
        'activity_value': [5.2, 7.1, 3.8, 9.4],
        'activity_type': ['IC50', 'EC50', 'IC50', 'Ki'],
        'reference': ['PMID123', 'PMID456', 'PMID789', 'PMID012']
    })
    
    print("Исходные данные:")
    print(df)
    print(f"\nКоличество строк: {len(df)}")
    print(f"Столбцы: {list(df.columns)}")
    
    # Настройки
    determinism = DeterminismSettings()
    output = OutputSettings(
        data_path=Path("demo_output.csv"),
        qc_report_path=Path("demo_qc.csv"),
        correlation_path=Path("demo_corr.csv"),
        format="csv",
        csv=CsvFormatSettings()
    )
    
    # Создаем выходной файл
    output_path = Path("data/output") / "demo_with_index.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Записываем данные с добавлением столбца index
    write_deterministic_csv(
        df,
        output_path,
        determinism=determinism,
        output=output
    )
    
    # Читаем обратно и показываем результат
    result_df = pd.read_csv(output_path)
    
    print("\nРезультат после добавления столбца index:")
    print(result_df)
    print(f"\nКоличество строк: {len(result_df)}")
    print(f"Столбцы: {list(result_df.columns)}")
    print(f"Значения index: {result_df['index'].tolist()}")
    
    # Проверяем, что index является первым столбцом
    assert result_df.columns[0] == 'index', f"Первым столбцом должен быть 'index', а получили '{result_df.columns[0]}'"
    
    # Проверяем, что значения index корректны
    expected_index = list(range(len(df)))
    assert result_df['index'].tolist() == expected_index, f"Index должен быть {expected_index}, а получили {result_df['index'].tolist()}"
    
    print("\nДемонстрация прошла успешно!")
    print(f"Файл сохранен: {output_path}")


if __name__ == "__main__":
    main()
