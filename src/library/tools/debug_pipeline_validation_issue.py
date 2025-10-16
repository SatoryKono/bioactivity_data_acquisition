#!/usr/bin/env python3
"""Диагностика проблемы с валидацией в пайплайне."""

import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def debug_pipeline_validation_issue():
    """Диагностика проблемы с валидацией в пайплайне."""
    
    print("=== ДИАГНОСТИКА ПРОБЛЕМЫ С ВАЛИДАЦИЕЙ В ПАЙПЛАЙНЕ ===")
    print()
    
    # Проверяем, есть ли файл с данными
    data_file = "data/input/documents.csv"
    if not os.path.exists(data_file):
        print(f"1. Файл {data_file} не найден!")
        return
    
    print(f"1. Файл {data_file} найден")
    
    # Читаем данные
    try:
        df = pd.read_csv(data_file)
        print(f"2. Данные прочитаны: {df.shape}")
        print(f"   Колонки: {list(df.columns)}")
    except Exception as e:
        print(f"2. Ошибка при чтении данных: {e}")
        return
    
    # Проверяем, есть ли колонки DOI
    doi_columns = [col for col in df.columns if 'doi' in col.lower()]
    print(f"3. Колонки DOI в исходных данных: {doi_columns}")
    
    # Проверяем, есть ли данные в колонках DOI
    for col in doi_columns:
        non_null_count = df[col].notna().sum()
        print(f"   {col}: {non_null_count} непустых значений")
    
    print()
    
    # Проверяем, есть ли выходные файлы
    output_dir = "data/output/full"
    if os.path.exists(output_dir):
        print(f"4. Выходная директория {output_dir} найдена")
        
        # Ищем файлы с результатами
        import glob
        result_files = glob.glob(os.path.join(output_dir, "documents_*.csv"))
        if result_files:
            print(f"   Найдены файлы результатов: {result_files}")
            
            # Проверяем последний файл
            latest_file = max(result_files, key=os.path.getctime)
            print(f"   Последний файл: {latest_file}")
            
            try:
                result_df = pd.read_csv(latest_file)
                print(f"   Размер результата: {result_df.shape}")
                
                # Проверяем, есть ли колонки валидации
                validation_columns = [col for col in result_df.columns if 'invalid_' in col or 'valid_' in col]
                print(f"   Колонки валидации: {validation_columns}")
                
                if 'invalid_doi' in result_df.columns:
                    invalid_count = result_df['invalid_doi'].sum()
                    total_count = len(result_df)
                    print(f"   Невалидных DOI: {invalid_count}/{total_count}")
                    
                    # Проверяем типы данных
                    print(f"   Тип invalid_doi: {result_df['invalid_doi'].dtype}")
                    print(f"   Значения invalid_doi: {result_df['invalid_doi'].tolist()}")
                    
                    # Проверяем, есть ли nan значения
                    nan_count = result_df['invalid_doi'].isna().sum()
                    print(f"   Количество nan значений: {nan_count}")
                    
                    # Проверяем, есть ли False значения
                    false_count = (~result_df['invalid_doi'].astype(bool)).sum()
                    print(f"   Количество False значений: {false_count}")
                    
                    # Проверяем, есть ли True значения
                    true_count = (result_df['invalid_doi'].astype(bool)).sum()
                    print(f"   Количество True значений: {true_count}")
                    
                else:
                    print("   Колонка invalid_doi не найдена!")
                    
            except Exception as e:
                print(f"   Ошибка при чтении результата: {e}")
        else:
            print("   Файлы результатов не найдены")
    else:
        print(f"4. Выходная директория {output_dir} не найдена")
    
    print()
    
    # Проверяем, есть ли ошибки в логах
    print("5. Проверяем возможные ошибки:")
    
    # Проверяем, есть ли проблемы с импортами
    try:
        from library.tools.data_validator import validate_all_fields
        print("   Импорт валидатора: OK")
    except Exception as e:
        print(f"   Ошибка импорта валидатора: {e}")
    
    try:
        from library.documents.pipeline import run_document_etl
        print("   Импорт пайплайна: OK")
    except Exception as e:
        print(f"   Ошибка импорта пайплайна: {e}")

if __name__ == "__main__":
    debug_pipeline_validation_issue()