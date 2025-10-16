#!/usr/bin/env python3
"""Диагностика реальных данных для валидации DOI."""

import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'library', 'tools'))

from data_validator import validate_doi_fields


def debug_real_data():
    """Диагностика реальных данных."""
    
    print("=== ДИАГНОСТИКА РЕАЛЬНЫХ ДАННЫХ ===")
    print()
    
    # Проверяем, есть ли файл с данными
    data_file = "data/input/documents.csv"
    if os.path.exists(data_file):
        print(f"1. Читаем данные из {data_file}")
        try:
            df = pd.read_csv(data_file)
            print(f"   Размер данных: {df.shape}")
            print()
            
            print("2. Колонки в данных:")
            for col in df.columns:
                print(f"   {col}")
            print()
            
            # Ищем колонки DOI
            doi_columns = [col for col in df.columns if 'doi' in col.lower()]
            print("3. Колонки DOI:")
            for col in doi_columns:
                print(f"   {col}: {df[col].dtype}")
                non_null_count = df[col].notna().sum()
                print(f"      Непустых значений: {non_null_count}")
            print()
            
            # Проверяем первые несколько строк
            print("4. Первые 3 строки данных:")
            print(df.head(3))
            print()
            
            # Пробуем валидировать
            print("5. Попытка валидации:")
            try:
                result = validate_doi_fields(df)
                print("   Валидация прошла успешно!")
                
                if 'invalid_doi' in result.columns:
                    invalid_count = result['invalid_doi'].sum()
                    print(f"   Количество невалидных DOI: {invalid_count}")
                    print(f"   Общее количество записей: {len(result)}")
                else:
                    print("   Колонка invalid_doi не найдена!")
                    
            except Exception as e:
                print(f"   ОШИБКА при валидации: {e}")
                import traceback
                traceback.print_exc()
                
        except Exception as e:
            print(f"   ОШИБКА при чтении файла: {e}")
    else:
        print(f"1. Файл {data_file} не найден!")
        
        # Проверяем другие возможные файлы
        possible_files = [
            "data/output/full/documents_*.csv",
            "document.csv",
            "tests/test_outputs/test_final_output.csv"
        ]
        
        print("2. Ищем другие файлы с данными:")
        for pattern in possible_files:
            import glob
            files = glob.glob(pattern)
            if files:
                print(f"   Найдены файлы: {files}")
                for file in files[:1]:  # Берем только первый файл
                    try:
                        df = pd.read_csv(file)
                        print(f"   Файл: {file}")
                        print(f"   Размер: {df.shape}")
                        
                        # Ищем колонки DOI
                        doi_columns = [col for col in df.columns if 'doi' in col.lower()]
                        print(f"   Колонки DOI: {doi_columns}")
                        
                        if doi_columns:
                            print("   Первые 3 строки:")
                            print(df[doi_columns].head(3))
                            
                    except Exception as e:
                        print(f"   Ошибка при чтении {file}: {e}")
            else:
                print(f"   Файлы по шаблону {pattern} не найдены")

if __name__ == "__main__":
    debug_real_data()
