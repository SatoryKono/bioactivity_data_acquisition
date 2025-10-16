#!/usr/bin/env python3
"""Диагностика импорта в пайплайне."""

import sys
import os

def debug_pipeline_import():
    """Диагностика импорта в пайплайне."""
    
    print("=== ДИАГНОСТИКА ИМПОРТА В ПАЙПЛАЙНЕ ===")
    print()
    
    # Проверяем пути
    print("1. Проверка путей:")
    print(f"   Текущая директория: {os.getcwd()}")
    print(f"   Python path: {sys.path[:3]}...")
    print()
    
    # Добавляем путь к src
    src_path = os.path.join(os.path.dirname(__file__), 'src')
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
        print(f"2. Добавлен путь: {src_path}")
    else:
        print(f"2. Путь уже есть: {src_path}")
    print()
    
    # Проверяем, есть ли файл валидатора
    validator_file = os.path.join(src_path, 'library', 'tools', 'data_validator.py')
    print("3. Проверка файла валидатора:")
    if os.path.exists(validator_file):
        print(f"   Файл найден: {validator_file}")
    else:
        print(f"   Файл НЕ найден: {validator_file}")
        return
    
    # Проверяем, есть ли файл __init__.py
    init_file = os.path.join(src_path, 'library', 'tools', '__init__.py')
    print("4. Проверка файла __init__.py:")
    if os.path.exists(init_file):
        print(f"   Файл найден: {init_file}")
    else:
        print(f"   Файл НЕ найден: {init_file}")
        return
    
    # Проверяем импорт
    print("5. Проверка импорта:")
    try:
        print("   Импорт validate_all_fields: OK")
    except Exception as e:
        print(f"   Ошибка импорта validate_all_fields: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Проверяем импорт пайплайна
    print("6. Проверка импорта пайплайна:")
    try:
        print("   Импорт write_document_outputs: OK")
    except Exception as e:
        print(f"   Ошибка импорта write_document_outputs: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print()
    print("7. Все импорты работают правильно!")

if __name__ == "__main__":
    debug_pipeline_import()
