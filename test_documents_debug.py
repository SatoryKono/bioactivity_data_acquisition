#!/usr/bin/env python3
"""Тестовый скрипт для диагностики пайплайна документов."""

import logging
import sys
from pathlib import Path

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent / "src"))

import pandas as pd
from library.documents.pipeline import run_document_etl
from library.documents.config import load_document_config

# Настраиваем логирование для детального вывода
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('test_documents_debug.log')
    ]
)

def main():
    print("=== ТЕСТ ПАЙПЛАЙНА ДОКУМЕНТОВ ===")
    
    # Загружаем конфигурацию
    config_path = Path("configs/config_document.yaml")
    print(f"Загружаем конфигурацию из: {config_path}")
    
    try:
        config = load_document_config(config_path)
    except Exception as e:
        print(f"Ошибка загрузки конфигурации: {e}")
        return
    
    # Устанавливаем лимит для тестирования
    config.runtime.limit = 3
    print(f"Установлен лимит: {config.runtime.limit}")
    
    # Загружаем входные данные
    input_path = Path("data/input/documents.csv")
    print(f"Загружаем данные из: {input_path}")
    
    try:
        df = pd.read_csv(input_path, encoding='utf-8')
        print(f"Загружено {len(df)} записей")
        print(f"Колонки: {list(df.columns)}")
        
        # Показываем первые несколько записей
        print("\nПервые записи:")
        try:
            print(df.head(3).to_string())
        except UnicodeEncodeError:
            print("Первые записи (без вывода из-за кодировки):")
            print(f"Количество строк: {len(df)}")
            print(f"Первая строка document_chembl_id: {df.iloc[0]['document_chembl_id']}")
        
    except Exception as e:
        print(f"Ошибка загрузки данных: {e}")
        return
    
    # Запускаем пайплайн
    print("\n=== ЗАПУСК ПАЙПЛАЙНА ===")
    try:
        result = run_document_etl(config, df)
        print(f"Пайплайн завершен успешно!")
        print(f"Обработано документов: {len(result.documents)}")
        
        # Проверяем ключевые колонки
        print("\n=== ПРОВЕРКА КЛЮЧЕВЫХ КОЛОНОК ===")
        key_columns = [
            'pubmed_mesh_descriptors', 'pubmed_mesh_qualifiers', 'pubmed_chemical_list',
            'crossref_pmid', 'crossref_abstract', 'crossref_issn',
            'pubmed_abstract', 'semantic_scholar_issn', 'semantic_scholar_journal',
            'pubmed_year'
        ]
        
        for col in key_columns:
            if col in result.documents.columns:
                non_null_count = result.documents[col].notna().sum()
                print(f"{col}: {non_null_count}/{len(result.documents)} записей заполнены")
            else:
                print(f"{col}: КОЛОНКА ОТСУТСТВУЕТ!")
        
        # Сохраняем результат для анализа
        output_path = Path("test_documents_output.csv")
        result.documents.to_csv(output_path, index=False)
        print(f"\nРезультат сохранен в: {output_path}")
        
    except Exception as e:
        print(f"Ошибка выполнения пайплайна: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
