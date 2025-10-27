#!/usr/bin/env python3
"""Тест извлечения данных из PubMed клиента."""

import sys
from pathlib import Path

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent / "src"))

import logging
from library.clients.pubmed import PubMedClient
from library.config import APIClientConfig
from library.documents.extract import extract_from_pubmed

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def test_pubmed_extraction():
    print("=== ТЕСТ ИЗВЛЕЧЕНИЯ ДАННЫХ ИЗ PUBMED ===")
    
    # Создаем конфигурацию
    config = APIClientConfig(
        name="pubmed",
        base_url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
        headers={"Accept": "application/json"},
        timeout=30.0
    )
    
    # Создаем клиент
    client = PubMedClient(config)
    
    # Тестируем с известным PMID
    test_pmids = ["6991692"]
    print(f"Тестируем PMIDs: {test_pmids}")
    
    try:
        # Используем функцию извлечения из пайплайна
        result_df = extract_from_pubmed(client, test_pmids, batch_size=200)
        print(f"Результат извлечения: {len(result_df)} записей")
        print(f"Колонки: {list(result_df.columns)}")
        
        if not result_df.empty:
            # Проверяем ключевые поля
            key_fields = [
                'pubmed_mesh_descriptors', 'pubmed_mesh_qualifiers', 'pubmed_chemical_list',
                'pubmed_abstract', 'pubmed_doi', 'pubmed_pmid'
            ]
            
            print("\nПроверка ключевых полей:")
            for field in key_fields:
                if field in result_df.columns:
                    value = result_df.iloc[0][field]
                    print(f"{field}: {value is not None} ({value})")
                else:
                    print(f"{field}: КОЛОНКА ОТСУТСТВУЕТ!")
            
            # Показываем все поля с данными
            print("\nВсе поля с данными:")
            for col in result_df.columns:
                value = result_df.iloc[0][col]
                if value is not None and str(value).strip():
                    print(f"{col}: {value}")
                    
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pubmed_extraction()
