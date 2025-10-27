#!/usr/bin/env python3
"""Простой тест PubMed клиента."""

import sys
from pathlib import Path

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent / "src"))

import logging
from library.clients.pubmed import PubMedClient
from library.config import APIClientConfig

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def test_pubmed_client():
    print("=== ТЕСТ PUBMED КЛИЕНТА ===")
    
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
    test_pmid = "6991692"
    print(f"Тестируем PMID: {test_pmid}")
    
    try:
        result = client.fetch_by_pmid(test_pmid)
        print(f"Результат получен: {len(result)} полей")
        
        # Проверяем ключевые поля
        key_fields = [
            'pubmed_mesh_descriptors', 'pubmed_mesh_qualifiers', 'pubmed_chemical_list',
            'pubmed_abstract', 'pubmed_doi'
        ]
        
        for field in key_fields:
            value = result.get(field)
            print(f"{field}: {value is not None} ({value})")
            
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pubmed_client()
