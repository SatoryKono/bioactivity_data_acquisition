#!/usr/bin/env python3
"""Тестовый скрипт для проверки ChEMBL API response structure."""

import requests
import json

def test_chembl_target_api():
    """Тестирует структуру ответа ChEMBL API для target."""
    url = "https://www.ebi.ac.uk/chembl/api/data/target/CHEMBL2343"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        print("=== ChEMBL API Response Structure ===")
        print(f"Top-level keys: {list(data.keys())}")
        
        # Проверяем target_components
        if 'target_components' in data:
            components = data['target_components']
            print(f"\ntarget_components type: {type(components)}")
            print(f"target_components length: {len(components) if isinstance(components, list) else 'N/A'}")
            
            if isinstance(components, list) and components:
                print(f"\nFirst component keys: {list(components[0].keys())}")
                print(f"First component sample:")
                print(json.dumps(components[0], indent=2)[:500] + "...")
        else:
            print("\n❌ target_components НЕ НАЙДЕНО в ответе!")
        
        # Проверяем другие важные поля
        important_fields = [
            'target_chembl_id', 'pref_name', 'target_type', 'tax_id',
            'protein_classifications', 'cross_references'
        ]
        
        print(f"\n=== Важные поля ===")
        for field in important_fields:
            if field in data:
                value = data[field]
                print(f"✅ {field}: {type(value)} - {str(value)[:100]}...")
            else:
                print(f"❌ {field}: НЕ НАЙДЕНО")
                
    except Exception as e:
        print(f"Ошибка при запросе к API: {e}")

if __name__ == "__main__":
    test_chembl_target_api()
