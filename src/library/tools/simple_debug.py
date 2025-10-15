#!/usr/bin/env python3
"""Простой скрипт для отладки одного API клиента."""

import sys
from pathlib import Path

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent / "src"))

from library.config import APIClientConfig, RetrySettings
from library.clients.chembl import ChEMBLClient

def test_chembl():
    """Тестируем ChEMBL клиент."""
    print("=== ТЕСТИРОВАНИЕ ChEMBL КЛИЕНТА ===")
    
    api_config = APIClientConfig(
        name="chembl",
        base_url="https://www.ebi.ac.uk/chembl/api/data",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=60.0,
        retries=RetrySettings(total=5, backoff_multiplier=2.0),
    )
    
    client = ChEMBLClient(api_config, timeout=60.0)
    
    # Тестируем на первом документе
    test_doc_id = "CHEMBL1121421"
    print(f"Тестируем ChEMBL с doc_id: {test_doc_id}")
    
    try:
        result = client.fetch_by_doc_id(test_doc_id)
        print(f"Результат ChEMBL: {result}")
        print(f"Количество полей: {len(result)}")
        print(f"Непустые поля: {[k for k, v in result.items() if v is not None]}")
    except Exception as e:
        print(f"Ошибка ChEMBL: {e}")
        print(f"Тип ошибки: {type(e).__name__}")

if __name__ == "__main__":
    test_chembl()
