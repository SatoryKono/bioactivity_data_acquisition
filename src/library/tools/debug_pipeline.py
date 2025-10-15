#!/usr/bin/env python3
"""Скрипт для отладки пайплайна."""

import sys
from pathlib import Path
import pandas as pd

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent / "src"))

from library.documents.config import load_document_config
from library.documents.pipeline import _extract_data_from_source, _create_api_client

def test_pipeline_step():
    """Тестируем один шаг пайплайна."""
    print("=== ТЕСТИРОВАНИЕ ШАГА ПАЙПЛАЙНА ===")
    
    # Создаем тестовые данные
    test_data = {
        'document_chembl_id': ['CHEMBL1121421'],
        'doi': ['10.1021/jm00178a007'],
        'pubmed_id': [6991692],
        'title': ['Test Title'],
        'journal': ['Test Journal']
    }
    
    frame = pd.DataFrame(test_data)
    print(f"Исходный DataFrame: {frame.columns.tolist()}")
    print(f"Исходные данные: {frame.iloc[0].to_dict()}")
    
    # Загружаем конфигурацию
    config_path = Path("configs/config.yaml")
    doc_config = load_document_config(config_path)
    
    # Тестируем ChEMBL
    print("\n--- Тестируем ChEMBL ---")
    client = _create_api_client("chembl", doc_config)
    result_frame = _extract_data_from_source("chembl", client, frame, doc_config)
    
    print(f"Результат ChEMBL: {result_frame.columns.tolist()}")
    print(f"ChEMBL данные: {result_frame.iloc[0].to_dict()}")
    
    # Тестируем Crossref
    print("\n--- Тестируем Crossref ---")
    client = _create_api_client("crossref", doc_config)
    result_frame = _extract_data_from_source("crossref", client, result_frame, doc_config)
    
    print(f"Результат Crossref: {result_frame.columns.tolist()}")
    print(f"Crossref данные: {result_frame.iloc[0].to_dict()}")
    
    # Тестируем OpenAlex
    print("\n--- Тестируем OpenAlex ---")
    client = _create_api_client("openalex", doc_config)
    result_frame = _extract_data_from_source("openalex", client, result_frame, doc_config)
    
    print(f"Результат OpenAlex: {result_frame.columns.tolist()}")
    print(f"OpenAlex данные: {result_frame.iloc[0].to_dict()}")

if __name__ == "__main__":
    test_pipeline_step()
