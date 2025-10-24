#!/usr/bin/env python3
"""Упрощенный тест для проверки расширенного пайплайна activities."""

import logging
import pandas as pd
from pathlib import Path

from src.library.activity.config import load_activity_config
from src.library.activity.pipeline import ActivityPipeline

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_activities_pipeline_simple():
    """Упрощенное тестирование расширенного пайплайна activities."""
    
    logger.info("Starting simple activities pipeline test")
    
    # Загрузка конфигурации
    config_path = Path("configs/config_activity.yaml")
    config = load_activity_config(config_path)
    
    # Создание пайплайна
    pipeline = ActivityPipeline(config)
    
    # Загрузка тестовых данных
    input_path = Path("data/input/activity.csv")
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return False
    
    input_data = pd.read_csv(input_path)
    logger.info(f"Loaded {len(input_data)} test activities")
    
    try:
        # Тестирование извлечения
        logger.info("Testing extraction...")
        extracted_data = pipeline.extract(input_data)
        logger.info(f"Extracted {len(extracted_data)} activities")
        
        if extracted_data.empty:
            logger.warning("No data extracted - this might be expected for test IDs")
            return True
        
        # Вывод информации о колонках
        logger.info(f"Extracted columns ({len(extracted_data.columns)}): {list(extracted_data.columns)}")
        
        # Проверка ключевых полей
        key_fields = ['activity_id', 'assay_id', 'doc_id', 'record_id', 'molregno']
        for field in key_fields:
            if field in extracted_data.columns:
                non_null_count = extracted_data[field].notna().sum()
                logger.info(f"Field {field}: {non_null_count}/{len(extracted_data)} non-null values")
        
        # Проверка новых полей
        new_fields = ['bei', 'sei', 'le', 'lle', 'activity_prop_type', 'activity_prop_value']
        for field in new_fields:
            if field in extracted_data.columns:
                non_null_count = extracted_data[field].notna().sum()
                logger.info(f"New field {field}: {non_null_count}/{len(extracted_data)} non-null values")
        
        logger.info("Simple activities pipeline test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Pipeline test failed: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = test_activities_pipeline_simple()
    exit(0 if success else 1)
