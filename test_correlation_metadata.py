#!/usr/bin/env python3
"""Тест для проверки сохранения корреляционного анализа в метаданные."""

import pandas as pd

from src.library.activity.pipeline import ActivityPipeline

def test_correlation_in_metadata():
    """Тест проверяет, что корреляционный анализ сохраняется в метаданные."""
    
    # Создаем конфигурацию
    from src.library.config import Config
    config = Config.load('configs/config_activity.yaml')
    pipeline = ActivityPipeline(config)

    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'activity_id': [1, 2, 3, 4, 5],
        'standard_value': [1.0, 2.0, 3.0, 4.0, 5.0],
        'standard_units': ['nM', 'nM', 'nM', 'nM', 'nM'],
        'pchembl_value': [9.0, 8.7, 8.5, 8.2, 8.0]
    })

    # Запускаем пайплайн
    result = pipeline.run(test_data)
    
    print('Pipeline completed successfully')
    print(f'Correlation analysis in result: {result.correlation_analysis is not None}')
    print(f'Correlation insights in result: {result.correlation_insights is not None}')
    
    if result.metadata and result.metadata.metadata:
        has_correlation_analysis = "correlation_analysis" in result.metadata.metadata
        has_correlation_insights = "correlation_insights" in result.metadata.metadata
        print(f'Metadata correlation_analysis: {has_correlation_analysis}')
        print(f'Metadata correlation_insights: {has_correlation_insights}')
        
        if has_correlation_analysis:
            print("Correlation analysis keys:", list(result.metadata.metadata["correlation_analysis"].keys()))
        if has_correlation_insights:
            print("Correlation insights keys:", list(result.metadata.metadata["correlation_insights"].keys()))
    else:
        print("No metadata found")

if __name__ == "__main__":
    test_correlation_in_metadata()
