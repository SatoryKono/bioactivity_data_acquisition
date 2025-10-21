#!/usr/bin/env python3
"""Тест улучшенной обработки ошибок batch-processing для targets."""

import pytest
import pandas as pd
from pathlib import Path

from library.target.pipeline import run_target_etl
from library.target.config import load_target_config


def test_improved_batch_handling():
    """Тестируем улучшенную обработку ошибок batch-processing."""
    print("=== Тест улучшенной обработки ошибок batch-processing ===")
    
    # Загружаем конфигурацию
    config = load_target_config('configs/config_target_full.yaml')
    
    # Создаем тестовые данные (небольшой набор для быстрого тестирования)
    test_ids = ['CHEMBL2343', 'CHEMBL4580', 'CHEMBL5255']
    input_frame = pd.DataFrame({'target_chembl_id': test_ids})
    
    print(f'Тестируем с {len(test_ids)} targets: {test_ids}')
    print('Ожидаем увидеть улучшенные сообщения об ошибках...')
    
    # Запускаем ETL
    result = run_target_etl(config, input_frame=input_frame)
    
    print('ETL завершен успешно!')
    print(f'Targets обработано: {len(result.targets)}')
    
    # Проверяем результаты
    if 'mapping_uniprot_id' in result.targets.columns:
        mapping_count = result.targets['mapping_uniprot_id'].notna().sum()
        print(f'Targets с mapping_uniprot_id: {mapping_count}/{len(result.targets)}')
    
    if 'iuphar_target_id' in result.targets.columns:
        iuphar_count = result.targets['iuphar_target_id'].notna().sum()
        print(f'Targets с iuphar_target_id: {iuphar_count}/{len(result.targets)}')
    
    # Показываем первые записи
    print("\nРезультаты:")
    print(result.targets[['target_chembl_id', 'mapping_uniprot_id', 'iuphar_target_id']].head())
    
    # Проверяем, что результат не пустой
    assert len(result.targets) > 0, "Результат не должен быть пустым"
    assert 'target_chembl_id' in result.targets.columns, "Должна быть колонка target_chembl_id"
    
    return result


@pytest.mark.integration
def test_improved_batch_handling_integration():
    """Интеграционный тест улучшенной обработки ошибок batch-processing."""
    result = test_improved_batch_handling()
    
    # Дополнительные проверки для интеграционного теста
    assert result is not None, "Результат не должен быть None"
    assert hasattr(result, 'targets'), "Результат должен иметь атрибут targets"
    assert isinstance(result.targets, pd.DataFrame), "targets должен быть DataFrame"


if __name__ == "__main__":
    try:
        result = test_improved_batch_handling()
        print("\n[SUCCESS] Тест прошел успешно!")
    except Exception as e:
        print(f"\n[ERROR] Тест не прошел: {e}")
        import traceback
        traceback.print_exc()
        import sys
        sys.exit(1)
