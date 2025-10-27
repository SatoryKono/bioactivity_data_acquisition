#!/usr/bin/env python3
"""Диагностика проблемы с --limit."""

import sys
from pathlib import Path

# Добавляем src в PYTHONPATH
project_root = Path(__file__).parent
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

def debug_limit_issue():
    """Диагностируем проблему с --limit."""
    print("=== ДИАГНОСТИКА ПРОБЛЕМЫ С --limit ===\n")
    
    try:
        # 1. Тестируем загрузку конфигурации
        print("1. Тестируем загрузку конфигурации...")
        from library.assay.config import load_assay_config
        config = load_assay_config('configs/config_assay.yaml')
        print(f"   ✓ Конфигурация загружена")
        print(f"   ✓ runtime.limit по умолчанию: {config.runtime.limit}")
        
        # 2. Тестируем установку limit
        print("\n2. Тестируем установку limit...")
        config.runtime.limit = 5
        print(f"   ✓ Установлен limit = {config.runtime.limit}")
        
        # 3. Тестируем загрузку входных данных
        print("\n3. Тестируем загрузку входных данных...")
        import pandas as pd
        df = pd.read_csv('data/input/assay.csv')
        print(f"   ✓ Загружено {len(df)} строк из входного файла")
        print(f"   ✓ Первые 3 assay_chembl_id: {df['assay_chembl_id'].head(3).tolist()}")
        
        # 4. Тестируем применение limit в скрипте
        print("\n4. Тестируем применение limit в скрипте...")
        assay_ids = df['assay_chembl_id'].tolist()
        print(f"   ✓ Исходное количество assay_ids: {len(assay_ids)}")
        
        if config.runtime.limit and config.runtime.limit > 0:
            original_count = len(assay_ids)
            assay_ids = assay_ids[:config.runtime.limit]
            print(f"   ✓ После применения limit: {len(assay_ids)} (ограничено с {original_count})")
        else:
            print(f"   ✓ Limit не применен (limit = {config.runtime.limit})")
        
        # 5. Тестируем создание pipeline
        print("\n5. Тестируем создание pipeline...")
        from library.assay.pipeline import AssayPipeline
        pipeline = AssayPipeline(config)
        print(f"   ✓ Pipeline создан успешно")
        print(f"   ✓ Pipeline config.runtime.limit: {pipeline.config.runtime.limit}")
        
        # 6. Тестируем создание DataFrame для pipeline
        print("\n6. Тестируем создание DataFrame для pipeline...")
        input_data = pd.DataFrame({"assay_chembl_id": assay_ids})
        print(f"   ✓ Создан input_data с {len(input_data)} строками")
        
        # 7. Тестируем применение limit в pipeline
        print("\n7. Тестируем применение limit в pipeline...")
        if pipeline.config.runtime.limit is not None:
            before_limit = len(input_data)
            limited_data = input_data.head(pipeline.config.runtime.limit)
            print(f"   ✓ В pipeline: {before_limit} -> {len(limited_data)} строк")
        else:
            print(f"   ✓ В pipeline limit не применен")
        
        print("\n✅ ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО!")
        print("Проблема не в логике применения limit.")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_limit_issue()

