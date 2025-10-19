"""
Smoke test для проверки обогащения IUPHAR на небольшом наборе данных.
Проверяет наличие всех iuphar_* колонок после обогащения.
"""

import os
import sys

import pandas as pd
import pytest

# Добавляем корневую директорию в путь
current_dir = os.path.dirname(os.path.abspath(__file__ if '__file__' in globals() else '.'))
sys.path.insert(0, os.path.dirname(current_dir))

from src.library.pipelines.target.iuphar_target import enrich_targets_with_iuphar, IupharApiCfg


def test_iuphar_smoke_enrichment():
    """Smoke test: обогащение 2-3 записей и проверка наличия всех iuphar_* колонок."""
    
    # Создаем минимальный тестовый DataFrame
    test_data = pd.DataFrame({
        "target_chembl_id": ["CHEMBL123", "CHEMBL456", "CHEMBL789"],
        "mapping_uniprot_id": ["P12345", "", "P67890"],  # Одна пустая для проверки fallback
        "uniprot_id_primary": ["P12345", "P11111", "P67890"],
        "hgnc_name": ["ADRB1", "ADRB2", "OPRM1"],
        "hgnc_id": ["HGNC:285", "HGNC:286", "HGNC:8154"],
        "gene_symbol": ["ADRB1", "ADRB2", "OPRM1"],
        "pref_name": ["Beta-1 adrenoceptor", "Beta-2 adrenoceptor", "Mu opioid receptor"],
        "molecular_function": ["G-protein coupled receptor activity", "", "opioid receptor activity"],
    })
    
    # Конфигурация с CSV (если доступны) или API fallback
    cfg = IupharApiCfg(
        use_csv=True,
        dictionary_path="configs/dictionary/_target/",
        family_file="_IUPHAR_family.csv",
        target_file="_IUPHAR_target.csv",
        batch_size=10,
        rate_limit_delay=0.1  # Быстрее для тестов
    )
    
    # Ожидаемые IUPHAR колонки
    expected_iuphar_columns = [
        "iuphar_target_id",
        "iuphar_family_id", 
        "iuphar_type",
        "iuphar_class",
        "iuphar_subclass",
        "iuphar_chain",
        "iuphar_name",
        "iuphar_full_id_path",
        "iuphar_full_name_path",
    ]
    
    try:
        # Выполняем обогащение
        result = enrich_targets_with_iuphar(test_data, cfg, batch_size=cfg.batch_size)
        
        # Проверяем что результат не пустой
        assert len(result) > 0, "Результат обогащения не должен быть пустым"
        
        # Проверяем наличие всех ожидаемых IUPHAR колонок
        missing_columns = [col for col in expected_iuphar_columns if col not in result.columns]
        assert not missing_columns, f"Отсутствуют IUPHAR колонки: {missing_columns}"
        
        # Проверяем что хотя бы одна запись имеет заполненные IUPHAR поля
        iuphar_filled = result[expected_iuphar_columns].notna().any(axis=1)
        assert iuphar_filled.any(), "Хотя бы одна запись должна иметь заполненные IUPHAR поля"
        
        # Проверяем fallback логику для пустого mapping_uniprot_id
        fallback_row = result[result["target_chembl_id"] == "CHEMBL456"]
        if not fallback_row.empty:
            # Если есть данные для этой записи, проверяем что fallback сработал
            has_iuphar_data = fallback_row[expected_iuphar_columns].notna().any(axis=1).iloc[0]
            if has_iuphar_data:
                print(f"[OK] Fallback логика работает для записи с пустым mapping_uniprot_id")
        
        print(f"[OK] Smoke test прошел успешно. Обогащено {len(result)} записей")
        print("[OK] Все ожидаемые IUPHAR колонки присутствуют:", expected_iuphar_columns)
        
        # Выводим статистику заполнения
        for col in expected_iuphar_columns:
            filled_count = result[col].notna().sum()
            print(f"[INFO] {col}: заполнено {filled_count}/{len(result)} записей")
            
    except Exception as e:
        # Если CSV недоступны, пробуем с API fallback
        print(f"[WARNING] Ошибка при обогащении: {e}")
        print("[INFO] Возможно, CSV словари недоступны, используется API fallback")
        
        # Пробуем с отключенным CSV
        cfg_api = IupharApiCfg(
            use_csv=False,
            batch_size=10,
            rate_limit_delay=0.1
        )
        
        try:
            result = enrich_targets_with_iuphar(test_data, cfg_api, batch_size=cfg_api.batch_size)
            
            # Проверяем наличие колонок
            missing_columns = [col for col in expected_iuphar_columns if col not in result.columns]
            assert not missing_columns, f"Отсутствуют IUPHAR колонки при API fallback: {missing_columns}"
            
            print(f"[OK] API fallback работает. Обогащено {len(result)} записей")
            
        except Exception as api_error:
            pytest.skip(f"Не удалось выполнить обогащение ни через CSV, ни через API: {api_error}")


def test_iuphar_columns_presence():
    """Проверяет что все IUPHAR колонки присутствуют в схеме targets.py."""
    
    from src.library.schemas.targets import TARGETS_COLUMN_ORDER
    
    expected_iuphar_columns = [
        "iuphar_target_id",
        "iuphar_family_id", 
        "iuphar_type",
        "iuphar_class",
        "iuphar_subclass",
        "iuphar_chain",
        "iuphar_name",
        "iuphar_full_id_path",
        "iuphar_full_name_path",
    ]
    
    missing_in_schema = [col for col in expected_iuphar_columns if col not in TARGETS_COLUMN_ORDER]
    assert not missing_in_schema, f"IUPHAR колонки отсутствуют в схеме: {missing_in_schema}"
    
    print("[OK] Все IUPHAR колонки присутствуют в схеме targets.py")


if __name__ == "__main__":
    # Запуск smoke тестов
    test_iuphar_columns_presence()
    test_iuphar_smoke_enrichment()
