#!/usr/bin/env python3
"""
Скрипт для анализа расхождений между column_order в конфигах и схемами Pandera.

Анализирует:
1. Поля в схемах Pandera, но не в column_order конфига
2. Поля в column_order конфига, но не в схемах Pandera  
3. Разный порядок полей
4. Генерирует CSV-отчет с расхождениями
"""

import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Добавляем src в путь для импорта схем
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def load_config_column_order(config_path: Path) -> list[str]:
    """Загружает column_order из конфигурационного файла."""
    try:
        with open(config_path, encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        column_order = config.get('determinism', {}).get('column_order', [])
        logger.info(f"Загружен column_order из {config_path}: {len(column_order)} колонок")
        return column_order
    except Exception as e:
        logger.error(f"Ошибка загрузки конфига {config_path}: {e}")
        return []

def load_schema_columns(schema_module_name: str) -> list[str]:
    """Загружает колонки из схемы Pandera."""
    try:
        if schema_module_name == "document":
            from library.schemas.document_schema_normalized import DocumentNormalizedSchema
            schema = DocumentNormalizedSchema.get_schema()
        elif schema_module_name == "testitem":
            from library.schemas.testitem_schema_normalized import TestitemNormalizedSchema
            schema = TestitemNormalizedSchema.get_schema()
        else:
            raise ValueError(f"Неизвестный тип схемы: {schema_module_name}")
        
        columns = list(schema.columns.keys())
        logger.info(f"Загружены колонки из схемы {schema_module_name}: {len(columns)} колонок")
        return columns
    except Exception as e:
        logger.error(f"Ошибка загрузки схемы {schema_module_name}: {e}")
        return []

def analyze_differences(config_columns: list[str], schema_columns: list[str]) -> dict[str, Any]:
    """Анализирует различия между колонками конфига и схемы."""
    config_set = set(config_columns)
    schema_set = set(schema_columns)
    
    # Поля в конфиге, но не в схеме
    missing_in_schema = config_set - schema_set
    
    # Поля в схеме, но не в конфиге
    missing_in_config = schema_set - config_set
    
    # Общие поля
    common_fields = config_set & schema_set
    
    # Проверка порядка для общих полей
    config_common = [col for col in config_columns if col in common_fields]
    schema_common = [col for col in schema_columns if col in common_fields]
    order_different = config_common != schema_common
    
    return {
        'config_columns': config_columns,
        'schema_columns': schema_columns,
        'missing_in_schema': list(missing_in_schema),
        'missing_in_config': list(missing_in_config),
        'common_fields': list(common_fields),
        'order_different': order_different,
        'config_common_order': config_common,
        'schema_common_order': schema_common
    }

def generate_report(analysis: dict[str, Any], entity_type: str) -> pd.DataFrame:
    """Генерирует отчет в виде DataFrame."""
    report_data = []
    
    # Добавляем поля, отсутствующие в схеме
    for col in analysis['missing_in_schema']:
        report_data.append({
            'entity_type': entity_type,
            'column_name': col,
            'issue_type': 'missing_in_schema',
            'severity': 'ERROR',
            'description': f'Колонка {col} есть в column_order конфига, но отсутствует в схеме Pandera',
            'recommendation': 'Добавить колонку в схему Pandera с правильной типизацией'
        })
    
    # Добавляем поля, отсутствующие в конфиге
    for col in analysis['missing_in_config']:
        report_data.append({
            'entity_type': entity_type,
            'column_name': col,
            'issue_type': 'missing_in_config',
            'severity': 'WARNING',
            'description': f'Колонка {col} есть в схеме Pandera, но отсутствует в column_order конфига',
            'recommendation': 'Добавить колонку в column_order конфига или убрать из схемы'
        })
    
    # Добавляем информацию о разном порядке
    if analysis['order_different']:
        report_data.append({
            'entity_type': entity_type,
            'column_name': 'ORDER_MISMATCH',
            'issue_type': 'order_different',
            'severity': 'WARNING',
            'description': 'Порядок общих полей отличается между конфигом и схемой',
            'recommendation': 'Синхронизировать порядок полей в схеме с column_order конфига'
        })
    
    return pd.DataFrame(report_data)

def main():
    """Основная функция скрипта."""
    logger.info("Начинаем анализ расхождений между конфигами и схемами Pandera")
    
    # Пути к файлам
    project_root = Path(__file__).parent.parent
    configs_dir = project_root / "configs"
    output_dir = project_root / "data" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Анализируем документы
    logger.info("Анализируем документы...")
    doc_config_path = configs_dir / "config_document.yaml"
    doc_config_columns = load_config_column_order(doc_config_path)
    doc_schema_columns = load_schema_columns("document")
    doc_analysis = analyze_differences(doc_config_columns, doc_schema_columns)
    doc_report = generate_report(doc_analysis, "documents")
    
    # Анализируем теститемы
    logger.info("Анализируем теститемы...")
    testitem_config_path = configs_dir / "config_testitem.yaml"
    testitem_config_columns = load_config_column_order(testitem_config_path)
    testitem_schema_columns = load_schema_columns("testitem")
    testitem_analysis = analyze_differences(testitem_config_columns, testitem_schema_columns)
    testitem_report = generate_report(testitem_analysis, "testitems")
    
    # Объединяем отчеты
    full_report = pd.concat([doc_report, testitem_report], ignore_index=True)
    
    # Сохраняем отчет
    report_path = output_dir / "column_order_sync_report.csv"
    full_report.to_csv(report_path, index=False, encoding='utf-8')
    logger.info(f"Отчет сохранен в {report_path}")
    
    # Выводим краткую статистику
    print("\n" + "="*80)
    print("КРАТКАЯ СТАТИСТИКА АНАЛИЗА")
    print("="*80)
    
    print("\nДОКУМЕНТЫ:")
    print(f"  Колонок в конфиге: {len(doc_config_columns)}")
    print(f"  Колонок в схеме: {len(doc_schema_columns)}")
    print(f"  Отсутствует в схеме: {len(doc_analysis['missing_in_schema'])}")
    print(f"  Отсутствует в конфиге: {len(doc_analysis['missing_in_config'])}")
    print(f"  Порядок отличается: {doc_analysis['order_different']}")
    
    print("\nТЕСТИТЕМЫ:")
    print(f"  Колонок в конфиге: {len(testitem_config_columns)}")
    print(f"  Колонок в схеме: {len(testitem_schema_columns)}")
    print(f"  Отсутствует в схеме: {len(testitem_analysis['missing_in_schema'])}")
    print(f"  Отсутствует в конфиге: {len(testitem_analysis['missing_in_config'])}")
    print(f"  Порядок отличается: {testitem_analysis['order_different']}")
    
    print("\nОБЩИЙ ОТЧЕТ:")
    print(f"  Всего проблем: {len(full_report)}")
    print(f"  Ошибок (ERROR): {len(full_report[full_report['severity'] == 'ERROR'])}")
    print(f"  Предупреждений (WARNING): {len(full_report[full_report['severity'] == 'WARNING'])}")
    
    # Выводим детали проблем
    if len(full_report) > 0:
        print("\nДЕТАЛИ ПРОБЛЕМ:")
        for _, row in full_report.iterrows():
            print(f"  [{row['severity']}] {row['entity_type']}: {row['description']}")
    
    print(f"\nПолный отчет сохранен в: {report_path}")
    print("="*80)
    
    return len(full_report[full_report['severity'] == 'ERROR']) > 0

if __name__ == "__main__":
    has_errors = main()
    sys.exit(1 if has_errors else 0)
