#!/usr/bin/env python3
"""
Извлечение спецификаций из YAML конфигураций пайплайнов.
Парсит column_order, dtype, валидацию из configs/config_<entity>.yaml
"""

import yaml
import json
import re
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
import sys

def parse_column_comment(comment: str) -> Dict[str, Any]:
    """Парсит комментарий к колонке в YAML для извлечения метаданных."""
    if not comment:
        return {}
    
    # Паттерны для извлечения информации
    patterns = {
        'dtype': r'тип:\s*([A-Z]+)',
        'source': r'источник:\s*([^,]+)',
        'validation': r'валидация:\s*([^,]+)',
        'nullable': r'nullable',
        'pattern': r'pattern\s+([^\s]+)',
        'min_value': r'>=?\s*([0-9.e-]+)',
        'max_value': r'<=?\s*([0-9.e-]+)',
        'enum': r'enum\s+\[([^\]]+)\]'
    }
    
    result = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, comment, re.IGNORECASE)
        if match:
            if key in ['nullable']:
                result[key] = True
            elif key in ['enum']:
                # Парсим enum значения
                enum_str = match.group(1)
                enum_values = [v.strip().strip('"\'') for v in enum_str.split(',')]
                result[key] = enum_values
            else:
                result[key] = match.group(1).strip()
    
    return result

def extract_yaml_specs(config_path: Path) -> Dict[str, Any]:
    """Извлечь спецификации из YAML конфигурации."""
    print(f"Обрабатываю {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Извлекаем column_order
    column_order = config.get('determinism', {}).get('column_order', [])
    
    # Парсим каждую колонку
    columns_spec = {}
    for i, col_spec in enumerate(column_order):
        if isinstance(col_spec, str):
            # Простая строка без комментария
            col_name = col_spec.strip('"\'')
            columns_spec[col_name] = {
                'order': i + 1,
                'yaml_dtype': None,
                'source': None,
                'validation': None,
                'nullable': None,
                'comment': None
            }
        elif isinstance(col_spec, dict):
            # Словарь с метаданными
            col_name = list(col_spec.keys())[0]
            col_data = col_spec[col_name]
            columns_spec[col_name] = {
                'order': i + 1,
                'yaml_dtype': col_data.get('dtype'),
                'source': col_data.get('source'),
                'validation': col_data.get('validation'),
                'nullable': col_data.get('nullable'),
                'comment': col_data.get('comment')
            }
    
    # Извлекаем другие настройки
    io_settings = config.get('io', {})
    output_settings = io_settings.get('output', {})
    csv_settings = output_settings.get('csv', {})
    
    # Извлекаем настройки валидации
    validation_settings = config.get('validation', {})
    
    # Извлекаем настройки нормализации
    normalization_settings = config.get('normalization', {})
    
    spec = {
        'config_path': str(config_path),
        'pipeline_name': config.get('pipeline', {}).get('name'),
        'pipeline_version': config.get('pipeline', {}).get('version'),
        'entity_type': config.get('pipeline', {}).get('entity_type'),
        'total_columns': len(column_order),
        'column_order': column_order,
        'columns_spec': columns_spec,
        'output_settings': {
            'format': output_settings.get('format'),
            'encoding': csv_settings.get('encoding'),
            'float_format': csv_settings.get('float_format'),
            'date_format': csv_settings.get('date_format'),
            'na_rep': csv_settings.get('na_rep'),
            'line_terminator': csv_settings.get('line_terminator')
        },
        'validation_settings': validation_settings,
        'normalization_settings': normalization_settings,
        'extracted_at': pd.Timestamp.now().isoformat()
    }
    
    return spec

def main():
    """Основная функция."""
    
    # Пути к конфигурациям
    configs_dir = Path("configs")
    reports_dir = Path("metadata/reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Маппинг пайплайнов к их конфигурациям
    configs = {
        "activity": configs_dir / "config_activity.yaml",
        "assay": configs_dir / "config_assay.yaml",
        "document": configs_dir / "config_document.yaml", 
        "target": configs_dir / "config_target.yaml",
        "testitem": configs_dir / "config_testitem.yaml"
    }
    
    # Извлекаем спецификации для каждого пайплайна
    all_specs = {}
    
    for entity, config_path in configs.items():
        if config_path.exists():
            try:
                spec = extract_yaml_specs(config_path)
                all_specs[entity] = spec
                
                # Сохраняем индивидуальный JSON
                output_file = reports_dir / f"{entity}_yaml_specs.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(spec, f, indent=2, ensure_ascii=False)
                print(f"Сохранено: {output_file}")
                
            except Exception as e:
                print(f"Ошибка при обработке {entity}: {e}")
        else:
            print(f"Файл не найден: {config_path}")
    
    # Сохраняем сводный JSON
    summary_file = reports_dir / "all_yaml_specs.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(all_specs, f, indent=2, ensure_ascii=False)
    print(f"Сводный файл сохранен: {summary_file}")
    
    # Создаем краткий отчет
    report_lines = [
        "# YAML спецификации пайплайнов",
        "",
        f"Извлечено: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "| Entity | Columns | Version | Config File |",
        "|--------|---------|---------|-------------|"
    ]
    
    for entity, spec in all_specs.items():
        report_lines.append(f"| {entity} | {spec['total_columns']} | {spec.get('pipeline_version', 'N/A')} | {Path(spec['config_path']).name} |")
    
    report_file = reports_dir / "yaml_specs_summary.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"Отчет сохранен: {report_file}")

if __name__ == "__main__":
    main()
