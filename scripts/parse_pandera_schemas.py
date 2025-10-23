#!/usr/bin/env python3
"""
Извлечение спецификаций из Pandera схем.
Парсит InputSchema, RawSchema, NormalizedSchema из src/library/schemas/<entity>_schema.py
"""

import ast
import json
import re
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import sys

# Добавляем src в путь для импорта библиотеки
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def extract_pandera_field_info(field_node: ast.Assign) -> Dict[str, Any]:
    """Извлечь информацию о поле Pandera из AST узла."""
    field_info = {
        'field_name': None,
        'dtype': None,
        'nullable': None,
        'checks': [],
        'description': None,
        'metadata': {}
    }
    
    # Получаем имя поля
    if field_node.targets and len(field_node.targets) > 0:
        if isinstance(field_node.targets[0], ast.Name):
            field_info['field_name'] = field_node.targets[0].id
    
    # Анализируем значение поля
    if isinstance(field_node.value, ast.Call):
        # pa.Field(...) вызов
        call_node = field_node.value
        
        # Извлекаем аргументы
        for keyword in call_node.keywords:
            if keyword.arg == 'nullable':
                if isinstance(keyword.value, ast.Constant):
                    field_info['nullable'] = keyword.value.value
            elif keyword.arg == 'description':
                if isinstance(keyword.value, ast.Constant):
                    field_info['description'] = keyword.value.value
            elif keyword.arg == 'checks':
                if isinstance(keyword.value, ast.List):
                    field_info['checks'] = [ast.unparse(check) for check in keyword.value.elts]
            elif keyword.arg == 'metadata':
                if isinstance(keyword.value, ast.Dict):
                    # Простое извлечение метаданных
                    field_info['metadata'] = {}
    
    # Определяем dtype из аннотации типа
    if hasattr(field_node, 'annotation'):
        if isinstance(field_node.annotation, ast.Subscript):
            # Series[Type]
            if isinstance(field_node.annotation.slice, ast.Name):
                dtype_name = field_node.annotation.slice.id
                field_info['dtype'] = dtype_name
        elif isinstance(field_node.annotation, ast.Name):
            field_info['dtype'] = field_node.annotation.id
    
    return field_info

def extract_pandera_schema_info(schema_path: Path) -> Dict[str, Any]:
    """Извлечь информацию о Pandera схемах из файла."""
    print(f"Обрабатываю {schema_path}")
    
    with open(schema_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Парсим AST
    try:
        tree = ast.parse(content)
    except SyntaxError as e:
        print(f"Ошибка синтаксиса в {schema_path}: {e}")
        return {}
    
    schemas_info = {}
    
    # Ищем классы схем
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            class_name = node.name
            
            # Проверяем, является ли это Pandera схемой
            if 'Schema' in class_name:
                schema_info = {
                    'class_name': class_name,
                    'fields': {},
                    'config': {},
                    'base_classes': [base.id for base in node.bases if isinstance(base, ast.Name)]
                }
                
                # Извлекаем поля
                for item in node.body:
                    if isinstance(item, ast.Assign):
                        field_info = extract_pandera_field_info(item)
                        if field_info['field_name']:
                            schema_info['fields'][field_info['field_name']] = field_info
                    
                    elif isinstance(item, ast.ClassDef) and item.name == 'Config':
                        # Извлекаем конфигурацию
                        for config_item in item.body:
                            if isinstance(config_item, ast.Assign):
                                if len(config_item.targets) > 0 and isinstance(config_item.targets[0], ast.Name):
                                    config_name = config_item.targets[0].id
                                    if isinstance(config_item.value, ast.Constant):
                                        schema_info['config'][config_name] = config_item.value.value
                
                schemas_info[class_name] = schema_info
    
    return schemas_info

def extract_pandera_specs(schema_path: Path) -> Dict[str, Any]:
    """Извлечь спецификации из файла Pandera схем."""
    schemas_info = extract_pandera_schema_info(schema_path)
    
    # Подсчитываем общую статистику
    total_fields = sum(len(schema['fields']) for schema in schemas_info.values())
    
    spec = {
        'schema_path': str(schema_path),
        'total_schemas': len(schemas_info),
        'total_fields': total_fields,
        'schemas': schemas_info,
        'extracted_at': pd.Timestamp.now().isoformat()
    }
    
    return spec

def main():
    """Основная функция."""
    # Пути к схемам
    schemas_dir = Path("src/library/schemas")
    reports_dir = Path("metadata/reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Маппинг пайплайнов к их схемам
    schemas = {
        "activity": schemas_dir / "activity_schema.py",
        "assay": schemas_dir / "assay_schema.py",
        "document": schemas_dir / "document_schema.py",
        "target": schemas_dir / "target_schema.py",  # Может не существовать
        "testitem": schemas_dir / "testitem_schema.py"
    }
    
    # Извлекаем спецификации для каждого пайплайна
    all_specs = {}
    
    for entity, schema_path in schemas.items():
        if schema_path.exists():
            try:
                spec = extract_pandera_specs(schema_path)
                all_specs[entity] = spec
                
                # Сохраняем индивидуальный JSON
                output_file = reports_dir / f"{entity}_pandera_specs.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(spec, f, indent=2, ensure_ascii=False)
                print(f"Сохранено: {output_file}")
                
            except Exception as e:
                print(f"Ошибка при обработке {entity}: {e}")
        else:
            print(f"Файл не найден: {schema_path}")
    
    # Сохраняем сводный JSON
    summary_file = reports_dir / "all_pandera_specs.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(all_specs, f, indent=2, ensure_ascii=False)
    print(f"Сводный файл сохранен: {summary_file}")
    
    # Создаем краткий отчет
    report_lines = [
        "# Pandera спецификации пайплайнов",
        "",
        f"Извлечено: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "| Entity | Schemas | Total Fields | Schema File |",
        "|--------|---------|--------------|-------------|"
    ]
    
    for entity, spec in all_specs.items():
        report_lines.append(f"| {entity} | {spec['total_schemas']} | {spec['total_fields']} | {Path(spec['schema_path']).name} |")
    
    report_file = reports_dir / "pandera_specs_summary.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"Отчет сохранен: {report_file}")

if __name__ == "__main__":
    main()
