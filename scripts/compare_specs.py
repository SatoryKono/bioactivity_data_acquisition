#!/usr/bin/env python3
"""
Сравнение фактических выходов с YAML и Pandera спецификациями.
Создает сводные таблицы несоответствий для каждого пайплайна.
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Set, Optional
import sys

def load_json_specs(file_path: Path) -> Dict[str, Any]:
    """Загрузить JSON спецификации."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_yaml_columns(yaml_specs: Dict[str, Any]) -> Set[str]:
    """Получить колонки из YAML спецификации."""
    return set(yaml_specs.get('columns_spec', {}).keys())

def get_pandera_columns(pandera_specs: Dict[str, Any]) -> Set[str]:
    """Получить колонки из Pandera спецификации."""
    columns = set()
    for schema_name, schema_info in pandera_specs.get('schemas', {}).items():
        columns.update(schema_info.get('fields', {}).keys())
    return columns

def get_actual_columns(actual_specs: Dict[str, Any]) -> Set[str]:
    """Получить колонки из фактического выхода."""
    return set(actual_specs.get('columns', []))

def determine_discrepancy_type(
    column: str,
    in_yaml: bool,
    in_pandera: bool, 
    in_output: bool,
    yaml_order: Optional[int],
    output_order: Optional[int]
) -> str:
    """Определить тип несоответствия для колонки."""
    if in_yaml and in_pandera and in_output:
        if yaml_order and output_order and yaml_order != output_order:
            return "ORDER"
        else:
            return "OK"
    elif in_yaml and not in_output:
        return "MISSING"
    elif in_output and not in_yaml and not in_pandera:
        return "EXTRA"
    elif in_yaml and in_output and not in_pandera:
        return "MISSING_PANDERA"
    elif in_pandera and in_output and not in_yaml:
        return "MISSING_YAML"
    else:
        return "UNKNOWN"

def compare_entity_specs(entity: str, actual_specs: Dict[str, Any], 
                        yaml_specs: Dict[str, Any], pandera_specs: Dict[str, Any]) -> pd.DataFrame:
    """Сравнить спецификации для одной сущности."""
    
    # Получаем колонки из всех источников
    actual_columns = get_actual_columns(actual_specs)
    yaml_columns = get_yaml_columns(yaml_specs)
    pandera_columns = get_pandera_columns(pandera_specs)
    
    # Получаем все уникальные колонки
    all_columns = actual_columns | yaml_columns | pandera_columns
    
    # Создаем DataFrame для сравнения
    comparison_data = []
    
    for column in sorted(all_columns):
        in_yaml = column in yaml_columns
        in_pandera = column in pandera_columns
        in_output = column in actual_columns
        
        # Получаем порядок из YAML
        yaml_order = None
        yaml_dtype = None
        if in_yaml and 'columns_spec' in yaml_specs:
            col_spec = yaml_specs['columns_spec'].get(column, {})
            yaml_order = col_spec.get('order')
            yaml_dtype = col_spec.get('yaml_dtype')
        
        # Получаем порядок из фактического выхода
        output_order = None
        output_dtype = None
        if in_output:
            output_order = actual_specs.get('column_order', {}).get(column)
            output_dtype = actual_specs.get('dtypes', {}).get(column)
        
        # Получаем Pandera dtype
        pandera_dtype = None
        if in_pandera:
            for schema_name, schema_info in pandera_specs.get('schemas', {}).items():
                if column in schema_info.get('fields', {}):
                    pandera_dtype = schema_info['fields'][column].get('dtype')
                    break
        
        # Определяем тип несоответствия
        discrepancy_type = determine_discrepancy_type(
            column, in_yaml, in_pandera, in_output, yaml_order, output_order
        )
        
        comparison_data.append({
            'entity': entity,
            'column_name': column,
            'in_yaml': in_yaml,
            'in_pandera': in_pandera,
            'in_output': in_output,
            'yaml_dtype': yaml_dtype,
            'pandera_dtype': pandera_dtype,
            'output_dtype': output_dtype,
            'yaml_order': yaml_order,
            'output_order': output_order,
            'discrepancy_type': discrepancy_type
        })
    
    return pd.DataFrame(comparison_data)

def main():
    """Основная функция."""
    reports_dir = Path("metadata/reports")
    
    # Загружаем все спецификации
    entities = ["activity", "assay", "document", "target", "testitem"]
    
    all_comparisons = []
    
    for entity in entities:
        print(f"Сравниваю спецификации для {entity}...")
        
        # Загружаем спецификации
        actual_file = reports_dir / f"{entity}_actual_output.json"
        yaml_file = reports_dir / f"{entity}_yaml_specs.json"
        pandera_file = reports_dir / f"{entity}_pandera_specs.json"
        
        if not all(f.exists() for f in [actual_file, yaml_file, pandera_file]):
            print(f"Пропускаю {entity}: не все файлы найдены")
            continue
        
        actual_specs = load_json_specs(actual_file)
        yaml_specs = load_json_specs(yaml_file)
        pandera_specs = load_json_specs(pandera_file)
        
        # Сравниваем спецификации
        comparison_df = compare_entity_specs(entity, actual_specs, yaml_specs, pandera_specs)
        
        # Сохраняем индивидуальный CSV
        output_file = reports_dir / f"column_comparison_{entity}.csv"
        comparison_df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Сохранено: {output_file}")
        
        all_comparisons.append(comparison_df)
    
    # Объединяем все сравнения
    if all_comparisons:
        combined_df = pd.concat(all_comparisons, ignore_index=True)
        
        # Сохраняем сводный CSV
        summary_file = reports_dir / "column_comparison_all.csv"
        combined_df.to_csv(summary_file, index=False, encoding='utf-8')
        print(f"Сводный файл сохранен: {summary_file}")
        
        # Создаем отчет по несоответствиям
        create_discrepancy_report(combined_df, reports_dir)

def create_discrepancy_report(df: pd.DataFrame, reports_dir: Path):
    """Создать отчет по несоответствиям."""
    
    # Группируем по типам несоответствий
    discrepancy_summary = df.groupby(['entity', 'discrepancy_type']).size().reset_index(name='count')
    
    # Создаем Markdown отчет
    report_lines = [
        "# Отчет по несоответствиям схем",
        "",
        f"Сгенерировано: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Сводка по типам несоответствий",
        "",
        "| Entity | Discrepancy Type | Count |",
        "|--------|------------------|-------|"
    ]
    
    for _, row in discrepancy_summary.iterrows():
        report_lines.append(f"| {row['entity']} | {row['discrepancy_type']} | {row['count']} |")
    
    # Добавляем детали по каждому типу несоответствия
    for entity in df['entity'].unique():
        entity_df = df[df['entity'] == entity]
        
        report_lines.extend([
            "",
            f"## {entity.title()} - Детали несоответствий",
            ""
        ])
        
        for disc_type in entity_df['discrepancy_type'].unique():
            if disc_type == 'OK':
                continue
                
            disc_df = entity_df[entity_df['discrepancy_type'] == disc_type]
            
            report_lines.extend([
                f"### {disc_type} ({len(disc_df)} колонок)",
                ""
            ])
            
            for _, row in disc_df.iterrows():
                report_lines.append(f"- **{row['column_name']}**")
                if row['yaml_dtype'] or row['pandera_dtype'] or row['output_dtype']:
                    dtypes = []
                    if row['yaml_dtype']:
                        dtypes.append(f"YAML: {row['yaml_dtype']}")
                    if row['pandera_dtype']:
                        dtypes.append(f"Pandera: {row['pandera_dtype']}")
                    if row['output_dtype']:
                        dtypes.append(f"Output: {row['output_dtype']}")
                    report_lines.append(f"  - Типы: {', '.join(dtypes)}")
                
                if row['yaml_order'] and row['output_order'] and row['yaml_order'] != row['output_order']:
                    report_lines.append(f"  - Порядок: YAML={row['yaml_order']}, Output={row['output_order']}")
    
    # Сохраняем отчет
    report_file = reports_dir / "discrepancy_report.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"Отчет по несоответствиям сохранен: {report_file}")

if __name__ == "__main__":
    main()
