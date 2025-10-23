#!/usr/bin/env python3
"""
Генерация YAML-патчей для исправления несоответствий.
Создает патчи для missing/extra колонок в column_order.
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Set
import sys

def load_comparison_data(comparison_file: Path) -> pd.DataFrame:
    """Загрузить данные сравнения."""
    return pd.read_csv(comparison_file)

def generate_yaml_patch(entity: str, comparison_df: pd.DataFrame) -> Dict[str, Any]:
    """Сгенерировать YAML-патч для сущности."""
    
    # Находим missing колонки (есть в YAML, но нет в выходе)
    missing_columns = comparison_df[
        (comparison_df['in_yaml'] == True) & 
        (comparison_df['in_output'] == False)
    ]['column_name'].tolist()
    
    # Находим extra колонки (есть в выходе, но нет в YAML)
    extra_columns = comparison_df[
        (comparison_df['in_yaml'] == False) & 
        (comparison_df['in_output'] == True)
    ]['column_name'].tolist()
    
    # Находим колонки с неправильным порядком
    order_issues = comparison_df[
        (comparison_df['discrepancy_type'] == 'ORDER') &
        (comparison_df['yaml_order'].notna()) &
        (comparison_df['output_order'].notna())
    ]
    
    # Создаем патч
    patch = {
        'entity': entity,
        'patch_type': 'column_order_sync',
        'missing_columns': missing_columns,
        'extra_columns': extra_columns,
        'order_issues': order_issues[['column_name', 'yaml_order', 'output_order']].to_dict('records'),
        'recommendations': []
    }
    
    # Генерируем рекомендации
    if missing_columns:
        patch['recommendations'].append({
            'action': 'add_missing_columns',
            'description': f'Добавить {len(missing_columns)} отсутствующих колонок в column_order',
            'columns': missing_columns
        })
    
    if extra_columns:
        patch['recommendations'].append({
            'action': 'remove_extra_columns',
            'description': f'Удалить {len(extra_columns)} лишних колонок из выхода или добавить в YAML',
            'columns': extra_columns
        })
    
    if not order_issues.empty:
        patch['recommendations'].append({
            'action': 'fix_column_order',
            'description': f'Исправить порядок {len(order_issues)} колонок',
            'details': order_issues[['column_name', 'yaml_order', 'output_order']].to_dict('records')
        })
    
    return patch

def create_yaml_patch_content(patch: Dict[str, Any]) -> str:
    """Создать содержимое YAML-патча."""
    
    lines = [
        f"# YAML-патч для {patch['entity']}",
        f"# Сгенерировано: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "# Добавить в секцию determinism.column_order:",
        "determinism:",
        "  column_order:"
    ]
    
    # Добавляем missing колонки
    if patch['missing_columns']:
        lines.extend([
            "    # Missing columns (есть в YAML, но отсутствуют в выходе):",
            ""
        ])
        for col in patch['missing_columns']:
            lines.append(f'    - "{col}"  # TODO: Проверить, почему отсутствует в выходе')
        lines.append("")
    
    # Добавляем extra колонки
    if patch['extra_columns']:
        lines.extend([
            "    # Extra columns (есть в выходе, но отсутствуют в YAML):",
            ""
        ])
        for col in patch['extra_columns']:
            lines.append(f'    - "{col}"  # TODO: Добавить описание и валидацию')
        lines.append("")
    
    # Добавляем рекомендации по порядку
    if patch['order_issues']:
        lines.extend([
            "    # Order issues (неправильный порядок колонок):",
            ""
        ])
        for issue in patch['order_issues']:
            lines.append(f'    # {issue["column_name"]}: YAML order={issue["yaml_order"]}, Output order={issue["output_order"]}')
        lines.append("")
    
    return '\n'.join(lines)

def create_pandera_patch_content(patch: Dict[str, Any]) -> str:
    """Создать содержимое Pandera-патча."""
    
    lines = [
        f"# Pandera-патч для {patch['entity']}",
        f"# Сгенерировано: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "from pandera import pa",
        "from pandera.typing import Series",
        "import pandas as pd",
        "",
        f"class {patch['entity'].title()}NormalizedSchemaPatch(pa.DataFrameModel):",
        "    \"\"\"Дополнительные поля для нормализованной схемы.\"\"\"",
        ""
    ]
    
    # Добавляем extra колонки как поля Pandera
    if patch['extra_columns']:
        for col in patch['extra_columns']:
            lines.extend([
                f"    {col}: Series[str] = pa.Field(",
                f"        nullable=True,",
                f"        description=\"TODO: Добавить описание для {col}\"",
                f"    )",
                ""
            ])
    
    lines.extend([
        "    class Config:",
        "        strict = True",
        "        coerce = True",
        ""
    ])
    
    return '\n'.join(lines)

def main():
    """Основная функция."""
    reports_dir = Path("metadata/reports")
    patches_dir = reports_dir / "yaml_patches"
    pandera_patches_dir = reports_dir / "pandera_patches"
    
    # Создаем директории
    patches_dir.mkdir(parents=True, exist_ok=True)
    pandera_patches_dir.mkdir(parents=True, exist_ok=True)
    
    entities = ["activity", "assay", "document", "target", "testitem"]
    all_patches = {}
    
    for entity in entities:
        comparison_file = reports_dir / f"column_comparison_{entity}.csv"
        
        if not comparison_file.exists():
            print(f"Файл сравнения не найден: {comparison_file}")
            continue
        
        print(f"Генерирую патчи для {entity}...")
        
        # Загружаем данные сравнения
        comparison_df = load_comparison_data(comparison_file)
        
        # Генерируем патч
        patch = generate_yaml_patch(entity, comparison_df)
        all_patches[entity] = patch
        
        # Создаем YAML-патч
        yaml_content = create_yaml_patch_content(patch)
        yaml_file = patches_dir / f"{entity}_column_order.patch.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            f.write(yaml_content)
        print(f"Сохранен YAML-патч: {yaml_file}")
        
        # Создаем Pandera-патч
        pandera_content = create_pandera_patch_content(patch)
        pandera_file = pandera_patches_dir / f"{entity}_schema.patch.py"
        with open(pandera_file, 'w', encoding='utf-8') as f:
            f.write(pandera_content)
        print(f"Сохранен Pandera-патч: {pandera_file}")
    
    # Сохраняем сводный JSON
    summary_file = reports_dir / "all_yaml_patches.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(all_patches, f, indent=2, ensure_ascii=False)
    print(f"Сводный файл патчей сохранен: {summary_file}")
    
    # Создаем отчет по патчам
    create_patches_report(all_patches, reports_dir)

def create_patches_report(patches: Dict[str, Any], reports_dir: Path):
    """Создать отчет по патчам."""
    
    report_lines = [
        "# Отчет по YAML-патчам",
        "",
        f"Сгенерировано: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Сводка по сущностям",
        "",
        "| Entity | Missing Columns | Extra Columns | Order Issues |",
        "|--------|-----------------|---------------|--------------|"
    ]
    
    for entity, patch in patches.items():
        missing_count = len(patch['missing_columns'])
        extra_count = len(patch['extra_columns'])
        order_count = len(patch['order_issues'])
        report_lines.append(f"| {entity} | {missing_count} | {extra_count} | {order_count} |")
    
    # Добавляем детали по каждой сущности
    for entity, patch in patches.items():
        if not any([patch['missing_columns'], patch['extra_columns'], patch['order_issues']]):
            continue
        
        report_lines.extend([
            "",
            f"## {entity.title()} - Детали патчей",
            ""
        ])
        
        if patch['missing_columns']:
            report_lines.extend([
                f"### Missing Columns ({len(patch['missing_columns'])}):",
                ""
            ])
            for col in patch['missing_columns']:
                report_lines.append(f"- `{col}`")
            report_lines.append("")
        
        if patch['extra_columns']:
            report_lines.extend([
                f"### Extra Columns ({len(patch['extra_columns'])}):",
                ""
            ])
            for col in patch['extra_columns']:
                report_lines.append(f"- `{col}`")
            report_lines.append("")
        
        if patch['order_issues']:
            report_lines.extend([
                f"### Order Issues ({len(patch['order_issues'])}):",
                "",
                "| Column | YAML Order | Output Order |",
                "|--------|------------|--------------|"
            ])
            for issue in patch['order_issues']:
                report_lines.append(f"| {issue['column_name']} | {issue['yaml_order']} | {issue['output_order']} |")
            report_lines.append("")
        
        # Добавляем рекомендации
        if patch['recommendations']:
            report_lines.extend([
                "### Рекомендации:",
                ""
            ])
            for rec in patch['recommendations']:
                report_lines.extend([
                    f"- **{rec['action']}**: {rec['description']}",
                    ""
                ])
    
    # Сохраняем отчет
    report_file = reports_dir / "yaml_patches_report.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"Отчет по патчам сохранен: {report_file}")

if __name__ == "__main__":
    main()
