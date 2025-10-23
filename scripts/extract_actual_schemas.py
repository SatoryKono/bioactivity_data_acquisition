#!/usr/bin/env python3
"""
Извлечение фактических схем из CSV выходов пайплайнов.
Создает JSON файлы с колонками, типами и порядком для каждого пайплайна.
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
import sys
import os

# Добавляем src в путь для импорта библиотеки
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def extract_schema_from_csv(csv_path: Path) -> Dict[str, Any]:
    """Извлечь схему из CSV файла."""
    print(f"Обрабатываю {csv_path}")
    
    # Читаем CSV
    df = pd.read_csv(csv_path, nrows=0)  # Только заголовки
    
    # Извлекаем информацию о колонках
    columns = df.columns.tolist()
    dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
    
    # Создаем схему
    schema = {
        "file_path": str(csv_path),
        "total_columns": len(columns),
        "columns": columns,
        "column_order": {col: i for i, col in enumerate(columns, 1)},
        "dtypes": dtypes,
        "extracted_at": pd.Timestamp.now().isoformat()
    }
    
    return schema

def main():
    """Основная функция."""
    # Пути к выходным файлам
    output_dir = Path("data/output")
    reports_dir = Path("metadata/reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Маппинг пайплайнов к их выходным файлам
    pipelines = {
        "activity": output_dir / "activities" / "activities_20251023.csv",
        "assay": output_dir / "assays" / "assays_20251023.csv", 
        "document": output_dir / "documents" / "documents_20251023.csv",
        "target": output_dir / "targets" / "targets_20251023.csv",
        "testitem": output_dir / "testitem" / "testitems_20251023.csv"
    }
    
    # Извлекаем схемы для каждого пайплайна
    all_schemas = {}
    
    for entity, csv_path in pipelines.items():
        if csv_path.exists():
            try:
                schema = extract_schema_from_csv(csv_path)
                all_schemas[entity] = schema
                
                # Сохраняем индивидуальный JSON
                output_file = reports_dir / f"{entity}_actual_output.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(schema, f, indent=2, ensure_ascii=False)
                print(f"Сохранено: {output_file}")
                
            except Exception as e:
                print(f"Ошибка при обработке {entity}: {e}")
        else:
            print(f"Файл не найден: {csv_path}")
    
    # Сохраняем сводный JSON
    summary_file = reports_dir / "all_actual_schemas.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(all_schemas, f, indent=2, ensure_ascii=False)
    print(f"Сводный файл сохранен: {summary_file}")
    
    # Создаем краткий отчет
    report_lines = [
        "# Фактические схемы пайплайнов",
        "",
        f"Извлечено: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "| Entity | Columns | File |",
        "|--------|---------|------|"
    ]
    
    for entity, schema in all_schemas.items():
        report_lines.append(f"| {entity} | {schema['total_columns']} | {Path(schema['file_path']).name} |")
    
    report_file = reports_dir / "actual_schemas_summary.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"Отчет сохранен: {report_file}")

if __name__ == "__main__":
    main()
