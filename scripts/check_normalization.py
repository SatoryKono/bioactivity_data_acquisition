#!/usr/bin/env python3
"""
Проверка специальных форматов нормализации.
Проверяет DOI, ChEMBL ID, UniProt, PMID, InChI, даты на соответствие стандартам.
"""

import json
import pandas as pd
import re
from pathlib import Path
from typing import Dict, Any, List, Optional
import sys

def check_doi_format(series: pd.Series) -> Dict[str, Any]:
    """Проверка формата DOI."""
    pattern = r'^10\.\d+/[^\s]+$'
    
    # Убираем NaN значения
    clean_series = series.dropna()
    
    if len(clean_series) == 0:
        return {
            'total_count': len(series),
            'valid_count': 0,
            'invalid_count': 0,
            'invalid_examples': [],
            'issues': ['No data to validate']
        }
    
    # Проверяем соответствие паттерну
    valid_mask = clean_series.astype(str).str.match(pattern, na=False)
    valid_count = int(valid_mask.sum())
    invalid_count = len(clean_series) - valid_count
    
    # Собираем примеры невалидных значений
    invalid_examples = clean_series[~valid_mask].unique()[:10]
    invalid_examples = [str(v) for v in invalid_examples]
    
    issues = []
    if invalid_count > 0:
        issues.append(f"{invalid_count} DOI не соответствуют стандартному формату")
    
    # Проверяем на наличие URL префиксов
    url_prefixes = ['https://doi.org/', 'http://doi.org/', 'doi.org/']
    for prefix in url_prefixes:
        url_count = int(clean_series.astype(str).str.startswith(prefix).sum())
        if url_count > 0:
            issues.append(f"{url_count} DOI содержат URL префикс '{prefix}'")
    
    # Проверяем регистр
    lowercase_count = int(clean_series.astype(str).str.islower().sum())
    if lowercase_count < len(clean_series):
        issues.append(f"{len(clean_series) - lowercase_count} DOI не в нижнем регистре")
    
    return {
        'total_count': len(series),
        'valid_count': valid_count,
        'invalid_count': invalid_count,
        'invalid_examples': invalid_examples,
        'issues': issues
    }

def check_chembl_id_format(series: pd.Series) -> Dict[str, Any]:
    """Проверка формата ChEMBL ID."""
    pattern = r'^CHEMBL\d+$'
    
    clean_series = series.dropna()
    
    if len(clean_series) == 0:
        return {
            'total_count': len(series),
            'valid_count': 0,
            'invalid_count': 0,
            'invalid_examples': [],
            'issues': ['No data to validate']
        }
    
    valid_mask = clean_series.astype(str).str.match(pattern, na=False)
    valid_count = int(valid_mask.sum())
    invalid_count = len(clean_series) - valid_count
    
    invalid_examples = clean_series[~valid_mask].unique()[:10]
    invalid_examples = [str(v) for v in invalid_examples]
    
    issues = []
    if invalid_count > 0:
        issues.append(f"{invalid_count} ChEMBL ID не соответствуют формату CHEMBL\\d+")
    
    # Проверяем регистр
    uppercase_count = int(clean_series.astype(str).str.isupper().sum())
    if uppercase_count < len(clean_series):
        issues.append(f"{len(clean_series) - uppercase_count} ChEMBL ID не в верхнем регистре")
    
    return {
        'total_count': len(series),
        'valid_count': valid_count,
        'invalid_count': invalid_count,
        'invalid_examples': invalid_examples,
        'issues': issues
    }

def check_uniprot_format(series: pd.Series) -> Dict[str, Any]:
    """Проверка формата UniProt ID."""
    pattern = r'^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$'
    
    clean_series = series.dropna()
    
    if len(clean_series) == 0:
        return {
            'total_count': len(series),
            'valid_count': 0,
            'invalid_count': 0,
            'invalid_examples': [],
            'issues': ['No data to validate']
        }
    
    valid_mask = clean_series.astype(str).str.match(pattern, na=False)
    valid_count = int(valid_mask.sum())
    invalid_count = len(clean_series) - valid_count
    
    invalid_examples = clean_series[~valid_mask].unique()[:10]
    invalid_examples = [str(v) for v in invalid_examples]
    
    issues = []
    if invalid_count > 0:
        issues.append(f"{invalid_count} UniProt ID не соответствуют стандартному формату")
    
    return {
        'total_count': len(series),
        'valid_count': valid_count,
        'invalid_count': invalid_count,
        'invalid_examples': invalid_examples,
        'issues': issues
    }

def check_pmid_format(series: pd.Series) -> Dict[str, Any]:
    """Проверка формата PMID."""
    pattern = r'^\d+$'
    
    clean_series = series.dropna()
    
    if len(clean_series) == 0:
        return {
            'total_count': len(series),
            'valid_count': 0,
            'invalid_count': 0,
            'invalid_examples': [],
            'issues': ['No data to validate']
        }
    
    valid_mask = clean_series.astype(str).str.match(pattern, na=False)
    valid_count = int(valid_mask.sum())
    invalid_count = len(clean_series) - valid_count
    
    invalid_examples = clean_series[~valid_mask].unique()[:10]
    invalid_examples = [str(v) for v in invalid_examples]
    
    issues = []
    if invalid_count > 0:
        issues.append(f"{invalid_count} PMID не являются числовыми")
    
    return {
        'total_count': len(series),
        'valid_count': valid_count,
        'invalid_count': invalid_count,
        'invalid_examples': invalid_examples,
        'issues': issues
    }

def check_inchi_format(series: pd.Series) -> Dict[str, Any]:
    """Проверка формата InChI."""
    pattern = r'^InChI=1S?/[^\s]+$'
    
    clean_series = series.dropna()
    
    if len(clean_series) == 0:
        return {
            'total_count': len(series),
            'valid_count': 0,
            'invalid_count': 0,
            'invalid_examples': [],
            'issues': ['No data to validate']
        }
    
    valid_mask = clean_series.astype(str).str.match(pattern, na=False)
    valid_count = int(valid_mask.sum())
    invalid_count = len(clean_series) - valid_count
    
    invalid_examples = clean_series[~valid_mask].unique()[:10]
    invalid_examples = [str(v) for v in invalid_examples]
    
    issues = []
    if invalid_count > 0:
        issues.append(f"{invalid_count} InChI не соответствуют стандартному формату")
    
    return {
        'total_count': len(series),
        'valid_count': valid_count,
        'invalid_count': invalid_count,
        'invalid_examples': invalid_examples,
        'issues': issues
    }

def check_inchi_key_format(series: pd.Series) -> Dict[str, Any]:
    """Проверка формата InChI Key."""
    pattern = r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$'
    
    clean_series = series.dropna()
    
    if len(clean_series) == 0:
        return {
            'total_count': len(series),
            'valid_count': 0,
            'invalid_count': 0,
            'invalid_examples': [],
            'issues': ['No data to validate']
        }
    
    valid_mask = clean_series.astype(str).str.match(pattern, na=False)
    valid_count = int(valid_mask.sum())
    invalid_count = len(clean_series) - valid_count
    
    invalid_examples = clean_series[~valid_mask].unique()[:10]
    invalid_examples = [str(v) for v in invalid_examples]
    
    issues = []
    if invalid_count > 0:
        issues.append(f"{invalid_count} InChI Key не соответствуют формату")
    
    return {
        'total_count': len(series),
        'valid_count': valid_count,
        'invalid_count': invalid_count,
        'invalid_examples': invalid_examples,
        'issues': issues
    }

def check_date_format(series: pd.Series) -> Dict[str, Any]:
    """Проверка формата дат."""
    iso_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?$'
    
    clean_series = series.dropna()
    
    if len(clean_series) == 0:
        return {
            'total_count': len(series),
            'valid_count': 0,
            'invalid_count': 0,
            'invalid_examples': [],
            'issues': ['No data to validate']
        }
    
    # Проверяем ISO 8601 формат
    iso_mask = clean_series.astype(str).str.match(iso_pattern, na=False)
    iso_count = int(iso_mask.sum())
    
    # Пытаемся парсить как datetime
    parseable_count = 0
    for val in clean_series:
        try:
            pd.to_datetime(val)
            parseable_count += 1
        except:
            pass
    
    invalid_count = len(clean_series) - iso_count
    invalid_examples = clean_series[~iso_mask].unique()[:10]
    invalid_examples = [str(v) for v in invalid_examples]
    
    issues = []
    if invalid_count > 0:
        issues.append(f"{invalid_count} дат не в ISO 8601 формате")
    
    if parseable_count < len(clean_series):
        issues.append(f"{len(clean_series) - parseable_count} дат не парсятся как datetime")
    
    return {
        'total_count': len(series),
        'valid_count': iso_count,
        'invalid_count': invalid_count,
        'invalid_examples': invalid_examples,
        'issues': issues
    }

def check_entity_normalization(entity: str, csv_path: Path) -> Dict[str, Any]:
    """Проверка нормализации для одной сущности."""
    print(f"Проверяю нормализацию для {entity}...")
    
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Ошибка чтения {csv_path}: {e}")
        return {}
    
    results = {}
    
    # Определяем колонки для проверки по паттернам в именах
    for column in df.columns:
        column_lower = column.lower()
        
        if 'doi' in column_lower:
            results[column] = check_doi_format(df[column])
        elif 'chembl' in column_lower and 'id' in column_lower:
            results[column] = check_chembl_id_format(df[column])
        elif 'uniprot' in column_lower and 'id' in column_lower:
            results[column] = check_uniprot_format(df[column])
        elif 'pmid' in column_lower or 'pubmed' in column_lower:
            results[column] = check_pmid_format(df[column])
        elif 'inchi' in column_lower and 'key' in column_lower:
            results[column] = check_inchi_key_format(df[column])
        elif 'inchi' in column_lower and 'key' not in column_lower:
            results[column] = check_inchi_format(df[column])
        elif any(date_word in column_lower for date_word in ['date', 'time', 'extracted', 'created', 'updated']):
            results[column] = check_date_format(df[column])
    
    return results

def main():
    """Основная функция."""
    reports_dir = Path("metadata/reports")
    
    # Пути к данным
    output_dir = Path("data/output")
    csv_files = {
        "activity": output_dir / "activities" / "activities_20251023.csv",
        "assay": output_dir / "assays" / "assays_20251023.csv",
        "document": output_dir / "documents" / "documents_20251023.csv",
        "target": output_dir / "targets" / "targets_20251023.csv",
        "testitem": output_dir / "testitem" / "testitems_20251023.csv"
    }
    
    all_results = {}
    
    for entity, csv_path in csv_files.items():
        if csv_path.exists():
            results = check_entity_normalization(entity, csv_path)
            all_results[entity] = results
            
            # Сохраняем индивидуальный JSON
            output_file = reports_dir / f"{entity}_normalization_check.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"Сохранено: {output_file}")
        else:
            print(f"CSV файл не найден: {csv_path}")
    
    # Сохраняем сводный JSON
    summary_file = reports_dir / "all_normalization_checks.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    print(f"Сводный файл сохранен: {summary_file}")
    
    # Создаем отчет
    create_normalization_report(all_results, reports_dir)

def create_normalization_report(results: Dict[str, Any], reports_dir: Path):
    """Создать отчет по нормализации."""
    
    report_lines = [
        "# Отчет по проверке нормализации",
        "",
        f"Сгенерировано: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Сводка по сущностям",
        "",
        "| Entity | Columns Checked | Total Issues |",
        "|--------|-----------------|--------------|"
    ]
    
    for entity, entity_results in results.items():
        total_columns = len(entity_results)
        total_issues = sum(len(col_results.get('issues', [])) for col_results in entity_results.values())
        report_lines.append(f"| {entity} | {total_columns} | {total_issues} |")
    
    # Добавляем детали по каждой сущности
    for entity, entity_results in results.items():
        if not entity_results:
            continue
        
        report_lines.extend([
            "",
            f"## {entity.title()} - Детали нормализации",
            ""
        ])
        
        for column, col_results in entity_results.items():
            if not col_results.get('issues'):
                continue
            
            report_lines.extend([
                f"### {column}",
                f"- **Total Records**: {col_results['total_count']}",
                f"- **Valid**: {col_results['valid_count']}",
                f"- **Invalid**: {col_results['invalid_count']}",
                ""
            ])
            
            for issue in col_results['issues']:
                report_lines.append(f"- {issue}")
            
            if col_results.get('invalid_examples'):
                report_lines.extend([
                    "",
                    "**Examples of invalid values:**",
                    ""
                ])
                for example in col_results['invalid_examples'][:5]:
                    report_lines.append(f"- `{example}`")
            
            report_lines.append("")
    
    # Сохраняем отчет
    report_file = reports_dir / "normalization_report.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"Отчет по нормализации сохранен: {report_file}")

if __name__ == "__main__":
    main()
