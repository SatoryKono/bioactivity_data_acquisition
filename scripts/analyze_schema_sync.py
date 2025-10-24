#!/usr/bin/env python3
"""
Анализ синхронизации схем и конфигураций.
Скрипт для инвентаризации фактических выходов пайплайнов и сопоставления с YAML.
"""

import pandas as pd
import yaml
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SchemaSyncAnalyzer:
    """Анализатор синхронизации схем и конфигураций."""
    
    def __init__(self, output_dir: str = "data/output", configs_dir: str = "configs"):
        self.output_dir = Path(output_dir)
        self.configs_dir = Path(configs_dir)
        self.entities = ["activities", "assays", "documents", "targets", "testitem"]
        self.results = {}
        
    def analyze_entity_outputs(self) -> Dict[str, Any]:
        """Анализ фактических выходов для всех сущностей."""
        logger.info("Начинаю анализ фактических выходов пайплайнов...")
        
        for entity in self.entities:
            logger.info(f"Анализирую {entity}...")
            
            # Поиск актуального CSV файла
            entity_dir = self.output_dir / entity
            if not entity_dir.exists():
                logger.warning(f"Директория {entity_dir} не найдена")
                continue
                
            csv_files = list(entity_dir.glob("*.csv"))
            if not csv_files:
                logger.warning(f"CSV файлы не найдены в {entity_dir}")
                continue
                
            # Исключаем QC и summary файлы, берем основной CSV
            main_csv_files = [f for f in csv_files if not any(x in f.name.lower() for x in ['qc', 'summary', 'correlation', 'meta'])]
            if main_csv_files:
                latest_csv = max(main_csv_files, key=os.path.getmtime)
            else:
                # Если нет основных файлов, берем любой
                latest_csv = max(csv_files, key=os.path.getmtime)
            logger.info(f"Анализирую файл: {latest_csv}")
            
            try:
                # Читаем только заголовки для анализа колонок
                df = pd.read_csv(latest_csv, nrows=0)
                columns = list(df.columns)
                
                # Анализ типов данных (читаем первые несколько строк)
                df_sample = pd.read_csv(latest_csv, nrows=10)
                dtypes = df_sample.dtypes.to_dict()
                
                # Анализ специальных форматов
                format_analysis = self._analyze_special_formats(df_sample, entity)
                
                self.results[entity] = {
                    'csv_file': str(latest_csv),
                    'columns': columns,
                    'column_count': len(columns),
                    'dtypes': {col: str(dtype) for col, dtype in dtypes.items()},
                    'format_analysis': format_analysis
                }
                
                logger.info(f"✅ {entity}: {len(columns)} колонок")
                
            except Exception as e:
                logger.error(f"Ошибка при анализе {entity}: {e}")
                self.results[entity] = {'error': str(e)}
                
        return self.results
    
    def _analyze_special_formats(self, df: pd.DataFrame, entity: str) -> Dict[str, Any]:
        """Анализ специальных форматов данных."""
        analysis = {}
        
        # DOI анализ
        doi_cols = [col for col in df.columns if 'doi' in col.lower()]
        if doi_cols:
            analysis['doi'] = self._analyze_doi_format(df, doi_cols)
            
        # ChEMBL ID анализ
        chembl_cols = [col for col in df.columns if 'chembl_id' in col.lower()]
        if chembl_cols:
            analysis['chembl_id'] = self._analyze_chembl_format(df, chembl_cols)
            
        # PMID анализ
        pmid_cols = [col for col in df.columns if 'pmid' in col.lower()]
        if pmid_cols:
            analysis['pmid'] = self._analyze_pmid_format(df, pmid_cols)
            
        # Дата анализ
        date_cols = [col for col in df.columns if any(x in col.lower() for x in ['date', 'extracted_at', 'timestamp'])]
        if date_cols:
            analysis['dates'] = self._analyze_date_format(df, date_cols)
            
        # Boolean анализ
        bool_cols = [col for col in df.columns if df[col].dtype == 'bool' or any(x in col.lower() for x in ['flag', 'is_', 'has_'])]
        if bool_cols:
            analysis['boolean'] = self._analyze_boolean_format(df, bool_cols)
            
        return analysis
    
    def _analyze_doi_format(self, df: pd.DataFrame, doi_cols: List[str]) -> Dict[str, Any]:
        """Анализ формата DOI."""
        analysis = {}
        for col in doi_cols:
            non_null_values = df[col].dropna()
            if len(non_null_values) > 0:
                sample_values = non_null_values.head(5).tolist()
                # Проверка на канонический формат DOI
                canonical_count = sum(1 for val in non_null_values if re.match(r'^10\.\d+/.+$', str(val)))
                analysis[col] = {
                    'sample_values': sample_values,
                    'canonical_format_rate': canonical_count / len(non_null_values),
                    'has_url_prefix': any('http' in str(val) for val in sample_values),
                    'case_issues': any(val != str(val).lower() for val in sample_values if isinstance(val, str))
                }
        return analysis
    
    def _analyze_chembl_format(self, df: pd.DataFrame, chembl_cols: List[str]) -> Dict[str, Any]:
        """Анализ формата ChEMBL ID."""
        analysis = {}
        for col in chembl_cols:
            non_null_values = df[col].dropna()
            if len(non_null_values) > 0:
                sample_values = non_null_values.head(5).tolist()
                # Проверка на правильный формат ChEMBL ID
                valid_count = sum(1 for val in non_null_values if re.match(r'^CHEMBL\d+$', str(val)))
                analysis[col] = {
                    'sample_values': sample_values,
                    'valid_format_rate': valid_count / len(non_null_values),
                    'case_issues': any(val != str(val).upper() for val in sample_values if isinstance(val, str))
                }
        return analysis
    
    def _analyze_pmid_format(self, df: pd.DataFrame, pmid_cols: List[str]) -> Dict[str, Any]:
        """Анализ формата PMID."""
        analysis = {}
        for col in pmid_cols:
            non_null_values = df[col].dropna()
            if len(non_null_values) > 0:
                sample_values = non_null_values.head(5).tolist()
                # Проверка на числовой формат PMID
                numeric_count = sum(1 for val in non_null_values if str(val).isdigit())
                analysis[col] = {
                    'sample_values': sample_values,
                    'numeric_format_rate': numeric_count / len(non_null_values)
                }
        return analysis
    
    def _analyze_date_format(self, df: pd.DataFrame, date_cols: List[str]) -> Dict[str, Any]:
        """Анализ формата дат."""
        analysis = {}
        for col in date_cols:
            non_null_values = df[col].dropna()
            if len(non_null_values) > 0:
                sample_values = non_null_values.head(3).tolist()
                # Проверка на ISO 8601 формат
                iso_count = sum(1 for val in non_null_values if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', str(val)))
                analysis[col] = {
                    'sample_values': sample_values,
                    'iso8601_format_rate': iso_count / len(non_null_values)
                }
        return analysis
    
    def _analyze_boolean_format(self, df: pd.DataFrame, bool_cols: List[str]) -> Dict[str, Any]:
        """Анализ формата boolean."""
        analysis = {}
        for col in bool_cols:
            non_null_values = df[col].dropna()
            if len(non_null_values) > 0:
                unique_values = non_null_values.unique()
                analysis[col] = {
                    'unique_values': unique_values.tolist(),
                    'is_boolean_dtype': df[col].dtype == 'bool'
                }
        return analysis
    
    def load_yaml_configs(self) -> Dict[str, Any]:
        """Загрузка YAML конфигураций."""
        logger.info("Загружаю YAML конфигурации...")
        
        yaml_configs = {}
        for entity in self.entities:
            # Правильные имена файлов конфигураций
            config_names = {
                "activities": "config_activity.yaml",
                "assays": "config_assay.yaml", 
                "documents": "config_document.yaml",
                "targets": "config_target.yaml",
                "testitem": "config_testitem.yaml"
            }
            config_file = self.configs_dir / config_names[entity]
            if config_file.exists():
                try:
                    with open(config_file, 'r', encoding='utf-8') as f:
                        config = yaml.safe_load(f)
                    yaml_configs[entity] = config
                    logger.info(f"✅ Загружен {config_file}")
                except Exception as e:
                    logger.error(f"Ошибка загрузки {config_file}: {e}")
            else:
                logger.warning(f"Файл {config_file} не найден")
                
        return yaml_configs
    
    def compare_with_yaml(self, yaml_configs: Dict[str, Any]) -> Dict[str, Any]:
        """Сопоставление фактических выходов с YAML конфигурациями."""
        logger.info("Сопоставляю с YAML конфигурациями...")
        
        comparison_results = {}
        
        for entity in self.entities:
            if entity not in self.results or 'error' in self.results[entity]:
                continue
                
            if entity not in yaml_configs:
                continue
                
            actual_columns = self.results[entity]['columns']
            yaml_columns = yaml_configs[entity].get('determinism', {}).get('column_order', [])
            
            # Анализ несоответствий
            missing_in_output = set(yaml_columns) - set(actual_columns)
            extra_in_output = set(actual_columns) - set(yaml_columns)
            
            # Проверка порядка
            order_matches = actual_columns == yaml_columns
            
            comparison_results[entity] = {
                'yaml_columns': yaml_columns,
                'actual_columns': actual_columns,
                'missing_in_output': list(missing_in_output),
                'extra_in_output': list(extra_in_output),
                'order_matches': order_matches,
                'column_count_yaml': len(yaml_columns),
                'column_count_actual': len(actual_columns)
            }
            
            logger.info(f"✅ {entity}: YAML={len(yaml_columns)}, Actual={len(actual_columns)}, Missing={len(missing_in_output)}, Extra={len(extra_in_output)}")
            
        return comparison_results
    
    def generate_report(self, comparison_results: Dict[str, Any]) -> str:
        """Генерация отчёта о несоответствиях."""
        report = []
        report.append("# Отчёт синхронизации схем и конфигураций")
        report.append("")
        report.append("## Executive Summary")
        report.append("")
        
        total_entities = len(self.entities)
        analyzed_entities = len([e for e in self.entities if e in comparison_results])
        
        report.append(f"- **Entities проверено**: {analyzed_entities}/{total_entities}")
        report.append("")
        
        # Сводная таблица несоответствий
        report.append("## Таблица несоответствий")
        report.append("")
        report.append("| Entity | Config Path | Check Type | Result | Details | Priority | Recommended Action |")
        report.append("|--------|-------------|------------|--------|---------|----------|-------------------|")
        
        for entity, result in comparison_results.items():
            # Определение приоритета
            priority = "P1" if len(result['missing_in_output']) > 0 or len(result['extra_in_output']) > 5 else "P2"
            if not result['order_matches']:
                priority = "P2"
                
            # Детали
            details = []
            if result['missing_in_output']:
                details.append(f"Missing: {len(result['missing_in_output'])} cols")
            if result['extra_in_output']:
                details.append(f"Extra: {len(result['extra_in_output'])} cols")
            if not result['order_matches']:
                details.append("Order mismatch")
                
            details_str = ", ".join(details) if details else "OK"
            
            # Рекомендуемые действия
            actions = []
            if result['missing_in_output']:
                actions.append("Add missing to YAML")
            if result['extra_in_output']:
                actions.append("Remove/map extra")
            if not result['order_matches']:
                actions.append("Fix column order")
                
            action_str = ", ".join(actions) if actions else "No action needed"
            
            report.append(f"| {entity} | configs/config_{entity}.yaml | columns | {'FAIL' if details_str != 'OK' else 'OK'} | {details_str} | {priority} | {action_str} |")
        
        report.append("")
        
        # Детальный анализ по сущностям
        report.append("## Детальный анализ по сущностям")
        report.append("")
        
        for entity, result in comparison_results.items():
            report.append(f"### {entity.title()}")
            report.append("")
            report.append(f"- **YAML колонок**: {result['column_count_yaml']}")
            report.append(f"- **Фактических колонок**: {result['column_count_actual']}")
            report.append(f"- **Порядок совпадает**: {'YES' if result['order_matches'] else 'NO'}")
            
            if result['missing_in_output']:
                report.append(f"- **Отсутствуют в выходе**: {', '.join(result['missing_in_output'][:5])}")
                if len(result['missing_in_output']) > 5:
                    report.append(f"  ... и ещё {len(result['missing_in_output']) - 5}")
                    
            if result['extra_in_output']:
                report.append(f"- **Лишние в выходе**: {', '.join(result['extra_in_output'][:5])}")
                if len(result['extra_in_output']) > 5:
                    report.append(f"  ... и ещё {len(result['extra_in_output']) - 5}")
                    
            report.append("")
        
        return "\n".join(report)
    
    def run_analysis(self) -> str:
        """Запуск полного анализа."""
        logger.info("Запускаю полный анализ синхронизации схем...")
        
        # 1. Анализ фактических выходов
        self.analyze_entity_outputs()
        
        # 2. Загрузка YAML конфигураций
        yaml_configs = self.load_yaml_configs()
        
        # 3. Сопоставление
        comparison_results = self.compare_with_yaml(yaml_configs)
        
        # 4. Генерация отчёта
        report = self.generate_report(comparison_results)
        
        # Сохранение отчёта
        report_path = Path("metadata/reports/schema_sync_analysis.md")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
            
        logger.info(f"Отчёт сохранён: {report_path}")
        
        return report

def main():
    """Основная функция."""
    analyzer = SchemaSyncAnalyzer()
    report = analyzer.run_analysis()
    # Вывод отчёта в файл для избежания проблем с кодировкой
    with open("schema_sync_report.txt", "w", encoding="utf-8") as f:
        f.write(report)
    print("Отчёт сохранён в schema_sync_report.txt")

if __name__ == "__main__":
    main()
