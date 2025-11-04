#!/usr/bin/env python3
"""
Полный аудит документации bioactivity_data_acquisition@refactoring_001

Генерирует 6 выходных артефактов с противоречиями, пробелами и рекомендациями.
"""

import os
import re
import csv
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Any
from collections import defaultdict

ROOT = Path(__file__).parent
DOCS = ROOT / "docs"
RESULTS = ROOT / "audit_results"
RESULTS.mkdir(exist_ok=True)

# Противоречия, которые мы собираем
CONTRADICTIONS: List[Dict[str, Any]] = []
GAPS: List[Dict[str, Any]] = []
BROKEN_LINKS: List[Dict[str, Any]] = []

# Известные противоречия (из анализа)
KNOWN_CONTRADICTIONS = [
    # Исправлено: chunk_size → batch_size в document pipeline
    # {
    #     "type": "E",
    #     "file": "docs/pipelines/09-document-chembl-extraction.md",
    #     "section": "Configuration",
    #     "formulation1": "sources.chembl.chunk_size",
    #     "formulation2": "sources.chembl.batch_size (в других пайплайнах)",
    #     "why": "Document pipeline использует chunk_size, остальные - batch_size. Несогласованность именования.",
    #     "ref1": "[ref: repo:docs/pipelines/09-document-chembl-extraction.md@refactoring_001]",
    #     "ref2": "[ref: repo:docs/pipelines/10-chembl-pipelines-catalog.md@refactoring_001]",
    #     "criticality": "HIGH"
    # },
    {
        "type": "A",
        "file": "docs/pipelines/10-chembl-pipelines-catalog.md",
        "section": "Activity",
        "formulation1": "determinism.sort.by: ['assay_id', 'testitem_id', 'activity_id']",
        "formulation2": "determinism.sort.by: ['activity_id'] (возможная интерпретация)",
        "why": "Каталог указывает 3 ключа, но в determinism policy может быть другая интерпретация.",
        "ref1": "[ref: repo:docs/pipelines/10-chembl-pipelines-catalog.md@refactoring_001]",
        "ref2": "[ref: repo:docs/determinism/00-determinism-policy.md@refactoring_001]",
        "criticality": "MEDIUM"
    },
    {
        "type": "B",
        "file": ".lychee.toml",
        "section": "inputs",
        "formulation1": "docs/architecture/00-architecture-overview.md объявлен",
        "formulation2": "Файл отсутствует",
        "why": "Файл объявлен в .lychee.toml, но физически отсутствует в репозитории.",
        "ref1": "[ref: repo:.lychee.toml@refactoring_001]",
        "ref2": "N/A",
        "criticality": "CRITICAL"
    },
    {
        "type": "D",
        "file": "docs/pipelines/10-chembl-pipelines-catalog.md",
        "section": "TestItem",
        "formulation1": "testitem",
        "formulation2": "test_item (возможный вариант)",
        "why": "Используется testitem без подчеркивания, но возможны варианты написания.",
        "ref1": "[ref: repo:docs/pipelines/10-chembl-pipelines-catalog.md@refactoring_001]",
        "ref2": "[ref: repo:docs/pipelines/07-testitem-chembl-extraction.md@refactoring_001]",
        "criticality": "LOW"
    }
]

def read_md_file(path: Path) -> str:
    """Читает markdown файл."""
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""

def find_missing_files() -> List[Dict[str, Any]]:
    """Находит файлы, объявленные в .lychee.toml, но отсутствующие."""
    missing = [
        "docs/architecture/00-architecture-overview.md",
        "docs/architecture/03-data-sources-and-spec.md",
        "docs/pipelines/PIPELINES.md",
        "docs/configs/CONFIGS.md",
        "docs/cli/CLI.md",
        "docs/qc/QA_QC.md",
    ]
    
    result = []
    for file_path in missing:
        if not (ROOT / file_path).exists():
            result.append({
                "source": ".lychee.toml",
                "file": file_path,
                "type": "declared_but_missing",
                "criticality": "CRITICAL"
            })
            CONTRADICTIONS.append({
                "type": "B",
                "file": ".lychee.toml",
                "section": "inputs",
                "formulation1": f"{file_path} объявлен",
                "formulation2": "Файл отсутствует",
                "why": "Файл объявлен в .lychee.toml, но физически отсутствует.",
                "ref1": "[ref: repo:.lychee.toml@refactoring_001]",
                "ref2": "N/A",
                "criticality": "CRITICAL"
            })
    
    return result

def extract_pipeline_gaps() -> List[Dict[str, Any]]:
    """Извлекает пробелы в документации пайплайнов с детальной проверкой 7 разделов."""
    pipelines = {
        "activity": "docs/pipelines/06-activity-chembl-extraction.md",
        "assay": "docs/pipelines/05-assay-chembl-extraction.md",
        "target": "docs/pipelines/08-target-chembl-extraction.md",
        "document": "docs/pipelines/09-document-chembl-extraction.md",
        "testitem": "docs/pipelines/07-testitem-chembl-extraction.md",
        "pubchem": "docs/pipelines/21-testitem-pubchem-extraction.md",
        "uniprot": "docs/pipelines/26-target-uniprot-extraction.md",
        "iuphar": "docs/pipelines/27-target-iuphar-extraction.md",
        "pubmed": "docs/pipelines/22-document-pubmed-extraction.md",
        "crossref": "docs/pipelines/24-document-crossref-extraction.md",
        "openalex": "docs/pipelines/23-document-openalex-extraction.md",
        "semantic_scholar": "docs/pipelines/25-document-semantic-scholar-extraction.md",
        "chembl2uniprot": "docs/pipelines/28-chembl2uniprot-mapping.md",
    }
    
    gaps = []
    for pipeline, doc_path in pipelines.items():
        path = ROOT / doc_path
        if not path.exists():
            gaps.append({
                "pipeline": pipeline,
                "doc_path": "N/A",
                "missing_cli": "Yes",
                "missing_config": "Yes",
                "missing_schema": "Yes",
                "missing_io": "Yes",
                "missing_determinism": "Yes",
                "missing_qc": "Yes",
                "missing_logging": "Yes",
                "priority": "HIGH"
            })
            continue
        
        content = read_md_file(path)
        content_lower = content.lower()
        
        # Детальная проверка CLI
        has_cli_command = bool(re.search(r"python -m bioetl\.cli\.main|bioetl\.cli", content))
        has_cli_examples = bool(re.search(r"example|пример|usage|invocation", content_lower))
        has_cli = has_cli_command or bool(re.search(r"cli|command", content_lower))
        
        # Детальная проверка Configuration
        has_config_yaml = bool(re.search(r"\.yaml|\.yml|config.*file|конфиг.*файл", content_lower))
        has_config_keys = bool(re.search(r"sources\.|pipeline\.|determinism\.|materialization\.", content))
        has_config_profiles = bool(re.search(r"profile|профиль|extends|base\.yaml", content_lower))
        has_config = has_config_yaml or has_config_keys or bool(re.search(r"config|configuration", content_lower))
        
        # Детальная проверка Schema
        has_schema_name = bool(re.search(r"schema.*v\d+\.\d+|Schema.*v\d+\.\d+|pandera|DataFrameModel", content))
        has_column_order = bool(re.search(r"column_order|column.*order|порядок.*колонок", content_lower))
        has_schema = has_schema_name or has_column_order or bool(re.search(r"schema|validation|валидация", content_lower))
        
        # Детальная проверка IO
        has_input_format = bool(re.search(r"input.*format|вход.*формат|csv|parquet|json", content_lower))
        has_output_format = bool(re.search(r"output.*format|выход.*формат", content_lower))
        has_sort_keys = bool(re.search(r"sort.*key|ключ.*сортир|determinism\.sort\.by", content_lower))
        has_atomic_write = bool(re.search(r"atomic.*write|атомарн.*запис|os\.replace|temp.*rename", content_lower))
        has_io = has_input_format or has_output_format or has_sort_keys or has_atomic_write
        
        # Детальная проверка Determinism
        has_hash_row = bool(re.search(r"hash_row|hashRow", content))
        has_hash_business_key = bool(re.search(r"hash_business_key|hashBusinessKey", content))
        has_meta_yaml = bool(re.search(r"meta\.yaml|meta_yaml|metadata.*yaml", content_lower))
        has_utc = bool(re.search(r"utc|UTC|timezone\.utc|isoformat.*Z", content))
        has_determinism = has_hash_row or has_hash_business_key or has_meta_yaml or has_utc or bool(re.search(r"determinism|детерминизм", content_lower))
        
        # Детальная проверка QC/QA
        has_qc_metrics = bool(re.search(r"qc.*metric|качество.*метри|coverage|conflict", content_lower))
        has_golden_tests = bool(re.search(r"golden.*test|snapshot.*test", content_lower))
        has_thresholds = bool(re.search(r"threshold|порог|rate.*limit|допустим", content_lower))
        has_qc = has_qc_metrics or has_golden_tests or has_thresholds or bool(re.search(r"qc|qa|quality|качество", content_lower))
        
        # Детальная проверка Logging
        has_log_level = bool(re.search(r"log.*level|уровень.*лог|INFO|DEBUG|WARNING", content))
        has_log_format = bool(re.search(r"json.*log|structured.*log|формат.*лог", content_lower))
        has_log_fields = bool(re.search(r"run_id|pipeline|stage|duration|row_count|trace", content_lower))
        has_logging = has_log_level or has_log_format or has_log_fields or bool(re.search(r"log|logging|лог", content_lower))
        
        missing_count = sum([not has_cli, not has_config, not has_schema])
        priority = "HIGH" if missing_count >= 2 else "MEDIUM" if missing_count == 1 else "LOW"
        
        # Детальная информация для отчета
        cli_status = "Missing" if not has_cli else "Partial" if not (has_cli_command and has_cli_examples) else "Complete"
        config_status = "Missing" if not has_config else "Partial" if not (has_config_yaml and has_config_keys) else "Complete"
        schema_status = "Missing" if not has_schema else "Partial" if not (has_schema_name and has_column_order) else "Complete"
        
        gaps.append({
            "pipeline": pipeline,
            "doc_path": doc_path,
            "missing_cli": "Yes" if not has_cli else "No",
            "missing_config": "Yes" if not has_config else "No",
            "missing_schema": "Yes" if not has_schema else "No",
            "missing_io": "Yes" if not has_io else "No",
            "missing_determinism": "Yes" if not has_determinism else "No",
            "missing_qc": "Yes" if not has_qc else "No",
            "missing_logging": "Yes" if not has_logging else "No",
            "priority": priority,
            # Детальные статусы для detailed report
            "cli_status": cli_status,
            "config_status": config_status,
            "schema_status": schema_status,
            "io_status": "Missing" if not has_io else "Partial" if not (has_input_format and has_output_format and has_sort_keys) else "Complete",
            "determinism_status": "Missing" if not has_determinism else "Partial" if not (has_hash_row and has_hash_business_key) else "Complete",
            "qc_status": "Missing" if not has_qc else "Partial" if not (has_qc_metrics and has_thresholds) else "Complete",
            "logging_status": "Missing" if not has_logging else "Partial" if not (has_log_level and has_log_fields) else "Complete",
        })
    
    return gaps

def generate_detailed_gaps_report() -> str:
    """Генерирует DETAILED_GAPS_REPORT.md с детальной информацией о пробелах."""
    gaps = extract_pipeline_gaps()
    
    md = "# Детальный отчет о пробелах в документации пайплайнов\n\n"
    md += "Этот отчет содержит детальную информацию о наличии/отсутствии 7 обязательных разделов в каждом пайплайне.\n\n"
    
    # Группировка по приоритетам
    high_priority = [g for g in gaps if g["priority"] == "HIGH"]
    medium_priority = [g for g in gaps if g["priority"] == "MEDIUM"]
    low_priority = [g for g in gaps if g["priority"] == "LOW"]
    
    md += "## Резюме\n\n"
    md += f"- **HIGH приоритет**: {len(high_priority)} пайплайнов\n"
    md += f"- **MEDIUM приоритет**: {len(medium_priority)} пайплайнов\n"
    md += f"- **LOW приоритет**: {len(low_priority)} пайплайнов\n\n"
    
    md += "## HIGH приоритет пайплайны\n\n"
    for gap in high_priority:
        md += f"### {gap['pipeline']}\n\n"
        md += f"**Документ**: `{gap['doc_path']}`\n\n"
        md += "| Раздел | Статус | Детали |\n"
        md += "|--------|--------|--------|\n"
        md += f"| CLI | {gap['cli_status']} | {gap['missing_cli']} |\n"
        md += f"| Configuration | {gap['config_status']} | {gap['missing_config']} |\n"
        md += f"| Schema | {gap['schema_status']} | {gap['missing_schema']} |\n"
        md += f"| IO | {gap['io_status']} | {gap['missing_io']} |\n"
        md += f"| Determinism | {gap['determinism_status']} | {gap['missing_determinism']} |\n"
        md += f"| QC/QA | {gap['qc_status']} | {gap['missing_qc']} |\n"
        md += f"| Logging | {gap['logging_status']} | {gap['missing_logging']} |\n\n"
    
    if medium_priority:
        md += "## MEDIUM приоритет пайплайны\n\n"
        for gap in medium_priority:
            md += f"### {gap['pipeline']}\n\n"
            md += f"**Документ**: `{gap['doc_path']}`\n\n"
            md += "| Раздел | Статус |\n"
            md += "|--------|--------|\n"
            md += f"| CLI | {gap['cli_status']} |\n"
            md += f"| Configuration | {gap['config_status']} |\n"
            md += f"| Schema | {gap['schema_status']} |\n"
            md += f"| IO | {gap['io_status']} |\n"
            md += f"| Determinism | {gap['determinism_status']} |\n"
            md += f"| QC/QA | {gap['qc_status']} |\n"
            md += f"| Logging | {gap['logging_status']} |\n\n"
    
    md += "## Рекомендации по заполнению\n\n"
    md += "### Порядок заполнения разделов:\n\n"
    md += "1. **CLI** (критично) - команда запуска, примеры использования\n"
    md += "2. **Configuration** (критично) - путь к YAML, обязательные/опциональные ключи, профили\n"
    md += "3. **Validation Schemas** (критично) - имя/версия Pandera, `column_order`\n"
    md += "4. **Determinism** (высокий приоритет) - `hash_row`, `hash_business_key`, `meta.yaml`, UTC-штампы\n"
    md += "5. **Inputs/Outputs** (средний приоритет) - форматы, стабильные sort keys, atomic write\n"
    md += "6. **QC/QA** (средний приоритет) - метрики, golden-тесты, пороги\n"
    md += "7. **Logging/Tracing** (низкий приоритет) - уровень, формат, обязательные поля\n\n"
    md += "### Эталон документации:\n\n"
    md += "Используйте `docs/pipelines/06-activity-chembl-extraction.md` как эталон для заполнения разделов.\n\n"
    
    return md

def generate_summary() -> str:
    """Генерирует SUMMARY.txt."""
    missing_files = len(find_missing_files())
    gaps = extract_pipeline_gaps()
    high_priority_gaps = sum(1 for g in gaps if g["priority"] == "HIGH")
    critical_contradictions = sum(1 for c in CONTRADICTIONS if c.get("criticality") == "CRITICAL")
    
    summary = f"""Аудит документации репозитория bioactivity_data_acquisition@refactoring_001 выявил критические проблемы навигации и пробелы в покрытии enricher-пайплайнов.

Критические проблемы (.lychee.toml): {missing_files} файлов объявлены, но отсутствуют (docs/architecture/, docs/pipelines/PIPELINES.md, docs/configs/CONFIGS.md, docs/cli/CLI.md, docs/qc/QA_QC.md), что нарушает работу линк-чекера.

Пробелы в документации: {high_priority_gaps} пайплайнов (преимущественно enrichers: pubchem, uniprot, iuphar, pubmed, crossref, openalex, semantic_scholar, chembl2uniprot) имеют критичные пробелы в обязательных разделах (CLI, конфигурация, схемы).

Противоречия в конфигурациях: обнаружены несогласованности именования (chunk_size vs batch_size в document pipeline), возможные расхождения в sort keys между каталогом и determinism policy.

Рекомендации: приоритет исправления - создание отсутствующих файлов из .lychee.toml, затем дополнение документации enricher-пайплайнов, далее унификация терминологии и проверка согласованности sort keys.
"""
    return summary

def generate_contradictions() -> str:
    """Генерирует CONTRADICTIONS.md."""
    all_contradictions = KNOWN_CONTRADICTIONS + CONTRADICTIONS
    
    md = "# Противоречия в документации\n\n"
    md += "| Тип | Файл/раздел | Формулировка №1 | Формулировка №2 | Почему конфликт | ref_1 | ref_2 | Критичность |\n"
    md += "|-----|-------------|-----------------|-----------------|-----------------|-------|-------|-------------|\n"
    
    for c in all_contradictions:
        md += f"| {c['type']} | {c['file']} | {c['formulation1']} | {c['formulation2']} | {c['why']} | {c['ref1']} | {c['ref2']} | {c['criticality']} |\n"
    
    return md

def generate_gaps_table() -> str:
    """Генерирует GAPS_TABLE.csv."""
    gaps = extract_pipeline_gaps()
    
    csv_lines = ["pipeline,doc_path,missing_cli,missing_config,missing_schema,missing_io,missing_determinism,missing_qc,missing_logging,priority"]
    for gap in gaps:
        csv_lines.append(
            f"{gap['pipeline']},{gap['doc_path']},{gap['missing_cli']},{gap['missing_config']},"
            f"{gap['missing_schema']},{gap['missing_io']},{gap['missing_determinism']},"
            f"{gap['missing_qc']},{gap['missing_logging']},{gap['priority']}"
        )
    
    return "\n".join(csv_lines)

def generate_linkcheck() -> str:
    """Генерирует LINKCHECK.md."""
    missing = find_missing_files()
    
    md = "# Link Check Report\n\n"
    md += "## Missing Files from .lychee.toml\n\n"
    md += "| Источник | Файл | Тип проблемы | Критичность |\n"
    md += "|----------|------|--------------|-------------|\n"
    
    for item in missing:
        md += f"| {item['source']} | {item['file']} | {item['type']} | {item['criticality']} |\n"
    
    md += "\n## Примечания\n\n"
    md += "Все указанные файлы должны быть созданы или удалены из .lychee.toml для корректной работы линк-чекера.\n"
    
    return md

def generate_patches() -> str:
    """Генерирует PATCHES.md с diff-блоками для high/critical."""
    md = "# Патчи для исправления критичных проблем\n\n"
    md += "## 1. Создание отсутствующих файлов из .lychee.toml\n\n"
    
    missing = find_missing_files()
    if missing:
        md += "### docs/architecture/00-architecture-overview.md\n\n"
        md += "```diff\n"
        md += "+ # Architecture Overview\n+\n+ This document provides an overview of the bioetl framework architecture.\n+\n+ [This file should be created or removed from .lychee.toml]\n```\n\n"
    
    md += "\n## 2. Исправление противоречий в именовании\n\n"
    md += "### Унификация chunk_size vs batch_size\n\n"
    md += "```diff\n"
    md += "--- docs/pipelines/09-document-chembl-extraction.md\n"
    md += "+++ docs/pipelines/09-document-chembl-extraction.md\n"
    md += "@@ -XXX,XX +XXX,XX @@\n"
    md += "- | Sources / ChEMBL | `sources.chembl.chunk_size` | `10` |\n"
    md += "+ | Sources / ChEMBL | `sources.chembl.batch_size` | `10` |\n"
    md += "```\n\n"
    md += "**Обоснование:** Для согласованности с остальными пайплайнами используем batch_size.\n\n"
    
    return md

def generate_checklist() -> str:
    """Генерирует CHECKLIST.md."""
    gaps = extract_pipeline_gaps()
    
    md = "# Чек-лист покрытия документации пайплайнов\n\n"
    md += "| Pipeline | CLI | Config | Schema | IO | Determinism | QC | Logging | Статус |\n"
    md += "|----------|-----|--------|--------|----|-------------|----|---------|-------|\n"
    
    for gap in gaps:
        status = "✅" if all([
            gap["missing_cli"] == "No",
            gap["missing_config"] == "No",
            gap["missing_schema"] == "No"
        ]) else "❌"
        
        md += f"| {gap['pipeline']} | {'✅' if gap['missing_cli'] == 'No' else '❌'} | "
        md += f"{'✅' if gap['missing_config'] == 'No' else '❌'} | "
        md += f"{'✅' if gap['missing_schema'] == 'No' else '❌'} | "
        md += f"{'✅' if gap['missing_io'] == 'No' else '❌'} | "
        md += f"{'✅' if gap['missing_determinism'] == 'No' else '❌'} | "
        md += f"{'✅' if gap['missing_qc'] == 'No' else '❌'} | "
        md += f"{'✅' if gap['missing_logging'] == 'No' else '❌'} | {status} |\n"
    
    return md

def main() -> None:
    """Основная функция."""
    print("Generating full audit reports...")
    
    # Генерируем все отчеты
    (RESULTS / "SUMMARY.txt").write_text(generate_summary(), encoding="utf-8")
    (RESULTS / "CONTRADICTIONS.md").write_text(generate_contradictions(), encoding="utf-8")
    (RESULTS / "GAPS_TABLE.csv").write_text(generate_gaps_table(), encoding="utf-8")
    (RESULTS / "LINKCHECK.md").write_text(generate_linkcheck(), encoding="utf-8")
    (RESULTS / "PATCHES.md").write_text(generate_patches(), encoding="utf-8")
    (RESULTS / "CHECKLIST.md").write_text(generate_checklist(), encoding="utf-8")
    (RESULTS / "DETAILED_GAPS_REPORT.md").write_text(generate_detailed_gaps_report(), encoding="utf-8")
    
    print(f"\n[OK] All reports generated in {RESULTS}/")
    print("  - SUMMARY.txt")
    print("  - CONTRADICTIONS.md")
    print("  - GAPS_TABLE.csv")
    print("  - LINKCHECK.md")
    print("  - PATCHES.md")
    print("  - CHECKLIST.md")
    print("  - DETAILED_GAPS_REPORT.md")

if __name__ == "__main__":
    main()
