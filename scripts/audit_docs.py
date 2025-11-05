#!/usr/bin/env python3
"""
Аудит документации bioactivity_data_acquisition@refactoring_001

Выявляет противоречия, пробелы, битые ссылки и терминологические несоответствия.
"""

import os
import re
import csv
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
import yaml

# Корневая директория проекта
ROOT = Path(__file__).parent
DOCS = ROOT / "docs"

# Файлы, объявленные в .lychee.toml, но отсутствующие
LYCHEE_MISSING = [
    "docs/architecture/00-architecture-overview.md",
    "docs/architecture/03-data-sources-and-spec.md",
    "docs/pipelines/PIPELINES.md",
    "docs/configs/CONFIGS.md",
    "docs/cli/CLI.md",
    "docs/qc/QA_QC.md",
]

# Список всех пайплайнов для проверки
ALL_PIPELINES = [
    "activity", "assay", "target", "document", "testitem",
    "pubchem", "uniprot", "iuphar", "pubmed", "crossref",
    "openalex", "semantic_scholar", "chembl2uniprot"
]

# 7 обязательных разделов
REQUIRED_SECTIONS = [
    "cli", "config", "schema", "io", "determinism", "qc", "logging"
]

def read_md_file(path: Path) -> str:
    """Читает markdown файл."""
    try:
        return path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"ERROR reading {path}: {e}")
        return ""

def extract_markdown_links(content: str) -> List[Tuple[str, str]]:
    """Извлекает все markdown ссылки из содержимого."""
    # Паттерн для [text](path) или [text](path#anchor)
    pattern = r'\[([^\]]+)\]\(([^)]+)\)'
    links = re.findall(pattern, content)
    return links

def check_file_exists(link_path: str, base_path: Path) -> Tuple[bool, Optional[Path]]:
    """Проверяет существование файла по ссылке."""
    # Убираем якорь из пути
    clean_path = link_path.split("#")[0].split("?")[0]
    
    # Относительный путь от docs/
    if clean_path.startswith("docs/"):
        clean_path = clean_path[5:]  # Убираем "docs/"
    
    # Проверяем относительно base_path
    full_path = base_path / clean_path if not os.path.isabs(clean_path) else Path(clean_path)
    
    # Пробуем несколько вариантов
    candidates = [
        base_path.parent / clean_path,  # От корня проекта
        base_path / clean_path,  # От docs/
        ROOT / clean_path,  # От корня
    ]
    
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return True, candidate
    
    return False, None

def audit_broken_links() -> List[Dict]:
    """Проверяет битые ссылки во всех .md файлах."""
    broken = []
    
    for md_file in DOCS.rglob("*.md"):
        content = read_md_file(md_file)
        links = extract_markdown_links(content)
        
        for link_text, link_path in links:
            # Пропускаем внешние ссылки
            if link_path.startswith("http://") or link_path.startswith("https://") or link_path.startswith("mailto:"):
                continue
            
            # Пропускаем якоря без пути
            if link_path.startswith("#"):
                continue
            
            exists, found_path = check_file_exists(link_path, md_file.parent)
            
            if not exists:
                broken.append({
                    "source": str(md_file.relative_to(ROOT)),
                    "link_text": link_text,
                    "link_path": link_path,
                    "type": "broken_internal_link"
                })
    
    return broken

def find_lychee_missing() -> List[Dict]:
    """Находит файлы, объявленные в .lychee.toml, но отсутствующие."""
    missing = []
    
    for file_path in LYCHEE_MISSING:
        full_path = ROOT / file_path
        if not full_path.exists():
            missing.append({
                "source": ".lychee.toml",
                "file": file_path,
                "type": "declared_but_missing"
            })
    
    return missing

def extract_pipeline_info(pipeline_name: str) -> Dict:
    """Извлекает информацию о пайплайне из документации."""
    info = {
        "pipeline": pipeline_name,
        "doc_path": "",
        "has_cli": False,
        "has_config": False,
        "has_schema": False,
        "has_io": False,
        "has_determinism": False,
        "has_qc": False,
        "has_logging": False,
    }
    
    # Ищем документ пайплайна
    possible_names = [
        f"{pipeline_name}-chembl-extraction.md",
        f"document-{pipeline_name}-extraction.md",
        f"target-{pipeline_name}-extraction.md",
        f"testitem-{pipeline_name}-extraction.md",
        f"{pipeline_name}-extraction.md",
        f"{pipeline_name}.md",
    ]
    
    doc_path = None
    for name in possible_names:
        candidates = list(DOCS.rglob(name))
        if candidates:
            doc_path = candidates[0]
            break
    
    # Также проверяем каталог
    if not doc_path and pipeline_name in ["activity", "assay", "target", "document", "testitem"]:
        catalog = DOCS / "pipelines" / "10-chembl-pipelines-catalog.md"
        if catalog.exists():
            doc_path = catalog
    
    if doc_path:
        info["doc_path"] = str(doc_path.relative_to(ROOT))
        content = read_md_file(doc_path).lower()
        
        # Проверяем наличие разделов
        info["has_cli"] = bool(re.search(r"cli|command|usage|invocation", content))
        info["has_config"] = bool(re.search(r"config|configuration|yaml|profile", content))
        info["has_schema"] = bool(re.search(r"schema|pandera|validation|column_order", content))
        info["has_io"] = bool(re.search(r"input|output|format|csv|parquet", content))
        info["has_determinism"] = bool(re.search(r"determinism|hash_row|hash_business_key|sort|utc", content))
        info["has_qc"] = bool(re.search(r"qc|quality|metric|golden", content))
        info["has_logging"] = bool(re.search(r"log|logging|structured|json|run_id", content))
    
    return info

def main():
    """Основная функция аудита."""
    print("Starting documentation audit...")
    
    # 1. Проверка битых ссылок
    print("\n1. Checking broken links...")
    broken_links = audit_broken_links()
    lychee_missing = find_lychee_missing()
    
    print(f"   Found {len(broken_links)} broken internal links")
    print(f"   Found {len(lychee_missing)} missing files from .lychee.toml")
    
    # 2. Инвентаризация пайплайнов
    print("\n2. Inventorying pipelines...")
    pipeline_info = []
    for pipeline in ALL_PIPELINES:
        info = extract_pipeline_info(pipeline)
        pipeline_info.append(info)
    
    # Сохраняем результаты
    results_dir = ROOT / "audit_results"
    results_dir.mkdir(exist_ok=True)
    
    # GAPS_TABLE.csv
    with open(results_dir / "GAPS_TABLE.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "pipeline", "doc_path", "missing_cli", "missing_config", "missing_schema",
            "missing_io", "missing_determinism", "missing_qc", "missing_logging", "priority"
        ])
        writer.writeheader()
        for info in pipeline_info:
            writer.writerow({
                "pipeline": info["pipeline"],
                "doc_path": info["doc_path"] or "N/A",
                "missing_cli": "Yes" if not info["has_cli"] else "No",
                "missing_config": "Yes" if not info["has_config"] else "No",
                "missing_schema": "Yes" if not info["has_schema"] else "No",
                "missing_io": "Yes" if not info["has_io"] else "No",
                "missing_determinism": "Yes" if not info["has_determinism"] else "No",
                "missing_qc": "Yes" if not info["has_qc"] else "No",
                "missing_logging": "Yes" if not info["has_logging"] else "No",
                "priority": "HIGH" if sum([
                    not info["has_cli"], not info["has_config"], not info["has_schema"]
                ]) >= 2 else "MEDIUM"
            })
    
    # LINKCHECK.md
    with open(results_dir / "LINKCHECK.md", "w", encoding="utf-8") as f:
        f.write("# Link Check Report\n\n")
        f.write("## Missing Files from .lychee.toml\n\n")
        f.write("| Source | File | Type | Criticality |\n")
        f.write("|--------|------|------|-------------|\n")
        for item in lychee_missing:
            f.write(f"| {item['source']} | {item['file']} | {item['type']} | CRITICAL |\n")
        
        f.write("\n## Broken Internal Links\n\n")
        f.write("| Source | Link Text | Link Path | Type | Criticality |\n")
        f.write("|--------|-----------|-----------|------|-------------|\n")
        for item in broken_links[:50]:  # Ограничиваем для читаемости
            f.write(f"| {item['source']} | {item['link_text'][:30]} | {item['link_path']} | {item['type']} | MEDIUM |\n")
    
    print(f"\nResults saved to {results_dir}/")
    print(f"  - GAPS_TABLE.csv")
    print(f"  - LINKCHECK.md")

if __name__ == "__main__":
    main()
