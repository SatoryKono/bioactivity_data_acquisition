#!/usr/bin/env python3
"""
Аудит документации BioETL на противоречия и пробелы.
Генерирует отчеты: SUMMARY.txt, CONTRADICTIONS.md, GAPS_TABLE.csv, LINKCHECK.md, PATCHES.md, CHECKLIST.md
"""

import re
import csv
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
from dataclasses import dataclass, field
from urllib.parse import urlparse

# Константы
DOCS_DIR = Path("docs")
REQUIRED_SECTIONS = [
    "CLI",
    "Конфигурация",
    "Схемы валидации",
    "Входы/выходы",
    "Детерминизм",
    "QC/QA",
    "Логирование и трассировка",
]

PIPELINE_PAGES = {
    "activity": "docs/pipelines/06-activity-data-extraction.md",
    "assay": "docs/pipelines/05-assay-extraction.md",
    "target": "docs/pipelines/08-target-data-extraction.md",
    "document": "docs/pipelines/09-document-chembl-extraction.md",
    "testitem": "docs/pipelines/07-testitem-extraction.md",
}


@dataclass
class LinkInfo:
    """Информация о ссылке"""
    source_file: str
    link_text: str
    target: str
    is_anchor: bool = False
    is_external: bool = False
    exists: bool = False


@dataclass
class Contradiction:
    """Противоречие в документации"""
    type: str  # A-G
    section: str
    file1: str
    formulation1: str
    file2: str
    formulation2: str
    reason: str
    ref_doc1: str
    ref_doc2: str
    criticality: str


@dataclass
class GapInfo:
    """Информация о пробеле"""
    pipeline: str
    doc_path: str
    missing_cli: bool
    missing_config: bool
    missing_schema: bool
    missing_io: bool
    missing_determinism: bool
    missing_qc: bool
    missing_logging: bool
    priority: str


def extract_markdown_links(content: str, file_path: Path) -> List[LinkInfo]:
    """Извлекает все ссылки из markdown контента"""
    links = []
    
    # Паттерны для ссылок
    # [text](link)
    pattern1 = r'\[([^\]]+)\]\(([^)]+)\)'
    # [ref: repo:path]
    pattern2 = r'\[ref:\s*repo:([^\]]+)\]'
    
    for match in re.finditer(pattern1, content):
        link_text = match.group(1)
        target = match.group(2)
        
        # Определяем тип ссылки
        is_anchor = target.startswith("#")
        is_external = bool(urlparse(target).netloc) and not target.startswith("docs/")
        
        # Проверяем существование для относительных ссылок
        exists = True
        if not is_external and not is_anchor:
            if target.startswith("/"):
                target_path = DOCS_DIR / target[1:]
            elif target.startswith("docs/"):
                target_path = Path(target)
            else:
                target_path = file_path.parent / target
            
            # Проверяем якорь
            if "#" in str(target_path):
                target_file, anchor = str(target_path).split("#", 1)
                target_path = Path(target_file)
                exists = target_path.exists()
                is_anchor = True
            else:
                exists = target_path.exists()
        
        links.append(LinkInfo(
            source_file=str(file_path),
            link_text=link_text,
            target=target,
            is_anchor=is_anchor,
            is_external=is_external,
            exists=exists
        ))
    
    return links


def check_section_presence(content: str, section_name: str) -> bool:
    """Проверяет наличие раздела в документе"""
    # Ищем заголовки уровня 1-3
    patterns = [
        rf'^#+\s+{re.escape(section_name)}',
        rf'^##+\s+.*{re.escape(section_name)}',
        rf'^###+\s+.*{re.escape(section_name)}',
    ]
    
    for pattern in patterns:
        if re.search(pattern, content, re.MULTILINE | re.IGNORECASE):
            return True
    
    # Также проверяем вариации на английском
    english_variants = {
        "CLI": ["CLI", "Command", "Usage", "Invocation"],
        "Конфигурация": ["Configuration", "Config", "Configs"],
        "Схемы валидации": ["Schema", "Validation", "Pandera"],
        "Входы/выходы": ["Input", "Output", "Inputs", "Outputs", "I/O"],
        "Детерминизм": ["Determinism", "Deterministic"],
        "QC/QA": ["QC", "Quality", "Quality Control", "QA"],
        "Логирование и трассировка": ["Logging", "Tracing", "Trace"],
    }
    
    if section_name in english_variants:
        for variant in english_variants[section_name]:
            if re.search(rf'^#+\s+.*{re.escape(variant)}', content, re.MULTILINE | re.IGNORECASE):
                return True
    
    return False


def extract_cli_command(content: str) -> Optional[str]:
    """Извлекает CLI команду из документа"""
    # Ищем паттерны типа "python -m bioetl.cli.main <command>"
    pattern = r'python\s+-m\s+bioetl\.cli\.main\s+(\w+)'
    match = re.search(pattern, content)
    if match:
        return match.group(1)
    
    # Ищем в заголовках
    pattern = r'^#+\s+.*?\(`(\w+)`\)'
    match = re.search(pattern, content, re.MULTILINE)
    if match:
        return match.group(1)
    
    return None


def extract_config_path(content: str) -> Optional[str]:
    """Извлекает путь к конфигу из документа"""
    # Ищем пути типа configs/pipelines/chembl/activity.yaml
    pattern = r'configs/pipelines/[\w/]+\.yaml'
    match = re.search(pattern, content)
    if match:
        return match.group(0)
    
    return None


def build_documentation_map() -> Dict:
    """Строит карту документации"""
    print("Построение карты документации...")
    
    map_data = {
        "index_files": [],
        "pipelines": {},
        "links": [],
        "references": defaultdict(list),
    }
    
    # Парсим INDEX.md
    index_path = DOCS_DIR / "INDEX.md"
    if index_path.exists():
        index_content = index_path.read_text(encoding="utf-8")
        map_data["index_files"] = extract_markdown_links(index_content, index_path)
    
    # Парсим каталог пайплайнов
    catalog_path = DOCS_DIR / "pipelines" / "10-chembl-pipelines-catalog.md"
    if catalog_path.exists():
        catalog_content = catalog_path.read_text(encoding="utf-8")
        
        # Извлекаем упоминания пайплайнов
        for pipeline_name in ["activity", "assay", "target", "document", "testitem"]:
            if pipeline_name in catalog_content.lower():
                map_data["pipelines"][pipeline_name] = {
                    "mentioned_in_catalog": True,
                    "page_path": PIPELINE_PAGES.get(pipeline_name),
                }
    
    return map_data


def inventory_pipelines() -> List[GapInfo]:
    """Инвентаризирует страницы пайплайнов"""
    print("Инвентаризация пайплайнов...")
    
    gaps = []
    
    for pipeline_name, page_path in PIPELINE_PAGES.items():
        page_file = Path(page_path)
        
        if not page_file.exists():
            gaps.append(GapInfo(
                pipeline=pipeline_name,
                doc_path=page_path,
                missing_cli=True,
                missing_config=True,
                missing_schema=True,
                missing_io=True,
                missing_determinism=True,
                missing_qc=True,
                missing_logging=True,
                priority="HIGH"
            ))
            continue
        
        content = page_file.read_text(encoding="utf-8")
        
        # Проверяем наличие разделов
        has_cli = check_section_presence(content, "CLI") or extract_cli_command(content) is not None
        has_config = check_section_presence(content, "Конфигурация") or extract_config_path(content) is not None
        has_schema = check_section_presence(content, "Схемы валидации") or "schema" in content.lower() or "pandera" in content.lower()
        has_io = check_section_presence(content, "Входы/выходы") or "input" in content.lower() or "output" in content.lower()
        has_determinism = check_section_presence(content, "Детерминизм") or "hash_row" in content or "hash_business_key" in content
        has_qc = check_section_presence(content, "QC/QA") or "quality" in content.lower() or "qc" in content.lower()
        has_logging = check_section_presence(content, "Логирование и трассировка") or "logging" in content.lower() or "trace" in content.lower()
        
        # Определяем приоритет
        missing_count = sum([
            not has_cli, not has_config, not has_schema,
            not has_io, not has_determinism, not has_qc, not has_logging
        ])
        
        priority = "HIGH" if missing_count >= 4 else "MEDIUM" if missing_count >= 2 else "LOW"
        
        gaps.append(GapInfo(
            pipeline=pipeline_name,
            doc_path=page_path,
            missing_cli=not has_cli,
            missing_config=not has_config,
            missing_schema=not has_schema,
            missing_io=not has_io,
            missing_determinism=not has_determinism,
            missing_qc=not has_qc,
            missing_logging=not has_logging,
            priority=priority
        ))
    
    return gaps


def check_links() -> List[LinkInfo]:
    """Проверяет все ссылки в документации"""
    print("Проверка ссылок...")
    
    broken_links = []
    all_links = []
    
    # Проходим по всем markdown файлам
    for md_file in DOCS_DIR.rglob("*.md"):
        try:
            content = md_file.read_text(encoding="utf-8")
            links = extract_markdown_links(content, md_file)
            
            for link in links:
                all_links.append(link)
                
                # Пропускаем внешние ссылки (они будут проверены отдельно)
                if link.is_external:
                    continue
                
                # Проверяем существование файла/якоря
                if not link.exists:
                    broken_links.append(link)
        except Exception as e:
            print(f"Ошибка при обработке {md_file}: {e}")
    
    return broken_links


def find_contradictions() -> List[Contradiction]:
    """Находит противоречия в документации"""
    print("Поиск противоречий...")
    
    contradictions = []
    
    # Проверяем CLI команды между разными источниками
    cli_commands_by_source = {}
    
    # Читаем CLI обзор
    cli_overview = DOCS_DIR / "cli" / "00-cli-overview.md"
    if cli_overview.exists():
        content = cli_overview.read_text(encoding="utf-8")
        for match in re.finditer(r'`(\w+)`.*?`python -m bioetl\.cli\.main (\w+)`', content):
            command_name = match.group(2)
            cli_commands_by_source[("cli_overview", command_name)] = match.group(0)
    
    # Читаем CLI commands
    cli_commands_doc = DOCS_DIR / "cli" / "01-cli-commands.md"
    if cli_commands_doc.exists():
        content = cli_commands_doc.read_text(encoding="utf-8")
        for match in re.finditer(r'### `(\w+)`', content):
            command_name = match.group(1)
            cli_commands_by_source[("cli_commands", command_name)] = command_name
    
    # Сравниваем с README
    readme_path = Path("README.md")
    if readme_path.exists():
        readme_content = readme_path.read_text(encoding="utf-8")
        
        # Извлекаем CLI команды из README (из таблицы)
        for pipeline in ["activity", "assay", "target", "document", "testitem"]:
            # Ищем в таблице: | Activity | ... | `bioetl.cli.main activity` | ...
            pattern = rf'\|.*?{pipeline.capitalize()}.*?\|.*?`bioetl\.cli\.main\s+(\w+)`'
            match = re.search(pattern, readme_content, re.IGNORECASE)
            if match:
                readme_command = match.group(1)
                cli_commands_by_source[("README", pipeline)] = readme_command
    
    # Проверяем каталог пайплайнов
    catalog_path = DOCS_DIR / "pipelines" / "10-chembl-pipelines-catalog.md"
    if catalog_path.exists():
        catalog_content = catalog_path.read_text(encoding="utf-8")
        for pipeline in ["activity", "assay", "target", "document", "testitem"]:
            catalog_pattern = rf'### {pipeline.capitalize()}\s*\(`{pipeline}`\).*?python -m bioetl\.cli\.main\s+(\w+)'
            catalog_match = re.search(catalog_pattern, catalog_content, re.DOTALL)
            if catalog_match:
                catalog_command = catalog_match.group(1)
                cli_commands_by_source[("catalog", pipeline)] = catalog_command
                
                # Сравниваем с README
                if ("README", pipeline) in cli_commands_by_source:
                    readme_cmd = cli_commands_by_source[("README", pipeline)]
                    if readme_cmd != catalog_command:
                        contradictions.append(Contradiction(
                            type="D",
                            section="CLI команда",
                            file1="README.md",
                            formulation1=f"`bioetl.cli.main {readme_cmd}`",
                            file2="docs/pipelines/10-chembl-pipelines-catalog.md",
                            formulation2=f"`bioetl.cli.main {catalog_command}`",
                            reason=f"Расхождение в названии CLI команды для пайплайна {pipeline}",
                            ref_doc1="README.md",
                            ref_doc2="docs/pipelines/10-chembl-pipelines-catalog.md",
                            criticality="HIGH"
                        ))
    
    # Проверяем пути к конфигам
    config_paths_by_source = {}
    
    # Из README
    if readme_path.exists():
        readme_content = readme_path.read_text(encoding="utf-8")
        # Ищем пути в формате [ref: repo:src/bioetl/configs/pipelines/chembl/activity.yaml@refactoring_001]
        for match in re.finditer(r'\[ref:\s*repo:src/bioetl/configs/(pipelines/chembl/(\w+)\.yaml)', readme_content):
            pipeline = match.group(2)
            config_paths_by_source[("README", pipeline)] = match.group(1)
    
    # Из каталога
    if catalog_path.exists():
        catalog_content = catalog_path.read_text(encoding="utf-8")
        for match in re.finditer(r'`(configs/pipelines/chembl/(\w+)\.yaml)`', catalog_content):
            pipeline = match.group(2)
            config_paths_by_source[("catalog", pipeline)] = match.group(1)
            
            # Сравниваем с README
            if ("README", pipeline) in config_paths_by_source:
                readme_path_str = config_paths_by_source[("README", pipeline)]
                catalog_path_str = config_paths_by_source[("catalog", pipeline)]
                if readme_path_str != catalog_path_str and "configs/" not in readme_path_str:
                    contradictions.append(Contradiction(
                        type="E",
                        section="Путь к конфигу",
                        file1="README.md",
                        formulation1=readme_path_str,
                        file2="docs/pipelines/10-chembl-pipelines-catalog.md",
                        formulation2=catalog_path_str,
                        reason=f"Расхождение в пути к конфигурационному файлу для пайплайна {pipeline}",
                        ref_doc1="README.md",
                        ref_doc2="docs/pipelines/10-chembl-pipelines-catalog.md",
                        criticality="MEDIUM"
                    ))
    
    # Проверяем архитектурные инварианты - стадии пайплайна
    stages_in_docs = {}
    
    # Из ETL overview
    etl_overview = DOCS_DIR / "etl_contract" / "00-etl-overview.md"
    if etl_overview.exists():
        content = etl_overview.read_text(encoding="utf-8")
        stages_match = re.search(r'extract.*?transform.*?validate.*?write', content, re.IGNORECASE)
        if stages_match:
            stages_in_docs["etl_overview"] = "extract → transform → validate → write"
    
    # Из pipeline contract
    pipeline_contract = DOCS_DIR / "etl_contract" / "01-pipeline-contract.md"
    if pipeline_contract.exists():
        content = pipeline_contract.read_text(encoding="utf-8")
        stages_match = re.search(r'extract.*?transform.*?validate.*?write.*?run', content, re.IGNORECASE)
        if stages_match:
            stages_in_docs["pipeline_contract"] = "extract → transform → validate → write → run"
    
    # Из pipeline base
    pipeline_base = DOCS_DIR / "pipelines" / "00-pipeline-base.md"
    if pipeline_base.exists():
        content = pipeline_base.read_text(encoding="utf-8")
        stages_match = re.search(r'extract.*?transform.*?validate.*?write', content, re.IGNORECASE)
        if stages_match:
            stages_in_docs["pipeline_base"] = "extract → transform → validate → write"
    
    # Проверяем противоречия в формулировках стадий
    if len(stages_in_docs) > 1:
        stages_list = list(stages_in_docs.values())
        if len(set(stages_list)) > 1:
            contradictions.append(Contradiction(
                type="F",
                section="Архитектурные инварианты: стадии пайплайна",
                file1="docs/etl_contract/01-pipeline-contract.md",
                formulation1=stages_in_docs.get("pipeline_contract", "N/A"),
                file2="docs/etl_contract/00-etl-overview.md",
                formulation2=stages_in_docs.get("etl_overview", "N/A"),
                reason="Расхождение в формулировке стадий пайплайна (с run или без)",
                ref_doc1="docs/etl_contract/01-pipeline-contract.md",
                ref_doc2="docs/etl_contract/00-etl-overview.md",
                criticality="HIGH"
            ))
    
    # Проверяем sort keys между каталогом и детерминизмом
    sort_keys_by_source = {}
    
    # Из каталога пайплайнов - ищем в таблице Determinism & Invariant Matrix
    if catalog_path.exists():
        catalog_content = catalog_path.read_text(encoding="utf-8")
        # Ищем таблицу с sort keys - ищем строки вида | Activity | `['assay_id', 'testitem_id', 'activity_id']` |
        for pipeline in ["activity", "assay", "target", "document", "testitem"]:
            # Более точный паттерн: ищем строку таблицы с названием пайплайна
            # Для Activity и Assay - обычное название, для TestItem - TestItem
            pipeline_name_map = {
                "activity": "Activity",
                "assay": "Assay", 
                "target": "Target",
                "document": "Document",
                "testitem": "TestItem"
            }
            pipeline_name = pipeline_name_map.get(pipeline, pipeline.capitalize())
            
            # Ищем строку таблицы: | Activity | `['assay_id', ...]` | ...
            pattern = rf'\|{pipeline_name}\s*\|.*?`(\[.*?\])`.*?\|'
            match = re.search(pattern, catalog_content, re.DOTALL)
            if match:
                sort_keys_by_source[("catalog", pipeline)] = match.group(1)
    
    # Из детерминизма - ищем в таблице Stable Sort Keys by Pipeline
    determinism_policy = DOCS_DIR / "determinism" / "01-determinism-policy.md"
    if determinism_policy.exists():
        content = determinism_policy.read_text(encoding="utf-8")
        for pipeline in ["activity", "assay", "target", "document", "testitem"]:
            # Паттерн для таблицы: | **`activity`** | `["assay_id", "testitem_id", "activity_id"]` | ...
            pattern = rf'\|.*?\*\*`{pipeline}`\*\*.*?\|.*?`(\[.*?\])`.*?\|'
            match = re.search(pattern, content, re.DOTALL)
            if match:
                sort_keys_by_source[("determinism", pipeline)] = match.group(1)
                
                # Сравниваем с каталогом - нормализуем кавычки для сравнения
                if ("catalog", pipeline) in sort_keys_by_source:
                    catalog_keys = sort_keys_by_source[("catalog", pipeline)].replace("'", '"')
                    determinism_keys = sort_keys_by_source[("determinism", pipeline)]
                    # Убираем пробелы и сравниваем содержимое
                    catalog_normalized = re.sub(r'\s+', '', catalog_keys)
                    determinism_normalized = re.sub(r'\s+', '', determinism_keys)
                    if catalog_normalized != determinism_normalized:
                        contradictions.append(Contradiction(
                            type="F",
                            section="Архитектурные инварианты: sort keys",
                            file1="docs/pipelines/10-chembl-pipelines-catalog.md",
                            formulation1=f"sort keys: {catalog_keys}",
                            file2="docs/determinism/01-determinism-policy.md",
                            formulation2=f"sort keys: {determinism_keys}",
                            reason=f"Расхождение в sort keys для пайплайна {pipeline}",
                            ref_doc1="docs/pipelines/10-chembl-pipelines-catalog.md",
                            ref_doc2="docs/determinism/01-determinism-policy.md",
                            criticality="HIGH"
                        ))
    
    return contradictions


def normalize_terms() -> Dict[str, List[str]]:
    """Нормализует термины и находит дубликаты"""
    print("Нормализация терминов...")
    
    term_variants = defaultdict(set)
    
    # Собираем CLI команды из всех источников
    cli_terms = set()
    
    # Читаем все документы и извлекаем термины
    for md_file in DOCS_DIR.rglob("*.md"):
        try:
            content = md_file.read_text(encoding="utf-8")
            
            # CLI команды
            for match in re.finditer(r'`(\w+)`.*?`python -m bioetl\.cli\.main\s+(\w+)`', content):
                cli_terms.add(match.group(2))
            
            for match in re.finditer(r'python -m bioetl\.cli\.main\s+(\w+)', content):
                cli_terms.add(match.group(1))
            
            # Pipeline names
            for match in re.finditer(r'pipeline.*?[:\s]+(\w+)', content, re.IGNORECASE):
                term = match.group(1).lower()
                if term in ["activity", "assay", "target", "document", "testitem"]:
                    term_variants["pipeline_names"].add(term)
            
            # Config paths
            for match in re.finditer(r'(pipelines?/chembl/(\w+)\.yaml)', content):
                variant = match.group(1)
                term_variants["config_paths"].add(variant)
            
            for match in re.finditer(r'(configs/pipelines/chembl/(\w+)\.yaml)', content):
                variant = match.group(1)
                term_variants["config_paths"].add(variant)
            
        except Exception as e:
            print(f"Ошибка при обработке {md_file}: {e}")
    
    # Определяем канонические формы
    canonical = {
        "cli_commands": sorted(cli_terms),
        "pipeline_names": sorted(term_variants.get("pipeline_names", set())),
        "config_paths": sorted(term_variants.get("config_paths", set())),
    }
    
    return canonical


def generate_reports(gaps: List[GapInfo], broken_links: List[LinkInfo], contradictions: List[Contradiction], canonical_terms: Dict[str, List[str]]):
    """Генерирует все отчеты"""
    print("Генерация отчетов...")
    
    # SUMMARY.txt
    total_gaps = sum(1 for g in gaps if any([g.missing_cli, g.missing_config, g.missing_schema, g.missing_io, g.missing_determinism, g.missing_qc, g.missing_logging]))
    high_priority_gaps = sum(1 for g in gaps if g.priority == "HIGH")
    
    summary = f"""Аудит документации BioETL (ветка refactoring_001)

Общая картина:
- Найдено {len(contradictions)} противоречий в документации, из них {sum(1 for c in contradictions if c.criticality == "HIGH")} критичных
- Обнаружено {total_gaps} пробелов в документации пайплайнов, из них {high_priority_gaps} высокого приоритета
- Найдено {len(broken_links)} битых ссылок в документации
- Проанализировано {len(PIPELINE_PAGES)} страниц пайплайнов
- Проверено обязательных разделов: {len(REQUIRED_SECTIONS)} на страницу

Основные риски:
1. Критичные противоречия в CLI командах и путях к конфигам могут привести к ошибкам при запуске пайплайнов
2. Отсутствие обязательных разделов в документации пайплайнов затрудняет воспроизводимость и поддержку
3. Битые ссылки нарушают навигацию и могут указывать на устаревшую информацию

Рекомендации:
- Приоритизировать исправление противоречий типа D (терминологические) и E (конфиги)
- Заполнить пробелы в документации пайплайнов, начиная с HIGH приоритета
- Исправить все битые ссылки для обеспечения корректной навигации
"""
    
    Path("SUMMARY.txt").write_text(summary, encoding="utf-8")
    print("[OK] SUMMARY.txt created")
    
    # CONTRADICTIONS.md
    contradictions_md = """# Противоречия в документации

| Тип | Раздел/файл | Формулировка №1 | Формулировка №2/факт | Почему конфликт | [ref_doc_1] | [ref_doc_2] | Критичность |
|-----|-------------|-----------------|---------------------|-----------------|-------------|-------------|-------------|
"""
    
    for c in contradictions:
        contradictions_md += f"| {c.type} | {c.section} | {c.formulation1} | {c.formulation2} | {c.reason} | [{c.ref_doc1}]({c.ref_doc1}) | [{c.ref_doc2}]({c.ref_doc2}) | {c.criticality} |\n"
    
    Path("CONTRADICTIONS.md").write_text(contradictions_md, encoding="utf-8")
    print("[OK] CONTRADICTIONS.md created")
    
    # GAPS_TABLE.csv
    with open("GAPS_TABLE.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "pipeline", "doc_path", "missing_cli", "missing_config", "missing_schema",
            "missing_io", "missing_determinism", "missing_qc", "missing_logging", "priority"
        ])
        for gap in gaps:
            writer.writerow([
                gap.pipeline,
                gap.doc_path,
                "YES" if gap.missing_cli else "NO",
                "YES" if gap.missing_config else "NO",
                "YES" if gap.missing_schema else "NO",
                "YES" if gap.missing_io else "NO",
                "YES" if gap.missing_determinism else "NO",
                "YES" if gap.missing_qc else "NO",
                "YES" if gap.missing_logging else "NO",
                gap.priority
            ])
    print("[OK] GAPS_TABLE.csv created")
    
    # LINKCHECK.md
    linkcheck_md = """# Битые ссылки в документации

| Источник | Целевой путь/якорь | Тип | Статус |
|----------|-------------------|-----|--------|
"""
    
    for link in broken_links:
        link_type = "Якорь" if link.is_anchor else "Внешняя" if link.is_external else "Файл"
        linkcheck_md += f"| [{Path(link.source_file).name}]({link.source_file}) | {link.target} | {link_type} | {'Битая' if not link.exists else 'OK'} |\n"
    
    Path("LINKCHECK.md").write_text(linkcheck_md, encoding="utf-8")
    print("[OK] LINKCHECK.md created")
    
    # PATCHES.md
    patches_md = """# Минимальные правки для закрытия критичных пунктов

## Высокий приоритет

"""
    
    for c in contradictions:
        if c.criticality == "HIGH":
            patches_md += f"### {c.type}: {c.section}\n\n"
            patches_md += f"**Файл:** {c.file1}\n\n"
            patches_md += f"**Проблема:** {c.reason}\n\n"
            patches_md += f"**Формулировка 1:** {c.formulation1}\n\n"
            patches_md += f"**Формулировка 2:** {c.formulation2}\n\n"
            
            # Добавляем конкретные исправления для разных типов
            if c.type == "F" and "стадии пайплайна" in c.section:
                patches_md += f"**Исправление:** Унифицировать формулировку стадий пайплайна. Рекомендуется использовать формулировку из {c.file2}: `extract → transform → validate → write` (без `run` в списке стадий, так как `run` - это оркестратор, а не стадия).\n\n"
            elif c.type == "F" and "sort keys" in c.section:
                patches_md += f"**Исправление:** Унифицировать sort keys в {c.file1} с формулировкой из {c.file2}. Использовать кавычки \" и тот же порядок ключей.\n\n"
            else:
                patches_md += f"**Исправление:** Унифицировать формулировку с {c.file2}\n\n"
    
    for gap in gaps:
        if gap.priority == "HIGH":
            missing = []
            if gap.missing_cli: missing.append("CLI")
            if gap.missing_config: missing.append("Конфигурация")
            if gap.missing_schema: missing.append("Схемы валидации")
            if gap.missing_io: missing.append("Входы/выходы")
            if gap.missing_determinism: missing.append("Детерминизм")
            if gap.missing_qc: missing.append("QC/QA")
            if gap.missing_logging: missing.append("Логирование")
            
            patches_md += f"### Пробел: {gap.pipeline}\n\n"
            patches_md += f"**Файл:** {gap.doc_path}\n\n"
            patches_md += f"**Проблема:** Отсутствуют разделы: {', '.join(missing)}\n\n"
            patches_md += f"**Исправление:** Добавить обязательные разделы согласно чек-листу\n\n"
    
    Path("PATCHES.md").write_text(patches_md, encoding="utf-8")
    print("[OK] PATCHES.md created")
    
    # CHECKLIST.md
    checklist_md = """# Чек-лист на закрытие

## Пайплайны

"""
    
    for pipeline_name in PIPELINE_PAGES.keys():
        checklist_md += f"### {pipeline_name}\n\n"
        for section in REQUIRED_SECTIONS:
            checklist_md += f"- [ ] {section}\n"
        checklist_md += "\n"
    
    checklist_md += """## Общие проверки

- [ ] Все противоречия разрешены
- [ ] Все битые ссылки исправлены
- [ ] Все обязательные разделы заполнены
- [ ] Пути к конфигам унифицированы
- [ ] CLI команды согласованы

## Глоссарий терминов

### CLI команды
"""
    
    # Добавляем глоссарий из normalize_terms
    for term in canonical_terms.get("cli_commands", []):
        checklist_md += f"- `{term}`\n"
    
    checklist_md += "\n### Pipeline names\n"
    for term in canonical_terms.get("pipeline_names", []):
        checklist_md += f"- `{term}`\n"
    
    checklist_md += "\n### Config paths (варианты)\n"
    for term in canonical_terms.get("config_paths", []):
        checklist_md += f"- `{term}`\n"
    
    Path("CHECKLIST.md").write_text(checklist_md, encoding="utf-8")
    print("[OK] CHECKLIST.md created")


def main():
    """Главная функция"""
    print("=== Аудит документации BioETL ===\n")
    
    # Этап 1: Построение карты документации
    doc_map = build_documentation_map()
    
    # Этап 2: Инвентаризация пайплайнов
    gaps = inventory_pipelines()
    
    # Этап 3-4: Поиск противоречий (упрощенно)
    contradictions = find_contradictions()
    
    # Этап 5: Проверка ссылок
    broken_links = check_links()
    
    # Этап 6: Нормализация терминов
    canonical_terms = normalize_terms()
    
    # Этап 7: Генерация отчетов
    generate_reports(gaps, broken_links, contradictions, canonical_terms)
    
    print("\n=== Audit completed ===")
    print(f"Reports created: 6")
    print(f"Gaps found: {len(gaps)}")
    print(f"Contradictions found: {len(contradictions)}")
    print(f"Broken links found: {len(broken_links)}")


if __name__ == "__main__":
    main()

