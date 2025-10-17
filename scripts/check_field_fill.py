#!/usr/bin/env python3
"""Проверка заполнения конкретных полей."""

import sys
from pathlib import Path

import pandas as pd


# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from library.logging_setup import get_logger

logger = get_logger(__name__)


def check_field_fill(csv_path: str):
    """Проверяет заполнение конкретных полей."""
    
    logger.info(f"Проверка заполнения полей: {csv_path}")
    logger.info("=" * 60)
    
    # Загружаем данные
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"Ошибка загрузки файла: {e}")
        return
    
    logger.info(f"Всего строк: {len(df)}")
    print()
    
    # Проверяем конкретные поля
    fields_to_check = {
        'Crossref': ['crossref_title', 'crossref_doc_type', 'crossref_subject', 'doi_key'],
        'OpenAlex': ['openalex_title', 'openalex_doc_type', 'openalex_year', 'openalex_doi'],
        'PubMed': ['pubmed_pmid', 'pubmed_doi', 'pubmed_article_title', 'pubmed_abstract', 'pubmed_journal'],
        'Semantic Scholar': ['semantic_scholar_pmid', 'semantic_scholar_doi', 'semantic_scholar_journal', 'semantic_scholar_doc_type']
    }
    
    for source, fields in fields_to_check.items():
        logger.info(f"{source.upper()}:")
        logger.info("-" * 30)
        
        for field in fields:
            if field in df.columns:
                filled = df[field].notna().sum()
                total = len(df)
                percentage = (filled / total) * 100
                logger.info(f"  {field:<30} {filled:2d}/{total:2d} ({percentage:5.1f}%)")
            else:
                logger.info(f"  {field:<30} НЕ НАЙДЕНО")
        print()

def main():
    if len(sys.argv) != 2:
        logger.info("Использование: python check_field_fill.py <путь_к_csv_файлу>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    if not Path(csv_path).exists():
        logger.info(f"Файл не найден: {csv_path}")
        sys.exit(1)
    
    check_field_fill(csv_path)

if __name__ == "__main__":
    main()
