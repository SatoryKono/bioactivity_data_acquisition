#!/usr/bin/env python3
"""Проверка заполнения конкретных полей."""

import sys
import pandas as pd
from pathlib import Path

def check_field_fill(csv_path: str):
    """Проверяет заполнение конкретных полей."""
    
    print(f"Проверка заполнения полей: {csv_path}")
    print("=" * 60)
    
    # Загружаем данные
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Ошибка загрузки файла: {e}")
        return
    
    print(f"Всего строк: {len(df)}")
    print()
    
    # Проверяем конкретные поля
    fields_to_check = {
        'Crossref': ['crossref_title', 'crossref_doc_type', 'crossref_subject', 'doi_key'],
        'OpenAlex': ['openalex_title', 'openalex_doc_type', 'openalex_publication_year', 'openalex_doi_key'],
        'PubMed': ['pubmed_pmid', 'pubmed_doi', 'pubmed_article_title', 'pubmed_abstract', 'pubmed_journal_title'],
        'Semantic Scholar': ['semantic_scholar_pmid', 'semantic_scholar_doi', 'semantic_scholar_venue', 'semantic_scholar_publication_types']
    }
    
    for source, fields in fields_to_check.items():
        print(f"{source.upper()}:")
        print("-" * 30)
        
        for field in fields:
            if field in df.columns:
                filled = df[field].notna().sum()
                total = len(df)
                percentage = (filled / total) * 100
                print(f"  {field:<30} {filled:2d}/{total:2d} ({percentage:5.1f}%)")
            else:
                print(f"  {field:<30} НЕ НАЙДЕНО")
        print()

def main():
    if len(sys.argv) != 2:
        print("Использование: python check_field_fill.py <путь_к_csv_файлу>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    if not Path(csv_path).exists():
        print(f"Файл не найден: {csv_path}")
        sys.exit(1)
    
    check_field_fill(csv_path)

if __name__ == "__main__":
    main()
