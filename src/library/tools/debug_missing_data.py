#!/usr/bin/env python3
"""Отладка недостающих данных в Crossref и OpenAlex."""

import sys
import pandas as pd
from pathlib import Path

def debug_missing_data(csv_path: str):
    """Анализирует недостающие данные."""
    
    print(f"Отладка недостающих данных: {csv_path}")
    print("=" * 60)
    
    # Загружаем данные
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Ошибка загрузки файла: {e}")
        return
    
    print(f"Всего строк: {len(df)}")
    print()
    
    # Анализируем Crossref
    print("АНАЛИЗ CROSSREF:")
    print("-" * 40)
    
    crossref_filled = df['crossref_title'].notna()
    crossref_empty = df['crossref_title'].isna()
    
    print(f"Crossref заполнен: {crossref_filled.sum()}/{len(df)} записей")
    print(f"Crossref пуст: {crossref_empty.sum()}/{len(df)} записей")
    
    if crossref_empty.sum() > 0:
        print("\nЗаписи с пустым Crossref:")
        empty_crossref = df[crossref_empty][['document_chembl_id', 'doi', 'document_pubmed_id', 'crossref_error']]
        for _, row in empty_crossref.head(5).iterrows():
            print(f"  {row['document_chembl_id']}: DOI={row['doi']}, PMID={row['document_pubmed_id']}, Error={row['crossref_error']}")
    
    # Анализируем OpenAlex
    print("\nАНАЛИЗ OPENALEX:")
    print("-" * 40)
    
    openalex_filled = df['openalex_title'].notna()
    openalex_empty = df['openalex_title'].isna()
    
    print(f"OpenAlex заполнен: {openalex_filled.sum()}/{len(df)} записей")
    print(f"OpenAlex пуст: {openalex_empty.sum()}/{len(df)} записей")
    
    if openalex_empty.sum() > 0:
        print("\nЗаписи с пустым OpenAlex:")
        empty_openalex = df[openalex_empty][['document_chembl_id', 'doi', 'document_pubmed_id', 'openalex_error']]
        for _, row in empty_openalex.head(5).iterrows():
            print(f"  {row['document_chembl_id']}: DOI={row['doi']}, PMID={row['document_pubmed_id']}, Error={row['openalex_error']}")
    
    # Анализируем ошибки
    print("\nАНАЛИЗ ОШИБОК:")
    print("-" * 40)
    
    error_columns = ['crossref_error', 'openalex_error', 'pubmed_error', 'semantic_scholar_error']
    
    for error_col in error_columns:
        if error_col in df.columns:
            error_count = df[error_col].notna().sum()
            if error_count > 0:
                print(f"{error_col}: {error_count} ошибок")
                # Показываем примеры ошибок
                errors = df[df[error_col].notna()][error_col].unique()[:3]
                for error in errors:
                    print(f"  - {error}")
    
    # Проверяем DOI
    print("\nАНАЛИЗ DOI:")
    print("-" * 40)
    
    doi_count = df['doi'].notna().sum()
    print(f"Записей с DOI: {doi_count}/{len(df)}")
    
    if doi_count < len(df):
        print("Записи без DOI:")
        no_doi = df[df['doi'].isna()][['document_chembl_id', 'document_pubmed_id']]
        for _, row in no_doi.head(5).iterrows():
            print(f"  {row['document_chembl_id']}: PMID={row['document_pubmed_id']}")

def main():
    if len(sys.argv) != 2:
        print("Использование: python debug_missing_data.py <путь_к_csv_файлу>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    if not Path(csv_path).exists():
        print(f"Файл не найден: {csv_path}")
        sys.exit(1)
    
    debug_missing_data(csv_path)

if __name__ == "__main__":
    main()
