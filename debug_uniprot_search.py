#!/usr/bin/env python3
"""
Скрипт для поиска UniProt IDs в словаре IUPHAR
"""

import sys
sys.path.insert(0, 'src')

import pandas as pd

def search_uniprot_ids():
    """Поиск UniProt IDs в словаре IUPHAR"""
    
    print("=== ПОИСК UNIPROT IDS В СЛОВАРЕ IUPHAR ===")
    print()
    
    # Загружаем словарь
    target_df = pd.read_csv('configs/dictionary/_target/_IUPHAR/_IUPHAR_target.csv')
    
    # UniProt IDs из нашего результата
    test_uniprot_ids = ['Q9FBC5', 'Q4VQ11', 'P12345']
    
    print("1. ПРЯМОЙ ПОИСК UNIPROT IDS:")
    for uniprot_id in test_uniprot_ids:
        matches = target_df[target_df['swissprot'] == uniprot_id]
        print(f"UniProt {uniprot_id}: {len(matches)} записей")
        if len(matches) > 0:
            row = matches.iloc[0]
            print(f"  Target ID: {row['target_id']}")
            print(f"  Target Name: {row['target_name']}")
            print(f"  Family ID: {row['family_id']}")
        print()
    
    print("2. ПОИСК ПО ЧАСТИЧНОМУ СОВПАДЕНИЮ:")
    for uniprot_id in test_uniprot_ids:
        # Ищем по частичному совпадению
        partial_matches = target_df[target_df['swissprot'].str.contains(uniprot_id, na=False)]
        print(f"UniProt {uniprot_id} (частично): {len(partial_matches)} записей")
        if len(partial_matches) > 0:
            for _, row in partial_matches.head(3).iterrows():
                print(f"  {row['swissprot']} -> {row['target_id']} ({row['target_name']})")
        print()
    
    print("3. ПОИСК ПО СИНОНИМАМ:")
    for uniprot_id in test_uniprot_ids:
        # Ищем в колонке synonyms
        synonym_matches = target_df[target_df['synonyms'].str.contains(uniprot_id, na=False)]
        print(f"UniProt {uniprot_id} (в synonyms): {len(synonym_matches)} записей")
        if len(synonym_matches) > 0:
            for _, row in synonym_matches.head(3).iterrows():
                print(f"  Target ID: {row['target_id']}")
                print(f"  Target Name: {row['target_name']}")
                print(f"  Synonyms: {row['synonyms'][:100]}...")
        print()
    
    print("4. СТАТИСТИКА UNIPROT IDS В СЛОВАРЕ:")
    uniprot_count = target_df['swissprot'].notna().sum()
    print(f"Всего записей с UniProt ID: {uniprot_count}")
    
    # Проверим уникальные UniProt IDs
    unique_uniprot = target_df['swissprot'].dropna().nunique()
    print(f"Уникальных UniProt IDs: {unique_uniprot}")
    
    # Проверим первые несколько UniProt IDs
    print("Первые 10 UniProt IDs в словаре:")
    first_uniprot = target_df['swissprot'].dropna().head(10).tolist()
    for i, uniprot_id in enumerate(first_uniprot):
        print(f"  {i+1}: {uniprot_id}")
    
    print()
    print("5. ПРОВЕРКА НАЛИЧИЯ НАШИХ UNIPROT IDS В СЛОВАРЕ:")
    all_uniprot_ids = set(target_df['swissprot'].dropna().tolist())
    for uniprot_id in test_uniprot_ids:
        if uniprot_id in all_uniprot_ids:
            print(f"✓ {uniprot_id} найден в словаре")
        else:
            print(f"✗ {uniprot_id} НЕ найден в словаре")

if __name__ == "__main__":
    search_uniprot_ids()
