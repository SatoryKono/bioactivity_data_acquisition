#!/usr/bin/env python3
"""
Скрипт для отладки IUPHAR маппинга
"""

import sys
sys.path.insert(0, 'src')

import pandas as pd
from library.target.iuphar_adapter import IupharApiCfg, _build_iuphar_lookup_index, _map_via_csv_first

def debug_iuphar_mapping():
    """Отладка IUPHAR маппинга для выявления проблем"""
    
    print("=== ОТЛАДКА IUPHAR МАППИНГА ===")
    print()
    
    # Загружаем словари напрямую
    target_df = pd.read_csv('configs/dictionary/_target/_IUPHAR/_IUPHAR_target.csv')
    family_df = pd.read_csv('configs/dictionary/_target/_IUPHAR/_IUPHAR_family.csv')
    
    print("1. СТАТИСТИКА СЛОВАРЕЙ:")
    print(f"Target записей: {len(target_df)}")
    print(f"Family записей: {len(family_df)}")
    print()
    
    # UniProt IDs из нашего результата
    test_uniprot_ids = ['Q9FBC5', 'Q4VQ11', 'P12345']
    
    print("2. ПРОВЕРКА UNIPROT IDS В СЛОВАРЕ:")
    for uniprot_id in test_uniprot_ids:
        matches = target_df[target_df['swissprot'] == uniprot_id]
        print(f"UniProt {uniprot_id}: {len(matches)} записей")
        if len(matches) > 0:
            print(f"  Target ID: {matches.iloc[0]['target_id']}")
            print(f"  Target Name: {matches.iloc[0]['target_name']}")
            print(f"  Family ID: {matches.iloc[0]['family_id']}")
        print()
    
    print("3. ПРОВЕРКА TARGET_ID=1386:")
    target_1386 = target_df[target_df['target_id'] == 1386]
    print(f"Записей с target_id=1386: {len(target_1386)}")
    if len(target_1386) > 0:
        print("Первая запись:")
        row = target_1386.iloc[0]
        print(f"  Target Name: {row['target_name']}")
        print(f"  Family ID: {row['family_id']}")
        print(f"  UniProt IDs: {row['swissprot']}")
        print(f"  Gene Name: {row['gene_name']}")
    print()
    
    print("4. ПРОВЕРКА FAMILY_ID=271 vs 0271:")
    family_271 = family_df[family_df['family_id'] == '271']
    family_0271 = family_df[family_df['family_id'] == '0271']
    print(f"Family ID=271: {len(family_271)} записей")
    print(f"Family ID=0271: {len(family_0271)} записей")
    if len(family_0271) > 0:
        print("Family 0271:")
        row = family_0271.iloc[0]
        print(f"  Family Name: {row['family_name']}")
        print(f"  Parent Family ID: {row['parent_family_id']}")
    print()
    
    print("5. ПРОВЕРКА LOOKUP ИНДЕКСА:")
    cfg = IupharApiCfg()
    lookup_index = _build_iuphar_lookup_index(cfg)
    
    print("Статистика индекса:")
    for key, index in lookup_index.items():
        print(f"  {key}: {len(index)} записей")
    print()
    
    print("6. ТЕСТ МАППИНГА:")
    test_data = {
        'target_chembl_id': 'CHEMBL1075024',
        'uniprot_id': 'Q9FBC5',
        'mapping_uniprot_id': 'Q9FBC5',
        'pref_name': 'Enoyl-(Acyl-carrier-protein) reductase'
    }
    
    result = _map_via_csv_first(test_data, lookup_index, cfg)
    if result:
        print("Маппинг найден:")
        for key, value in result.items():
            if key.startswith('iuphar_'):
                print(f"  {key}: {value}")
    else:
        print("Маппинг НЕ найден")
    print()
    
    print("7. ПРОВЕРКА ПЕРВЫХ 10 UNIPROT IDS В СЛОВАРЕ:")
    uniprot_ids = target_df['swissprot'].dropna().head(10).tolist()
    for i, uniprot_id in enumerate(uniprot_ids):
        print(f"{i+1}: {uniprot_id}")

if __name__ == "__main__":
    debug_iuphar_mapping()
