#!/usr/bin/env python3
"""Debug скрипт для анализа IUPHAR маппинга."""

import pandas as pd

# Читаем результаты target
target_df = pd.read_csv("data/output/target/target_20251021.csv")

print("=== Анализ IUPHAR маппинга ===\n")

# Статистика по iuphar_target_id
print(f"Всего target записей: {len(target_df)}")
print(f"\nУникальные iuphar_target_id:")
print(target_df['iuphar_target_id'].value_counts())

# Проверяем mapping_uniprot_id
print(f"\n\nПервые 5 mapping_uniprot_id:")
print(target_df[['target_chembl_id', 'mapping_uniprot_id', 'iuphar_target_id']].head())

# Проверяем наличие mapping_uniprot_id в словаре IUPHAR
iuphar_target_df = pd.read_csv("configs/dictionary/_target/_IUPHAR/_IUPHAR_target.csv")
print(f"\n\nВсего записей в IUPHAR словаре: {len(iuphar_target_df)}")
print(f"Уникальные swissprot (UniProt IDs) в IUPHAR: {iuphar_target_df['swissprot'].nunique()}")

# Проверяем, есть ли наши UniProt IDs в словаре
sample_uniprot_ids = target_df['mapping_uniprot_id'].dropna().head(10).tolist()
print(f"\n\nПроверка первых 10 UniProt IDs из результатов в словаре IUPHAR:")
for uid in sample_uniprot_ids:
    matches = iuphar_target_df[iuphar_target_df['swissprot'] == uid]
    if len(matches) > 0:
        print(f"[FOUND] {uid} найден: target_id={matches.iloc[0]['target_id']}, target_name={matches.iloc[0]['target_name']}")
    else:
        print(f"[NOT FOUND] {uid} НЕ найден в словаре IUPHAR")

# Проверяем 1386
print(f"\n\n=== Анализ target_id 1386 ===")
matches_1386 = iuphar_target_df[iuphar_target_df['target_id'] == 1386]
print(f"Записей с target_id=1386: {len(matches_1386)}")
print(f"UniProt IDs для target_id=1386:")
print(matches_1386['swissprot'].unique())

