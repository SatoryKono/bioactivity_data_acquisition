#!/usr/bin/env python3
"""Простой тест IUPHAR адаптера."""

import pytest
import logging
import pandas as pd

from library.target.iuphar_adapter import IupharApiCfg, enrich_targets_with_iuphar

# Настраиваем логирование
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

# Создаем тестовые данные
test_data = pd.DataFrame({
    'target_chembl_id': ['CHEMBL1075024', 'CHEMBL1075029'],
    'mapping_uniprot_id': ['Q9FBC5', 'Q4VQ11'],
    'pref_name': ['Enoyl-(Acyl-carrier-protein) reductase', 'A1 adenosine receptor']
})

print("=== РўРµСЃС‚ IUPHAR Р°РґР°РїС‚РµСЂР° ===")
print(f"Р’С…РѕРґРЅС‹Рµ РґР°РЅРЅС‹Рµ: {len(test_data)} Р·Р°РїРёСЃРµР№")
print(test_data[['target_chembl_id', 'mapping_uniprot_id', 'pref_name']])

# Р—Р°РїСѓСЃРєР°РµРј РѕР±РѕРіР°С‰РµРЅРёРµ
cfg = IupharApiCfg()
result = enrich_targets_with_iuphar(test_data, cfg)

print(f"\nР РµР·СѓР»СЊС‚Р°С‚: {len(result)} Р·Р°РїРёСЃРµР№")
print(result[['target_chembl_id', 'mapping_uniprot_id', 'iuphar_target_id', 'iuphar_name']].to_string())

