#!/usr/bin/env python3
"""РўРµСЃС‚РѕРІС‹Р№ СЃРєСЂРёРїС‚ РґР»СЏ Р·Р°РїСѓСЃРєР° IUPHAR РјР°РїРїРёРЅРіР° СЃ debug Р»РѕРіРёСЂРѕРІР°РЅРёРµРј."""

import pytest
import logging
import sys
from pathlib import Path

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ


from library.target import load_target_config, run_target_etl
from library.logging_setup import configure_logging

# РќР°СЃС‚СЂР°РёРІР°РµРј debug Р»РѕРіРёСЂРѕРІР°РЅРёРµ
configure_logging(
    level="DEBUG",
    file_enabled=False,
    console_format="text"
)

# Р—Р°РіСЂСѓР¶Р°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ
config = load_target_config("configs/config_target_full.yaml")

# РЎРѕР·РґР°РµРј С‚РµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ (РїРµСЂРІС‹Рµ 3 target РёР· СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ)
test_targets = [
    "CHEMBL1075024",  # Q9FBC5 -> РґРѕР»Р¶РµРЅ РїРѕР»СѓС‡РёС‚СЊ 1386
    "CHEMBL1075029",  # Q4VQ11 -> РґРѕР»Р¶РµРЅ РїРѕР»СѓС‡РёС‚СЊ 1386  
    "CHEMBL1075045",  # Q81VW8 -> РґРѕР»Р¶РµРЅ РїРѕР»СѓС‡РёС‚СЊ 1386
]

print("=== Р—Р°РїСѓСЃРє IUPHAR РјР°РїРїРёРЅРіР° СЃ debug Р»РѕРіРёСЂРѕРІР°РЅРёРµРј ===")
print(f"РўРµСЃС‚РёСЂСѓРµРј {len(test_targets)} targets: {test_targets}")

try:
    result = run_target_etl(config, target_ids=test_targets)
    
    print(f"\n=== Р РµР·СѓР»СЊС‚Р°С‚С‹ ===")
    print(f"Р’СЃРµРіРѕ targets: {len(result.targets)}")
    
    for _, row in result.targets.iterrows():
        print(f"  {row['target_chembl_id']}: iuphar_target_id={row.get('iuphar_target_id', 'N/A')}, "
              f"mapping_uniprot_id={row.get('mapping_uniprot_id', 'N/A')}")
              
except Exception as e:
    print(f"РћС€РёР±РєР°: {e}")
    import traceback
    traceback.print_exc()

