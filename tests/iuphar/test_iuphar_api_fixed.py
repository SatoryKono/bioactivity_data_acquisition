#!/usr/bin/env python3
"""РўРµСЃС‚ РёСЃРїСЂР°РІР»РµРЅРЅРѕРіРѕ API РїРѕРёСЃРєР° РІ IUPHAR Р°РґР°РїС‚РµСЂРµ."""

import pytest
import logging
import sys
from pathlib import Path
import pandas as pd

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ


from library.target.iuphar_adapter import (
    IupharApiCfg, 
    _search_iuphar_targets,
    _create_iuphar_session
)

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== РўРµСЃС‚ РёСЃРїСЂР°РІР»РµРЅРЅРѕРіРѕ API РїРѕРёСЃРєР° РІ IUPHAR Р°РґР°РїС‚РµСЂРµ ===")

# РўРµСЃС‚РѕРІС‹Рµ UniProt ID РёР· СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ
test_uniprot_ids = ['Q9FBC5', 'Q4VQ11', 'Q81VW8', 'P00376']

print(f"РўРµСЃС‚РёСЂСѓРµРј {len(test_uniprot_ids)} UniProt ID: {test_uniprot_ids}")

# РЎРѕР·РґР°РµРј СЃРµСЃСЃРёСЋ
cfg = IupharApiCfg()
session = _create_iuphar_session(cfg)

# РўРµСЃС‚РёСЂСѓРµРј РїРѕРёСЃРє РґР»СЏ РєР°Р¶РґРѕРіРѕ UniProt ID
for uniprot_id in test_uniprot_ids:
    print(f"\n--- РџРѕРёСЃРє РїРѕ UniProt ID: {uniprot_id} ---")
    results = _search_iuphar_targets(uniprot_id, session, cfg)
    print(f"Р РµР·СѓР»СЊС‚Р°С‚РѕРІ: {len(results)}")
    
    if results:
        # Р‘РµСЂРµРј РїРµСЂРІС‹Р№ СЂРµР·СѓР»СЊС‚Р°С‚ (РєР°Рє РІ РєРѕРґРµ)
        first_result = results[0]
        print(f"РџРµСЂРІС‹Р№ СЂРµР·СѓР»СЊС‚Р°С‚: targetId={first_result.get('targetId', 'N/A')}, name={first_result.get('name', 'N/A')}")
        
        # РџРѕРєР°Р·С‹РІР°РµРј РІСЃРµ СЂРµР·СѓР»СЊС‚Р°С‚С‹
        for i, result in enumerate(results):
            print(f"  {i+1}. targetId={result.get('targetId', 'N/A')}, name={result.get('name', 'N/A')}")
    else:
        print("Р РµР·СѓР»СЊС‚Р°С‚РѕРІ РЅРµС‚")

print(f"\n=== РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ ===")
print("Р•СЃР»Рё РёСЃРїСЂР°РІР»РµРЅРёРµ СЂР°Р±РѕС‚Р°РµС‚ РїСЂР°РІРёР»СЊРЅРѕ, РєР°Р¶РґС‹Р№ UniProt ID РґРѕР»Р¶РµРЅ РІРѕР·РІСЂР°С‰Р°С‚СЊ СѓРЅРёРєР°Р»СЊРЅС‹Рµ СЂРµР·СѓР»СЊС‚Р°С‚С‹.")

