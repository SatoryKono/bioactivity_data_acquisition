#!/usr/bin/env python3
"""РўРµСЃС‚ API fallback РІ IUPHAR Р°РґР°РїС‚РµСЂРµ."""

import pytest
import logging
import sys
from pathlib import Path
import pandas as pd

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ


from library.target.iuphar_adapter import (
    IupharApiCfg, 
    _build_iuphar_lookup_index, 
    _map_via_api,
    _create_iuphar_session
)

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== РўРµСЃС‚ API fallback РІ IUPHAR Р°РґР°РїС‚РµСЂРµ ===")

# РЎРѕР·РґР°РµРј С‚РµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ
test_data = {
    'target_chembl_id': 'CHEMBL1075024',
    'mapping_uniprot_id': 'Q9FBC5',
    'pref_name': 'Enoyl-(Acyl-carrier-protein) reductase'
}

print(f"РўРµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ: {test_data}")

# РЎС‚СЂРѕРёРј РёРЅРґРµРєСЃ
cfg = IupharApiCfg()
lookup_index = _build_iuphar_lookup_index(cfg)

# РЎРѕР·РґР°РµРј СЃРµСЃСЃРёСЋ
session = _create_iuphar_session(cfg)

# РўРµСЃС‚РёСЂСѓРµРј API lookup
print(f"\nРўРµСЃС‚РёСЂСѓРµРј _map_via_api:")
result = _map_via_api(test_data, session, cfg)
if result:
    print(f"[SUCCESS] API СЂРµР·СѓР»СЊС‚Р°С‚: {result}")
else:
    print(f"[FAILED] API СЂРµР·СѓР»СЊС‚Р°С‚: None")

