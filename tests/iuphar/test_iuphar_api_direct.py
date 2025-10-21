#!/usr/bin/env python3
"""РџСЂСЏРјРѕР№ С‚РµСЃС‚ С„СѓРЅРєС†РёРё _map_via_api."""

import pytest
import logging
import sys
from pathlib import Path

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ


from library.target.iuphar_adapter import _map_via_api, _create_iuphar_session, IupharApiCfg

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ РЅР° DEBUG СѓСЂРѕРІРµРЅСЊ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== РџСЂСЏРјРѕР№ С‚РµСЃС‚ С„СѓРЅРєС†РёРё _map_via_api ===")

# РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ
cfg = IupharApiCfg()
session = _create_iuphar_session(cfg)

# РўРµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ
target_data = {
    "target_chembl_id": "CHEMBL1234",
    "pref_name": "Enoyl-(Acyl-carrier-protein) reductase",
    "target_type": "SINGLE PROTEIN",
    "mapping_uniprot_id": "Q9FBC5",
    "gene_symbol": "fabI"
}

print(f"РўРµСЃС‚РёСЂСѓРµРј _map_via_api РґР»СЏ {target_data['target_chembl_id']}")

# РџСЂСЏРјРѕР№ РІС‹Р·РѕРІ С„СѓРЅРєС†РёРё
result = _map_via_api(target_data, session, cfg)

if result:
    print(f"Р РµР·СѓР»СЊС‚Р°С‚: iuphar_target_id={result.get('iuphar_target_id', 'N/A')}")
else:
    print("Р РµР·СѓР»СЊС‚Р°С‚: None")

print(f"\n=== РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ ===")

