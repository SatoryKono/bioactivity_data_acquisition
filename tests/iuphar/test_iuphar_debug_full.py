#!/usr/bin/env python3
"""РџРѕР»РЅС‹Р№ С‚РµСЃС‚ СЃ debug Р»РѕРіРёСЂРѕРІР°РЅРёРµРј РґР»СЏ РїРѕРЅРёРјР°РЅРёСЏ РїСЂРѕР±Р»РµРјС‹."""

import logging

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ
from library.target.iuphar_adapter import IupharApiCfg, _build_iuphar_lookup_index, _map_via_csv_first

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ РЅР° DEBUG СѓСЂРѕРІРµРЅСЊ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== РџРѕР»РЅС‹Р№ С‚РµСЃС‚ СЃ debug Р»РѕРіРёСЂРѕРІР°РЅРёРµРј ===")

# РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ
cfg = IupharApiCfg()

# РЎС‚СЂРѕРёРј lookup РёРЅРґРµРєСЃ
print("РЎС‚СЂРѕРёРј lookup РёРЅРґРµРєСЃ...")
lookup_index = _build_iuphar_lookup_index(cfg)

# РўРµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ
target_data = {
    "target_chembl_id": "CHEMBL1234",
    "pref_name": "Enoyl-(Acyl-carrier-protein) reductase",
    "target_type": "SINGLE PROTEIN",
    "mapping_uniprot_id": "Q9FBC5",
    "gene_symbol": "fabI"
}

print(f"\n--- РўРµСЃС‚РёСЂСѓРµРј CSV lookup РґР»СЏ {target_data['target_chembl_id']} ---")

# Р—Р°РїСѓСЃРєР°РµРј CSV lookup СЃ debug Р»РѕРіРёСЂРѕРІР°РЅРёРµРј
result = _map_via_csv_first(target_data, lookup_index, cfg)

if result:
    print(f"Р РµР·СѓР»СЊС‚Р°С‚ CSV lookup: iuphar_target_id={result.get('iuphar_target_id', 'N/A')}, iuphar_name={result.get('iuphar_name', 'N/A')}")
else:
    print("CSV lookup РЅРµ РЅР°С€РµР» СЃРѕРІРїР°РґРµРЅРёР№")

print("\n=== РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ ===")

