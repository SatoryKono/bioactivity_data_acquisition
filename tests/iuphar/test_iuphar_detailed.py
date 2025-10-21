#!/usr/bin/env python3
"""Р”РµС‚Р°Р»СЊРЅС‹Р№ С‚РµСЃС‚ IUPHAR Р°РґР°РїС‚РµСЂР° СЃ РїРѕС€Р°РіРѕРІРѕР№ РґРёР°РіРЅРѕСЃС‚РёРєРѕР№."""

import logging

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ
from library.target.iuphar_adapter import IupharApiCfg, _build_iuphar_lookup_index, _map_via_csv_first, _map_via_simple_fallback

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== Р”РµС‚Р°Р»СЊРЅС‹Р№ С‚РµСЃС‚ IUPHAR Р°РґР°РїС‚РµСЂР° ===")

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

print("\nРРЅРґРµРєСЃ РїРѕСЃС‚СЂРѕРµРЅ:")
print(f"  by_target_id: {len(lookup_index['by_target_id'])} Р·Р°РїРёСЃРµР№")
print(f"  by_uniprot: {len(lookup_index['by_uniprot'])} Р·Р°РїРёСЃРµР№")
print(f"  by_gene: {len(lookup_index['by_gene'])} Р·Р°РїРёСЃРµР№")
print(f"  by_name: {len(lookup_index['by_name'])} Р·Р°РїРёСЃРµР№")

# РџСЂРѕРІРµСЂСЏРµРј, РµСЃС‚СЊ Р»Рё РЅР°С€ UniProt ID РІ РёРЅРґРµРєСЃРµ
uniprot_id = test_data['mapping_uniprot_id']
print(f"\nРџСЂРѕРІРµСЂСЏРµРј UniProt ID: {uniprot_id}")
if uniprot_id in lookup_index['by_uniprot']:
    print(f"[FOUND] РќР°Р№РґРµРЅ РІ by_uniprot: {lookup_index['by_uniprot'][uniprot_id]}")
else:
    print("[NOT FOUND] РќР• РЅР°Р№РґРµРЅ РІ by_uniprot")

# РўРµСЃС‚РёСЂСѓРµРј CSV lookup
print("\nРўРµСЃС‚РёСЂСѓРµРј _map_via_csv_first:")
result = _map_via_csv_first(test_data, lookup_index, cfg)
if result:
    print(f"[SUCCESS] Р РµР·СѓР»СЊС‚Р°С‚: {result}")
else:
    print("[FAILED] Р РµР·СѓР»СЊС‚Р°С‚: None")

# РўРµСЃС‚РёСЂСѓРµРј simple fallback
print("\nРўРµСЃС‚РёСЂСѓРµРј _map_via_simple_fallback:")
fallback_result = _map_via_simple_fallback(test_data)
if fallback_result:
    print(f"[SUCCESS] Fallback СЂРµР·СѓР»СЊС‚Р°С‚: {fallback_result}")
else:
    print("[FAILED] Fallback СЂРµР·СѓР»СЊС‚Р°С‚: None")

