#!/usr/bin/env python3
"""Р”РµС‚Р°Р»СЊРЅС‹Р№ С‚РµСЃС‚ API fallback РІ IUPHAR Р°РґР°РїС‚РµСЂРµ."""

import logging

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ
from library.target.iuphar_adapter import IupharApiCfg, _create_iuphar_session, _search_iuphar_targets

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== Р”РµС‚Р°Р»СЊРЅС‹Р№ С‚РµСЃС‚ API fallback РІ IUPHAR Р°РґР°РїС‚РµСЂРµ ===")

# РЎРѕР·РґР°РµРј С‚РµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ
test_data = {
    'target_chembl_id': 'CHEMBL1075024',
    'mapping_uniprot_id': 'Q9FBC5',
    'pref_name': 'Enoyl-(Acyl-carrier-protein) reductase'
}

print(f"РўРµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ: {test_data}")

# РЎРѕР·РґР°РµРј СЃРµСЃСЃРёСЋ
cfg = IupharApiCfg()
session = _create_iuphar_session(cfg)

# РўРµСЃС‚РёСЂСѓРµРј РїРѕРёСЃРє РїРѕ UniProt ID
print(f"\nРўРµСЃС‚РёСЂСѓРµРј РїРѕРёСЃРє РїРѕ UniProt ID: {test_data['mapping_uniprot_id']}")
uniprot_results = _search_iuphar_targets(test_data['mapping_uniprot_id'], session, cfg)
print(f"Р РµР·СѓР»СЊС‚Р°С‚С‹ РїРѕРёСЃРєР° РїРѕ UniProt ID: {len(uniprot_results)} Р·Р°РїРёСЃРµР№")
for i, result in enumerate(uniprot_results):
    print(f"  {i+1}. targetId={result.get('targetId', 'N/A')}, name={result.get('name', 'N/A')}")

# РўРµСЃС‚РёСЂСѓРµРј РїРѕРёСЃРє РїРѕ target name
print(f"\nРўРµСЃС‚РёСЂСѓРµРј РїРѕРёСЃРє РїРѕ target name: {test_data['pref_name']}")
name_results = _search_iuphar_targets(test_data['pref_name'], session, cfg)
print(f"Р РµР·СѓР»СЊС‚Р°С‚С‹ РїРѕРёСЃРєР° РїРѕ target name: {len(name_results)} Р·Р°РїРёСЃРµР№")
for i, result in enumerate(name_results):
    print(f"  {i+1}. targetId={result.get('targetId', 'N/A')}, name={result.get('name', 'N/A')}")

