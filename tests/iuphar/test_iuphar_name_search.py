#!/usr/bin/env python3
"""РўРµСЃС‚ СѓР»СѓС‡С€РµРЅРЅРѕРіРѕ РїРѕРёСЃРєР° РїРѕ РЅР°Р·РІР°РЅРёСЋ РјРёС€РµРЅРё РІ IUPHAR API."""

import logging

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ
from library.target.iuphar_adapter import IupharApiCfg, _create_iuphar_session, _find_best_target_match, _search_iuphar_targets

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== РўРµСЃС‚ СѓР»СѓС‡С€РµРЅРЅРѕРіРѕ РїРѕРёСЃРєР° РїРѕ РЅР°Р·РІР°РЅРёСЋ РјРёС€РµРЅРё РІ IUPHAR API ===")

# РўРµСЃС‚РѕРІС‹Рµ РЅР°Р·РІР°РЅРёСЏ РјРёС€РµРЅРµР№ РёР· СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ
test_target_names = [
    "Enoyl-(Acyl-carrier-protein) reductase",
    "A1 adenosine receptor", 
    "Dihydropteroate synthase",
    "Dihydrofolate reductase"
]

print(f"РўРµСЃС‚РёСЂСѓРµРј {len(test_target_names)} РЅР°Р·РІР°РЅРёР№ РјРёС€РµРЅРµР№:")
for i, name in enumerate(test_target_names, 1):
    print(f"  {i}. {name}")

# РЎРѕР·РґР°РµРј СЃРµСЃСЃРёСЋ
cfg = IupharApiCfg()
session = _create_iuphar_session(cfg)

# РўРµСЃС‚РёСЂСѓРµРј РїРѕРёСЃРє РґР»СЏ РєР°Р¶РґРѕРіРѕ РЅР°Р·РІР°РЅРёСЏ РјРёС€РµРЅРё
for target_name in test_target_names:
    print(f"\n--- РџРѕРёСЃРє РїРѕ РЅР°Р·РІР°РЅРёСЋ: {target_name} ---")
    
    # РўРµСЃС‚РёСЂСѓРµРј API РїРѕРёСЃРє
    results = _search_iuphar_targets(target_name, session, cfg)
    print(f"API СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ: {len(results)}")
    
    if results:
        # РџРѕРєР°Р·С‹РІР°РµРј РїРµСЂРІС‹Рµ 5 СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ
        print("РџРµСЂРІС‹Рµ 5 СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ:")
        for i, result in enumerate(results[:5]):
            print(f"  {i+1}. targetId={result.get('targetId', 'N/A')}, name={result.get('name', 'N/A')}")
        
        # РўРµСЃС‚РёСЂСѓРµРј СѓР»СѓС‡С€РµРЅРЅС‹Р№ РїРѕРёСЃРє
        best_match = _find_best_target_match(target_name, results)
        if best_match:
            print(f"Р›СѓС‡С€РµРµ СЃРѕРІРїР°РґРµРЅРёРµ: targetId={best_match.get('targetId', 'N/A')}, name={best_match.get('name', 'N/A')}")
        else:
            print("РҐРѕСЂРѕС€РёС… СЃРѕРІРїР°РґРµРЅРёР№ РЅРµ РЅР°Р№РґРµРЅРѕ")
    else:
        print("Р РµР·СѓР»СЊС‚Р°С‚РѕРІ РЅРµС‚")

print("\n=== РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ ===")
print("Р•СЃР»Рё РёСЃРїСЂР°РІР»РµРЅРёРµ СЂР°Р±РѕС‚Р°РµС‚ РїСЂР°РІРёР»СЊРЅРѕ, РєР°Р¶РґС‹Р№ target name РґРѕР»Р¶РµРЅ РЅР°С…РѕРґРёС‚СЊ СѓРЅРёРєР°Р»СЊРЅС‹Рµ СЂРµР·СѓР»СЊС‚Р°С‚С‹.")

