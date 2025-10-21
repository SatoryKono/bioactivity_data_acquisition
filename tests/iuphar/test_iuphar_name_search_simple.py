#!/usr/bin/env python3
"""РџСЂРѕСЃС‚РѕР№ С‚РµСЃС‚ РїРѕРёСЃРєР° РїРѕ РЅР°Р·РІР°РЅРёСЋ РјРёС€РµРЅРё РІ IUPHAR API."""

import logging

import requests

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== РџСЂРѕСЃС‚РѕР№ С‚РµСЃС‚ РїРѕРёСЃРєР° РїРѕ РЅР°Р·РІР°РЅРёСЋ РјРёС€РµРЅРё РІ IUPHAR API ===")

# РўРµСЃС‚РѕРІС‹Рµ РЅР°Р·РІР°РЅРёСЏ РјРёС€РµРЅРµР№
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
session = requests.Session()
session.headers.update({
    "Accept": "application/json", 
    "User-Agent": "bioactivity-data-acquisition/0.1.0"
})

base_url = "https://www.guidetopharmacology.org/services"

# РўРµСЃС‚РёСЂСѓРµРј РїРѕРёСЃРє РґР»СЏ РєР°Р¶РґРѕРіРѕ РЅР°Р·РІР°РЅРёСЏ РјРёС€РµРЅРё
for target_name in test_target_names:
    print(f"\n--- РџРѕРёСЃРє РїРѕ РЅР°Р·РІР°РЅРёСЋ: {target_name} ---")
    
    # API РїРѕРёСЃРє
    url = f"{base_url}/targets"
    params = {"search": target_name, "limit": 5}
    
    try:
        response = session.get(url, params=params, timeout=30)
        print(f"URL: {response.url}")
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and data:
                print(f"Р РµР·СѓР»СЊС‚Р°С‚РѕРІ: {len(data)}")
                print("РџРµСЂРІС‹Рµ 5 СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ:")
                for i, result in enumerate(data[:5]):
                    print(f"  {i+1}. targetId={result.get('targetId', 'N/A')}, name={result.get('name', 'N/A')}")
                
                # РџСЂРѕСЃС‚Р°СЏ Р»РѕРіРёРєР° РїРѕРёСЃРєР° Р»СѓС‡С€РµРіРѕ СЃРѕРІРїР°РґРµРЅРёСЏ
                target_name_lower = target_name.lower()
                best_match = None
                best_score = 0
                
                for result in data:
                    result_name = str(result.get("name", "")).lower()
                    score = 0
                    
                    # Exact match
                    if result_name == target_name_lower:
                        score = 100
                    # Partial match
                    elif target_name_lower in result_name:
                        score = 80
                    elif result_name in target_name_lower:
                        score = 70
                    
                    if score > best_score:
                        best_score = score
                        best_match = result
                
                if best_match and best_score >= 50:
                    print(f"Р›СѓС‡С€РµРµ СЃРѕРІРїР°РґРµРЅРёРµ (score={best_score}): targetId={best_match.get('targetId', 'N/A')}, name={best_match.get('name', 'N/A')}")
                else:
                    print("РҐРѕСЂРѕС€РёС… СЃРѕРІРїР°РґРµРЅРёР№ РЅРµ РЅР°Р№РґРµРЅРѕ")
            else:
                print(f"РќРµРѕР¶РёРґР°РЅРЅС‹Р№ С„РѕСЂРјР°С‚ РґР°РЅРЅС‹С…: {data}")
        else:
            print(f"РћС€РёР±РєР° HTTP: {response.status_code}")
            
    except Exception as e:
        print(f"РћС€РёР±РєР°: {e}")

print("\n=== РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ ===")

