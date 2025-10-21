#!/usr/bin/env python3
"""Р”РµС‚Р°Р»СЊРЅС‹Р№ С‚РµСЃС‚ API Р·Р°РїСЂРѕСЃРѕРІ Рє IUPHAR."""

import pytest
import logging
import requests

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== Р”РµС‚Р°Р»СЊРЅС‹Р№ С‚РµСЃС‚ API Р·Р°РїСЂРѕСЃРѕРІ Рє IUPHAR ===")

# РўРµСЃС‚РѕРІС‹Рµ UniProt ID
test_uniprot_ids = ['Q9FBC5', 'Q4VQ11']

# РЎРѕР·РґР°РµРј СЃРµСЃСЃРёСЋ
session = requests.Session()
session.headers.update({
    "Accept": "application/json", 
    "User-Agent": "bioactivity-data-acquisition/0.1.0"
})

base_url = "https://www.guidetopharmacology.org/services"

for uniprot_id in test_uniprot_ids:
    print(f"\n--- РўРµСЃС‚РёСЂСѓРµРј UniProt ID: {uniprot_id} ---")
    
    # РўРµСЃС‚ 1: РЎС‚Р°СЂС‹Р№ СЃРїРѕСЃРѕР± (search РїР°СЂР°РјРµС‚СЂ)
    print(f"1. РЎС‚Р°СЂС‹Р№ СЃРїРѕСЃРѕР± (search):")
    url1 = f"{base_url}/targets"
    params1 = {"search": uniprot_id, "limit": 5}
    try:
        response1 = session.get(url1, params=params1, timeout=30)
        print(f"   URL: {response1.url}")
        print(f"   Status: {response1.status_code}")
        data1 = response1.json()
        if isinstance(data1, list) and data1:
            print(f"   Р РµР·СѓР»СЊС‚Р°С‚РѕРІ: {len(data1)}")
            print(f"   РџРµСЂРІС‹Р№: targetId={data1[0].get('targetId', 'N/A')}, name={data1[0].get('name', 'N/A')}")
        else:
            print(f"   Р РµР·СѓР»СЊС‚Р°С‚: {data1}")
    except Exception as e:
        print(f"   РћС€РёР±РєР°: {e}")
    
    # РўРµСЃС‚ 2: РќРѕРІС‹Р№ СЃРїРѕСЃРѕР± (accession РїР°СЂР°РјРµС‚СЂ)
    print(f"2. РќРѕРІС‹Р№ СЃРїРѕСЃРѕР± (accession):")
    url2 = f"{base_url}/targets"
    params2 = {"accession": uniprot_id, "database": "UniProt", "limit": 5}
    try:
        response2 = session.get(url2, params=params2, timeout=30)
        print(f"   URL: {response2.url}")
        print(f"   Status: {response2.status_code}")
        data2 = response2.json()
        if isinstance(data2, list) and data2:
            print(f"   Р РµР·СѓР»СЊС‚Р°С‚РѕРІ: {len(data2)}")
            print(f"   РџРµСЂРІС‹Р№: targetId={data2[0].get('targetId', 'N/A')}, name={data2[0].get('name', 'N/A')}")
        else:
            print(f"   Р РµР·СѓР»СЊС‚Р°С‚: {data2}")
    except Exception as e:
        print(f"   РћС€РёР±РєР°: {e}")
    
    # РўРµСЃС‚ 3: РџРѕРїСЂРѕР±СѓРµРј РґСЂСѓРіРёРµ РІР°СЂРёР°РЅС‚С‹ РїР°СЂР°РјРµС‚СЂРѕРІ
    print(f"3. РђР»СЊС‚РµСЂРЅР°С‚РёРІРЅС‹Рµ РїР°СЂР°РјРµС‚СЂС‹:")
    url3 = f"{base_url}/targets"
    params3 = {"uniprot": uniprot_id, "limit": 5}
    try:
        response3 = session.get(url3, params=params3, timeout=30)
        print(f"   URL: {response3.url}")
        print(f"   Status: {response3.status_code}")
        data3 = response3.json()
        if isinstance(data3, list) and data3:
            print(f"   Р РµР·СѓР»СЊС‚Р°С‚РѕРІ: {len(data3)}")
            print(f"   РџРµСЂРІС‹Р№: targetId={data3[0].get('targetId', 'N/A')}, name={data3[0].get('name', 'N/A')}")
        else:
            print(f"   Р РµР·СѓР»СЊС‚Р°С‚: {data3}")
    except Exception as e:
        print(f"   РћС€РёР±РєР°: {e}")

print(f"\n=== РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ ===")

