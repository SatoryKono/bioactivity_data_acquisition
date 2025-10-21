#!/usr/bin/env python3
"""РўРµСЃС‚ CSV lookup РґР»СЏ РїРѕРЅРёРјР°РЅРёСЏ РїСЂРѕР±Р»РµРјС‹ СЃ 1386."""

import logging

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ
from library.target.iuphar_adapter import IupharApiCfg, _build_iuphar_lookup_index, _map_via_csv_first

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== РўРµСЃС‚ CSV lookup РґР»СЏ РїРѕРЅРёРјР°РЅРёСЏ РїСЂРѕР±Р»РµРјС‹ СЃ 1386 ===")

# РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ
cfg = IupharApiCfg()

# РЎС‚СЂРѕРёРј lookup РёРЅРґРµРєСЃ
print("РЎС‚СЂРѕРёРј lookup РёРЅРґРµРєСЃ...")
lookup_index = _build_iuphar_lookup_index(cfg)

# РўРµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ
test_targets = [
    {
        "target_chembl_id": "CHEMBL1234",
        "pref_name": "Enoyl-(Acyl-carrier-protein) reductase",
        "target_type": "SINGLE PROTEIN",
        "mapping_uniprot_id": "Q9FBC5",
        "gene_symbol": "fabI"
    },
    {
        "target_chembl_id": "CHEMBL5678", 
        "pref_name": "A1 adenosine receptor",
        "target_type": "SINGLE PROTEIN",
        "mapping_uniprot_id": "Q4VQ11",
        "gene_symbol": "ADORA1"
    }
]

print(f"\n--- РўРµСЃС‚РёСЂСѓРµРј CSV lookup РґР»СЏ {len(test_targets)} targets ---")

for target_data in test_targets:
    chembl_id = target_data["target_chembl_id"]
    print(f"\n--- {chembl_id}: {target_data['pref_name']} ---")
    
    # РџСЂРѕРІРµСЂСЏРµРј РєР°Р¶РґС‹Р№ С‚РёРї РїРѕРёСЃРєР°
    print(f"1. РџРѕРёСЃРє РїРѕ target_id: {target_data.get('target_id', 'N/A')}")
    print(f"2. РџРѕРёСЃРє РїРѕ uniprot_id: {target_data.get('mapping_uniprot_id', 'N/A')}")
    print(f"3. РџРѕРёСЃРє РїРѕ gene_symbol: {target_data.get('gene_symbol', 'N/A')}")
    print(f"4. РџРѕРёСЃРє РїРѕ pref_name: {target_data.get('pref_name', 'N/A')}")
    
    # Р—Р°РїСѓСЃРєР°РµРј CSV lookup
    result = _map_via_csv_first(target_data, lookup_index, cfg)
    
    if result:
        print(f"Р РµР·СѓР»СЊС‚Р°С‚ CSV lookup: iuphar_target_id={result.get('iuphar_target_id', 'N/A')}, iuphar_name={result.get('iuphar_name', 'N/A')}")
    else:
        print("CSV lookup РЅРµ РЅР°С€РµР» СЃРѕРІРїР°РґРµРЅРёР№")
    
    # РџСЂРѕРІРµСЂСЏРµРј, РµСЃС‚СЊ Р»Рё UniProt ID РІ РёРЅРґРµРєСЃРµ
    uniprot_id = target_data.get("mapping_uniprot_id", "")
    if uniprot_id and uniprot_id in lookup_index["by_uniprot"]:
        print(f"UniProt ID {uniprot_id} РЅР°Р№РґРµРЅ РІ CSV СЃР»РѕРІР°СЂРµ!")
        csv_data = lookup_index["by_uniprot"][uniprot_id]
        print(f"  CSV РґР°РЅРЅС‹Рµ: target_id={csv_data.get('target_id', 'N/A')}, name={csv_data.get('target_name', 'N/A')}")
    else:
        print(f"UniProt ID {uniprot_id} РќР• РЅР°Р№РґРµРЅ РІ CSV СЃР»РѕРІР°СЂРµ")

print("\n=== РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ ===")

