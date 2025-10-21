#!/usr/bin/env python3
"""РўРµСЃС‚ РёСЃРїСЂР°РІР»РµРЅРЅРѕРіРѕ IUPHAR РјР°РїРїРёРЅРіР°."""

import logging

import pandas as pd

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ
from library.target.iuphar_adapter import IupharApiCfg, enrich_targets_with_iuphar

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== РўРµСЃС‚ РёСЃРїСЂР°РІР»РµРЅРЅРѕРіРѕ IUPHAR РјР°РїРїРёРЅРіР° ===")

# РЎРѕР·РґР°РµРј С‚РµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ
test_data = [
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
    },
    {
        "target_chembl_id": "CHEMBL9999",
        "pref_name": "Dihydrofolate reductase", 
        "target_type": "SINGLE PROTEIN",
        "mapping_uniprot_id": "Q81VW8",
        "gene_symbol": "DHFR"
    }
]

print(f"РўРµСЃС‚РёСЂСѓРµРј {len(test_data)} targets:")
for i, target in enumerate(test_data, 1):
    print(f"  {i}. {target['target_chembl_id']}: {target['pref_name']}")

# РЎРѕР·РґР°РµРј DataFrame
df = pd.DataFrame(test_data)

# РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ
cfg = IupharApiCfg()

print("\n--- Р—Р°РїСѓСЃРє IUPHAR РѕР±РѕРіР°С‰РµРЅРёСЏ ---")
try:
    # Р—Р°РїСѓСЃРєР°РµРј РѕР±РѕРіР°С‰РµРЅРёРµ
    result_df = enrich_targets_with_iuphar(df, cfg)
    
    print(f"Р РµР·СѓР»СЊС‚Р°С‚: {len(result_df)} Р·Р°РїРёСЃРµР№")
    
    # РђРЅР°Р»РёР·РёСЂСѓРµРј СЂРµР·СѓР»СЊС‚Р°С‚С‹
    print("\n--- РђРЅР°Р»РёР· СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ ---")
    iuphar_ids = result_df['iuphar_target_id'].value_counts()
    print(f"РЈРЅРёРєР°Р»СЊРЅС‹С… iuphar_target_id: {len(iuphar_ids)}")
    print("Р Р°СЃРїСЂРµРґРµР»РµРЅРёРµ iuphar_target_id:")
    for iuphar_id, count in iuphar_ids.items():
        print(f"  {iuphar_id}: {count} Р·Р°РїРёСЃРµР№")
    
    # РџРѕРєР°Р·С‹РІР°РµРј РґРµС‚Р°Р»Рё РґР»СЏ РєР°Р¶РґРѕР№ Р·Р°РїРёСЃРё
    print("\n--- Р”РµС‚Р°Р»Рё Р·Р°РїРёСЃРµР№ ---")
    for _, row in result_df.iterrows():
        chembl_id = row['target_chembl_id']
        iuphar_id = row['iuphar_target_id']
        iuphar_name = row['iuphar_name']
        print(f"{chembl_id}: iuphar_target_id={iuphar_id}, iuphar_name={iuphar_name}")
    
    # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ Р±РѕР»СЊС€Рµ РЅРµС‚ РїСЂРѕР±Р»РµРјС‹ СЃ 1386
    if '1386' in iuphar_ids.index:
        print("\n[РџР РћР‘Р›Р•РњРђ] Р’СЃРµ РµС‰Рµ РµСЃС‚СЊ Р·Р°РїРёСЃРё СЃ iuphar_target_id=1386!")
    else:
        print("\n[РЈРЎРџР•РҐ] РџСЂРѕР±Р»РµРјР° СЃ iuphar_target_id=1386 СЂРµС€РµРЅР°!")
        
except Exception as e:
    print(f"РћС€РёР±РєР°: {e}")
    import traceback
    traceback.print_exc()

print("\n=== РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ ===")

