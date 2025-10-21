#!/usr/bin/env python3
"""Р¤РёРЅР°Р»СЊРЅР°СЏ РІР°Р»РёРґР°С†РёСЏ РёСЃРїСЂР°РІР»РµРЅРёСЏ IUPHAR РјР°РїРїРёРЅРіР°."""

import logging
from pathlib import Path

import pandas as pd

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ
from library.target.iuphar_adapter import IupharApiCfg, enrich_targets_with_iuphar

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

print("=== Р¤РёРЅР°Р»СЊРЅР°СЏ РІР°Р»РёРґР°С†РёСЏ РёСЃРїСЂР°РІР»РµРЅРёСЏ IUPHAR РјР°РїРїРёРЅРіР° ===")

# Р§РёС‚Р°РµРј СЂРµР°Р»СЊРЅС‹Рµ РґР°РЅРЅС‹Рµ РёР· С„Р°Р№Р»Р° СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ
input_file = "data/output/target/target_20251021.csv"
if Path(input_file).exists():
    print(f"Р§РёС‚Р°РµРј РґР°РЅРЅС‹Рµ РёР· {input_file}")
    df = pd.read_csv(input_file)
    print(f"Р—Р°РіСЂСѓР¶РµРЅРѕ {len(df)} Р·Р°РїРёСЃРµР№")
    
    # Р‘РµСЂРµРј РїРµСЂРІС‹Рµ 10 Р·Р°РїРёСЃРµР№ РґР»СЏ С‚РµСЃС‚РёСЂРѕРІР°РЅРёСЏ
    test_df = df.head(10).copy()
    print(f"РўРµСЃС‚РёСЂСѓРµРј РїРµСЂРІС‹Рµ {len(test_df)} Р·Р°РїРёСЃРµР№")
    
    # РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ
    cfg = IupharApiCfg()
    
    print("\n--- Р—Р°РїСѓСЃРє IUPHAR РѕР±РѕРіР°С‰РµРЅРёСЏ ---")
    try:
        # Р—Р°РїСѓСЃРєР°РµРј РѕР±РѕРіР°С‰РµРЅРёРµ
        result_df = enrich_targets_with_iuphar(test_df, cfg)
        
        print(f"Р РµР·СѓР»СЊС‚Р°С‚: {len(result_df)} Р·Р°РїРёСЃРµР№")
        
        # РђРЅР°Р»РёР·РёСЂСѓРµРј СЂРµР·СѓР»СЊС‚Р°С‚С‹
        print("\n--- РђРЅР°Р»РёР· СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ ---")
        iuphar_ids = result_df['iuphar_target_id'].value_counts()
        print(f"РЈРЅРёРєР°Р»СЊРЅС‹С… iuphar_target_id: {len(iuphar_ids)}")
        print("Р Р°СЃРїСЂРµРґРµР»РµРЅРёРµ iuphar_target_id:")
        for iuphar_id, count in iuphar_ids.head(10).items():
            print(f"  {iuphar_id}: {count} Р·Р°РїРёСЃРµР№")
        
        # РџСЂРѕРІРµСЂСЏРµРј РїСЂРѕР±Р»РµРјСѓ СЃ 1386
        if '1386' in iuphar_ids.index:
            count_1386 = iuphar_ids['1386']
            total = len(result_df)
            percentage = (count_1386 / total) * 100
            print(f"\n[РџР РћР‘Р›Р•РњРђ] Р’СЃРµ РµС‰Рµ РµСЃС‚СЊ {count_1386} Р·Р°РїРёСЃРµР№ СЃ iuphar_target_id=1386 ({percentage:.1f}%)")
        else:
            print("\n[РЈРЎРџР•РҐ] РџСЂРѕР±Р»РµРјР° СЃ iuphar_target_id=1386 РїРѕР»РЅРѕСЃС‚СЊСЋ СЂРµС€РµРЅР°!")
        
        # РџРѕРєР°Р·С‹РІР°РµРј РїСЂРёРјРµСЂС‹ Р·Р°РїРёСЃРµР№
        print("\n--- РџСЂРёРјРµСЂС‹ Р·Р°РїРёСЃРµР№ ---")
        for _, row in result_df.head(5).iterrows():
            chembl_id = row['target_chembl_id']
            iuphar_id = row['iuphar_target_id']
            iuphar_name = row['iuphar_name']
            pref_name = row['pref_name']
            print(f"{chembl_id}: {pref_name} -> iuphar_target_id={iuphar_id}, iuphar_name={iuphar_name}")
            
    except Exception as e:
        print(f"РћС€РёР±РєР°: {e}")
        import traceback
        traceback.print_exc()
        
else:
    print(f"Р¤Р°Р№Р» {input_file} РЅРµ РЅР°Р№РґРµРЅ")
    print("РЎРѕР·РґР°РµРј С‚РµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ...")
    
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
    
    test_df = pd.DataFrame(test_data)
    print(f"РЎРѕР·РґР°РЅРѕ {len(test_df)} С‚РµСЃС‚РѕРІС‹С… Р·Р°РїРёСЃРµР№")

print("\n=== Р’Р°Р»РёРґР°С†РёСЏ Р·Р°РІРµСЂС€РµРЅР° ===")

