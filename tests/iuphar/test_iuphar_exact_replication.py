#!/usr/bin/env python3
"""РўРѕС‡РЅРѕРµ РІРѕСЃРїСЂРѕРёР·РІРµРґРµРЅРёРµ Р»РѕРіРёРєРё iter_iuphar_batches."""

import pytest
import logging
import sys
from pathlib import Path
import pandas as pd

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ


from library.target.iuphar_adapter import (
    _build_iuphar_lookup_index, 
    _map_via_csv_first, 
    _map_via_api, 
    _map_via_simple_fallback,
    _create_iuphar_session,
    IupharApiCfg
)

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ РЅР° DEBUG СѓСЂРѕРІРµРЅСЊ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== РўРѕС‡РЅРѕРµ РІРѕСЃРїСЂРѕРёР·РІРµРґРµРЅРёРµ Р»РѕРіРёРєРё iter_iuphar_batches ===")

# РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ
cfg = IupharApiCfg()

# РЎС‚СЂРѕРёРј lookup РёРЅРґРµРєСЃ
print("РЎС‚СЂРѕРёРј lookup РёРЅРґРµРєСЃ...")
lookup_index = _build_iuphar_lookup_index(cfg)
session = _create_iuphar_session(cfg)

# РўРµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ
target_data = {
    "target_chembl_id": "CHEMBL1234",
    "pref_name": "Enoyl-(Acyl-carrier-protein) reductase",
    "target_type": "SINGLE PROTEIN",
    "mapping_uniprot_id": "Q9FBC5",
    "gene_symbol": "fabI"
}

print(f"\n--- РўРµСЃС‚РёСЂСѓРµРј С‚РѕС‡РЅСѓСЋ Р»РѕРіРёРєСѓ РґР»СЏ {target_data['target_chembl_id']} ---")

# РўРѕС‡РЅР°СЏ Р»РѕРіРёРєР° РёР· iter_iuphar_batches
mapped = None

# 1. РџРѕРїС‹С‚РєР° С‡РµСЂРµР· CSV lookup
if any(lookup_index.values()):
    print("1. РџСЂРѕР±СѓРµРј CSV lookup...")
    mapped = _map_via_csv_first(target_data, lookup_index, cfg)
    if mapped:
        print(f"   CSV lookup РЅР°С€РµР»: iuphar_target_id={mapped.get('iuphar_target_id', 'N/A')}")
    else:
        print("   CSV lookup РЅРµ РЅР°С€РµР» СЃРѕРІРїР°РґРµРЅРёР№")

# 2. Fallback РЅР° API РµСЃР»Рё CSV РЅРµ РґР°Р» СЂРµР·СѓР»СЊС‚Р°С‚Р°
if not mapped:
    print("2. РџСЂРѕР±СѓРµРј API lookup...")
    mapped = _map_via_api(target_data, session, cfg)
    if mapped:
        print(f"   API lookup РЅР°С€РµР»: iuphar_target_id={mapped.get('iuphar_target_id', 'N/A')}")
    else:
        print("   API lookup РЅРµ РЅР°С€РµР» СЃРѕРІРїР°РґРµРЅРёР№")

# 3. Fallback РЅР° simple fallback РµСЃР»Рё API С‚РѕР¶Рµ РЅРµ РґР°Р» СЂРµР·СѓР»СЊС‚Р°С‚Р°
if not mapped:
    print("3. РџСЂРѕР±СѓРµРј simple fallback...")
    mapped = _map_via_simple_fallback(target_data)
    if mapped:
        print(f"   Simple fallback СЃРѕР·РґР°Р»: iuphar_target_id={mapped.get('iuphar_target_id', 'N/A')}")
    else:
        print("   Simple fallback РЅРµ СЃРѕР·РґР°Р» СЂРµР·СѓР»СЊС‚Р°С‚")

# Р¤РёРЅР°Р»СЊРЅС‹Р№ СЂРµР·СѓР»СЊС‚Р°С‚
if mapped:
    mapped["source_target_chembl_id"] = target_data.get("target_chembl_id", "")
    print(f"\nР¤РёРЅР°Р»СЊРЅС‹Р№ СЂРµР·СѓР»СЊС‚Р°С‚: iuphar_target_id={mapped.get('iuphar_target_id', 'N/A')}")
else:
    print("\nРќРёРєР°РєРѕРіРѕ СЂРµР·СѓР»СЊС‚Р°С‚Р° РЅРµ РїРѕР»СѓС‡РµРЅРѕ!")

print(f"\n=== РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ ===")

