#!/usr/bin/env python3
"""РўРµСЃС‚ С„СѓРЅРєС†РёРё iter_iuphar_batches РґР»СЏ РїРѕРЅРёРјР°РЅРёСЏ РїСЂРѕР±Р»РµРјС‹."""

import pytest
import logging
import sys
from pathlib import Path
import pandas as pd

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ


from library.target.iuphar_adapter import iter_iuphar_batches, IupharApiCfg

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== РўРµСЃС‚ С„СѓРЅРєС†РёРё iter_iuphar_batches ===")

# РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ
cfg = IupharApiCfg()

# РўРµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ
test_data = [
    {
        "target_chembl_id": "CHEMBL1234",
        "pref_name": "Enoyl-(Acyl-carrier-protein) reductase",
        "target_type": "SINGLE PROTEIN",
        "mapping_uniprot_id": "Q9FBC5",
        "gene_symbol": "fabI"
    }
]

print(f"РўРµСЃС‚РёСЂСѓРµРј {len(test_data)} target")

# РўРµСЃС‚РёСЂСѓРµРј iter_iuphar_batches
results = []
try:
    for result in iter_iuphar_batches(iter(test_data), cfg, batch_size=1):
        results.append(result)
        print(f"РџРѕР»СѓС‡РµРЅ СЂРµР·СѓР»СЊС‚Р°С‚: {result}")
    
    print(f"Р’СЃРµРіРѕ СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ: {len(results)}")
    
    if results:
        print("Р РµР·СѓР»СЊС‚Р°С‚С‹:")
        for i, result in enumerate(results):
            print(f"  {i+1}. {result}")
    else:
        print("Р РµР·СѓР»СЊС‚Р°С‚РѕРІ РЅРµС‚!")
        
except Exception as e:
    print(f"РћС€РёР±РєР°: {e}")
    import traceback
    traceback.print_exc()

print(f"\n=== РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ ===")

