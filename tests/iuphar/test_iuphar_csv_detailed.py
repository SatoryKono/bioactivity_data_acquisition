#!/usr/bin/env python3
"""Р”РµС‚Р°Р»СЊРЅС‹Р№ С‚РµСЃС‚ CSV lookup РґР»СЏ РїРѕРЅРёРјР°РЅРёСЏ СЃРѕРІРїР°РґРµРЅРёСЏ."""

import pytest
import logging
import sys
from pathlib import Path
import pandas as pd

# Р”РѕР±Р°РІР»СЏРµРј РєРѕСЂРЅРµРІСѓСЋ РґРёСЂРµРєС‚РѕСЂРёСЋ РїСЂРѕРµРєС‚Р° РІ РїСѓС‚СЊ РґР»СЏ РёРјРїРѕСЂС‚РѕРІ


from library.target.iuphar_adapter import _build_iuphar_lookup_index, IupharApiCfg

# РќР°СЃС‚СЂР°РёРІР°РµРј Р»РѕРіРёСЂРѕРІР°РЅРёРµ
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s: %(message)s')

print("=== Р”РµС‚Р°Р»СЊРЅС‹Р№ С‚РµСЃС‚ CSV lookup ===")

# РЎРѕР·РґР°РµРј РєРѕРЅС„РёРіСѓСЂР°С†РёСЋ
cfg = IupharApiCfg()

# РЎС‚СЂРѕРёРј lookup РёРЅРґРµРєСЃ
print("РЎС‚СЂРѕРёРј lookup РёРЅРґРµРєСЃ...")
lookup_index = _build_iuphar_lookup_index(cfg)

# РўРµСЃС‚РѕРІС‹Рµ РґР°РЅРЅС‹Рµ
target_data = {
    "target_chembl_id": "CHEMBL1234",
    "pref_name": "Enoyl-(Acyl-carrier-protein) reductase",
    "target_type": "SINGLE PROTEIN",
    "mapping_uniprot_id": "Q9FBC5",
    "gene_symbol": "fabI"
}

print(f"\n--- РўРµСЃС‚РёСЂСѓРµРј target: {target_data['target_chembl_id']} ---")
print(f"pref_name: {target_data['pref_name']}")
print(f"gene_symbol: {target_data['gene_symbol']}")
print(f"mapping_uniprot_id: {target_data['mapping_uniprot_id']}")

# РџСЂРѕРІРµСЂСЏРµРј РєР°Р¶РґС‹Р№ РёРЅРґРµРєСЃ РѕС‚РґРµР»СЊРЅРѕ
print(f"\n--- РџСЂРѕРІРµСЂРєР° РёРЅРґРµРєСЃРѕРІ ---")

# 1. РџРѕРёСЃРє РїРѕ target_id
target_id = target_data.get("target_id", "")
if target_id and target_id in lookup_index["by_target_id"]:
    print(f"[FOUND] РќР°Р№РґРµРЅРѕ РїРѕ target_id: {target_id}")
    csv_data = lookup_index["by_target_id"][target_id]
    print(f"  CSV РґР°РЅРЅС‹Рµ: target_id={csv_data.get('target_id', 'N/A')}, name={csv_data.get('target_name', 'N/A')}")
else:
    print(f"[NOT FOUND] РќРµ РЅР°Р№РґРµРЅРѕ РїРѕ target_id: {target_id}")

# 2. РџРѕРёСЃРє РїРѕ uniprot_id
uniprot_id = target_data.get("mapping_uniprot_id", "")
if uniprot_id and uniprot_id in lookup_index["by_uniprot"]:
    print(f"[FOUND] РќР°Р№РґРµРЅРѕ РїРѕ uniprot_id: {uniprot_id}")
    csv_data = lookup_index["by_uniprot"][uniprot_id]
    print(f"  CSV РґР°РЅРЅС‹Рµ: target_id={csv_data.get('target_id', 'N/A')}, name={csv_data.get('target_name', 'N/A')}")
else:
    print(f"[NOT FOUND] РќРµ РЅР°Р№РґРµРЅРѕ РїРѕ uniprot_id: {uniprot_id}")

# 3. РџРѕРёСЃРє РїРѕ gene_symbol
gene_symbol = target_data.get("gene_symbol", "")
if gene_symbol and gene_symbol in lookup_index["by_gene"]:
    print(f"[FOUND] РќР°Р№РґРµРЅРѕ РїРѕ gene_symbol: {gene_symbol}")
    csv_data = lookup_index["by_gene"][gene_symbol]
    print(f"  CSV РґР°РЅРЅС‹Рµ: target_id={csv_data.get('target_id', 'N/A')}, name={csv_data.get('target_name', 'N/A')}")
else:
    print(f"[NOT FOUND] РќРµ РЅР°Р№РґРµРЅРѕ РїРѕ gene_symbol: {gene_symbol}")

# 4. РџРѕРёСЃРє РїРѕ pref_name
pref_name = target_data.get("pref_name", "")
if pref_name and pref_name in lookup_index["by_name"]:
    print(f"[FOUND] РќР°Р№РґРµРЅРѕ РїРѕ pref_name: {pref_name}")
    csv_data = lookup_index["by_name"][pref_name]
    print(f"  CSV РґР°РЅРЅС‹Рµ: target_id={csv_data.get('target_id', 'N/A')}, name={csv_data.get('target_name', 'N/A')}")
else:
    print(f"[NOT FOUND] РќРµ РЅР°Р№РґРµРЅРѕ РїРѕ pref_name: {pref_name}")

# РџСЂРѕРІРµСЂСЏРµРј, РµСЃС‚СЊ Р»Рё РїРѕС…РѕР¶РёРµ Р·Р°РїРёСЃРё РІ РёРЅРґРµРєСЃР°С…
print(f"\n--- РџРѕРёСЃРє РїРѕС…РѕР¶РёС… Р·Р°РїРёСЃРµР№ ---")

# РџРѕРёСЃРє РїРѕ С‡Р°СЃС‚РёС‡РЅРѕРјСѓ СЃРѕРІРїР°РґРµРЅРёСЋ gene_symbol
if gene_symbol:
    similar_genes = [key for key in lookup_index["by_gene"].keys() if gene_symbol.lower() in key.lower() or key.lower() in gene_symbol.lower()]
    if similar_genes:
        print(f"РџРѕС…РѕР¶РёРµ gene_symbol: {similar_genes[:5]}")
        for similar_gene in similar_genes[:3]:
            csv_data = lookup_index["by_gene"][similar_gene]
            print(f"  {similar_gene}: target_id={csv_data.get('target_id', 'N/A')}, name={csv_data.get('target_name', 'N/A')}")

# РџРѕРёСЃРє РїРѕ С‡Р°СЃС‚РёС‡РЅРѕРјСѓ СЃРѕРІРїР°РґРµРЅРёСЋ pref_name
if pref_name:
    similar_names = [key for key in lookup_index["by_name"].keys() if any(word in key.lower() for word in pref_name.lower().split())]
    if similar_names:
        print(f"РџРѕС…РѕР¶РёРµ pref_name: {similar_names[:5]}")
        for similar_name in similar_names[:3]:
            csv_data = lookup_index["by_name"][similar_name]
            print(f"  {similar_name}: target_id={csv_data.get('target_id', 'N/A')}, name={csv_data.get('target_name', 'N/A')}")

print(f"\n=== РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ ===")

