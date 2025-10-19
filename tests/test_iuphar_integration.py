from __future__ import annotations

import pandas as pd

from src.library.pipelines.target.iuphar_target import IupharApiCfg, enrich_targets_with_iuphar


def _make_cfg() -> IupharApiCfg:
    return IupharApiCfg(
        use_csv=True,
        dictionary_path="configs/dictionary/_target/",
        family_file="_IUPHAR_family.csv",
        target_file="_IUPHAR_target.csv",
        base_url="https://www.guidetopharmacology.org/services",
        timeout=10.0,
        batch_size=10,
        rate_limit_delay=0.0,
    )


def test_iuphar_csv_mapping_basic():
    # Q8TCD5 is present in _IUPHAR_target.csv sample shown in repo
    df = pd.DataFrame(
        {
            "target_chembl_id": ["CHEMBL_TEST1"],
            "mapping_uniprot_id": ["Q8TCD5"],
            "uniprot_id_primary": [""],
            "pref_name": ["test protein"],
        }
    )
    cfg = _make_cfg()
    enriched = enrich_targets_with_iuphar(df, cfg, batch_size=10)

    assert not enriched.empty
    row = enriched.iloc[0]
    # Validate essential IUPHAR fields are populated
    assert "iuphar_target_id" in enriched.columns
    assert "iuphar_family_id" in enriched.columns
    assert "iuphar_type" in enriched.columns
    assert "iuphar_class" in enriched.columns
    assert "iuphar_subclass" in enriched.columns
    assert "iuphar_name" in enriched.columns
    assert isinstance(row.get("iuphar_target_id"), str)
    assert isinstance(row.get("iuphar_type"), str)


def test_iuphar_csv_fallback_uniprot_primary_when_mapping_empty():
    # mapping_uniprot_id is empty -> fallback to uniprot_id_primary
    df = pd.DataFrame(
        {
            "target_chembl_id": ["CHEMBL_TEST2"],
            "mapping_uniprot_id": [""],
            "uniprot_id_primary": ["Q8TCD5"],
            "pref_name": ["test protein"],
        }
    )
    cfg = _make_cfg()
    enriched = enrich_targets_with_iuphar(df, cfg, batch_size=10)
    assert not enriched.empty
    row = enriched.iloc[0]
    assert isinstance(row.get("iuphar_target_id"), str)
    # At least type/class inferred when dictionary present
    assert isinstance(row.get("iuphar_type"), str)


