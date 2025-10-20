from __future__ import annotations

import pandas as pd

from library.pipelines.target.uniprot_target import _parse_uniprot_entry
from library.pipelines.target.postprocessing import postprocess_targets


def test_parse_uniprot_entry_isoforms_basic() -> None:
    entry = {
        "primaryAccession": "P12345",
        "comments": [
            {
                "commentType": "ALTERNATIVE PRODUCTS",
                "isoforms": [
                    {
                        "name": {"value": "alpha"},
                        "isoformIds": ["P12345-1", "VAR_1"],
                        "synonyms": [{"value": "A1"}, {"value": "Alpha"}],
                    },
                    {
                        "name": {"value": "beta"},
                        "isoformIds": ["P12345-2"],
                        "synonyms": [],
                    },
                ],
            }
        ],
    }

    result = _parse_uniprot_entry(entry)

    assert result["isoform_names"] == "alpha|beta"
    assert result["isoform_ids"] == "P12345-1:VAR_1|P12345-2"
    # When an isoform has no synonyms, we expect "N/A" for that isoform slot
    assert result["isoform_synonyms"] == "A1:Alpha|N/A"


def test_parse_uniprot_entry_no_isoforms() -> None:
    entry = {"primaryAccession": "Q99999", "comments": []}

    result = _parse_uniprot_entry(entry)

    assert result["isoform_names"] == "None"
    assert result["isoform_ids"] == "None"
    assert result["isoform_synonyms"] == "None"


def test_postprocess_targets_replaces_none_with_dash() -> None:
    # Minimal frame containing isoform columns with "None" and essential IDs
    df = pd.DataFrame(
        {
            "target_chembl_id": ["CHEMBL1"],
            "uniProtkbId": ["P12345"],
            "geneName": ["GENE1"],
            "isoform_names": ["None"],
            "isoform_ids": ["None"],
            "isoform_synonyms": ["None"],
        }
    )

    processed = postprocess_targets(df)
    row = processed.iloc[0]

    assert row["isoform_names"] == "-"
    assert row["isoform_ids"] == "-"
    assert row["isoform_synonyms"] == "-"


def test_parse_uniprot_entry_transmembrane_intramembrane_and_reactions() -> None:
    entry = {
        "primaryAccession": "P00001",
        "features": [
            {"type": "Transmembrane region", "description": "helix 1"},
            {"type": "Intramembrane region", "description": "span"},
        ],
        "comments": [
            {
                "commentType": "CATALYTIC ACTIVITY",
                "reaction": {
                    "name": {"value": "L-arginine + H2O = L-ornithine + urea"},
                    "ecNumber": {"value": "3.5.3.1"},
                },
            }
        ],
    }

    result = _parse_uniprot_entry(entry)

    # Boolean flags
    assert result["transmembrane"] is True
    assert result["intramembrane"] is True

    # Catalytic activity
    assert result["reactions"] == "L-arginine + H2O = L-ornithine + urea"
    assert result["reaction_ec_numbers"] == "3.5.3.1"


def test_parse_uniprot_entry_reaction_ec_formats() -> None:
    # ecNumber as list[dict], string, and alias ecNumbers
    entry = {
        "primaryAccession": "P00002",
        "comments": [
            {
                "commentType": "CATALYTIC ACTIVITY",
                "reaction": {
                    "name": "A = B",
                    "ecNumber": [
                        {"value": "1.1.1.1"},
                        {"value": "2.2.2.2"},
                    ],
                },
            },
            {
                "commentType": "CATALYTIC ACTIVITY",
                "reaction": {
                    "name": {"value": "C = D"},
                    "ecNumbers": "3.3.3.3",
                },
            },
        ],
    }

    result = _parse_uniprot_entry(entry)

    assert result["reactions"] == "A = B|C = D"
    assert result["reaction_ec_numbers"] == "1.1.1.1|2.2.2.2|3.3.3.3"


