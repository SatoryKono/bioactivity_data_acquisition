from __future__ import annotations

import pytest


@pytest.fixture()
def sample_uniprot_entry() -> dict[str, object]:
    return {
        "primaryAccession": "P12345",
        "genes": [
            {
                "geneName": {"value": "ABC1"},
                "synonyms": [{"value": "DEF"}, {"value": "GHI"}],
            }
        ],
        "proteinDescription": {
            "recommendedName": {"fullName": {"value": "Protein ABC"}},
        },
        "sequence": {"length": 512},
        "organism": {
            "taxonId": 9606,
            "scientificName": "Homo sapiens",
            "lineage": ["Eukaryota", "Metazoa"],
        },
        "secondaryAccession": ["Q99999"],
        "comments": [
            {
                "commentType": "ALTERNATIVE PRODUCTS",
                "isoforms": [
                    {
                        "isoformIds": ["P12345-2"],
                        "names": [{"value": "Isoform 2"}],
                        "sequence": {"length": 300},
                    }
                ],
            }
        ],
    }
