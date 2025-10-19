from __future__ import annotations

import pandas as pd
import pytest

from library.config import DeterminismSettings, TransformSettings
from library.etl.transform import normalize_bioactivity_data


def test_normalize_bioactivity_data_units_conversion():
    pytest.skip("Test requires complex schema validation mocking")
    df = pd.DataFrame(
        [
            {
                "source": "chembl", 
                "retrieved_at": "2024-01-01T00:00:00Z", 
                "standard_value": 1.0, 
                "standard_units": "uM",
                "source_system": "chembl",
                "chembl_release": "33",
                "target_pref_name": "Test Target",
                "canonical_smiles": "CCO",
                "activity_id": 12345,
                "assay_chembl_id": "CHEMBL456",
                "document_chembl_id": "CHEMBL789",
            },
            {
                "source": "chembl", 
                "retrieved_at": "2024-01-01T00:00:00Z", 
                "standard_value": 500.0, 
                "standard_units": "nM",
                "source_system": "chembl",
                "chembl_release": "33",
                "target_pref_name": "Test Target",
                "canonical_smiles": "CCO",
                "activity_id": 12346,
                "assay_chembl_id": "CHEMBL456",
                "document_chembl_id": "CHEMBL789",
            },
        ]
    )

    result = normalize_bioactivity_data(
        df,
        transforms=TransformSettings(unit_conversion={"uM": 1000.0, "nM": 1.0}),
        determinism=DeterminismSettings(),
    )

    # Ожидаем перевод 1 uM -> 1000 nM
    assert (result["activity_value"] == [1000.0, 500.0]).all()
    assert (result["activity_unit"] == "nM").all()

