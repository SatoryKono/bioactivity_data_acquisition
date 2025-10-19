from __future__ import annotations

import pandas as pd

from library.etl.transform import normalize_bioactivity_data
from library.config import DeterminismSettings, TransformSettings


def test_normalize_bioactivity_data_units_conversion():
    df = pd.DataFrame(
        [
            {"source": "chembl", "retrieved_at": "2024-01-01T00:00:00Z", "standard_value": 1.0, "standard_units": "uM"},
            {"source": "chembl", "retrieved_at": "2024-01-01T00:00:00Z", "standard_value": 500.0, "standard_units": "nM"},
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

