from __future__ import annotations

import pandas as pd
import pytest

from library.transform import normalise_units


def test_normalise_units_converts_all_to_nm(sample_frame: pd.DataFrame) -> None:
    sample_frame["source"] = "chembl"
    result = normalise_units(sample_frame)
    assert set(result["standard_units"].unique()) == {"nM"}
    expected = [1.5, 2_500.0]
    assert result["standard_value_nm"].tolist() == expected


def test_normalise_units_rejects_unknown_unit(sample_frame: pd.DataFrame) -> None:
    sample_frame.loc[0, "standard_units"] = "mg/mL"
    sample_frame["source"] = "chembl"
    with pytest.raises(ValueError):
        normalise_units(sample_frame, strict=False)
