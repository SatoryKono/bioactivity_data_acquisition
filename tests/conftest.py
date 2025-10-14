from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture()
def sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "assay_id": [1, 2],
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL2"],
            "standard_value": [1.5, 2.5],
            "standard_units": ["nM", "uM"],
            "activity_comment": [None, "active"],
        }
    )
