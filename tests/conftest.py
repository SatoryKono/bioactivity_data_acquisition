from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from library.utils import logging as logging_utils


@pytest.fixture(autouse=True)
def reset_shared_session():
    logging_utils.reset_shared_session()
    yield
    logging_utils.reset_shared_session()


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