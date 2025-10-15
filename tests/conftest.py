from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bioactivity.clients.session import reset_shared_session as _reset_shared_session


def pytest_configure() -> None:
    src = Path(__file__).resolve().parents[1] / "src"
    src_str = str(src)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)


@pytest.fixture(autouse=True)
def reset_shared_session():
    _reset_shared_session()
    yield
    _reset_shared_session()


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
