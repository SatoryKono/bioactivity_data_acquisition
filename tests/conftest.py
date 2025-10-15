from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    from library.clients.session import reset_shared_session as _reset_shared_session
except ImportError:  # pragma: no cover - optional when clients are not available
    def _reset_shared_session() -> None:  # type: ignore[return-type]
        """Fallback no-op when the shared session cannot be imported."""

        return None


def pytest_configure() -> None:
    src_str = str(SRC)
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
