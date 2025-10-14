import sys
from pathlib import Path

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
