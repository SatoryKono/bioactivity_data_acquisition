"""Совместимость с прежним путём теста `test_chembl_input_file_branching`."""

from __future__ import annotations

from tests.bioetl.pipelines.common.test_chembl_input_file_branching import (
    test_extract_uses_shared_input_file_helper,
)

__all__ = ["test_extract_uses_shared_input_file_helper"]
