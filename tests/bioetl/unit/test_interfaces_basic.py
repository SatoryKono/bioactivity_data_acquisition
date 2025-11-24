from __future__ import annotations

import bioetl
from bioetl.base_classes import BaseApiClient, INormalizer, IParser


def test_top_level_module_reexports_interfaces() -> None:
    assert bioetl.BaseApiClient is BaseApiClient
    assert bioetl.IParser is IParser
    assert bioetl.INormalizer is INormalizer
