"""Общие типы клиента."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

Record = MutableMapping[str, Any]
Headers = Mapping[str, str]
QueryParams = Mapping[str, Any]
JsonData = Mapping[str, Any]

__all__ = ["Record", "Headers", "QueryParams", "JsonData"]
