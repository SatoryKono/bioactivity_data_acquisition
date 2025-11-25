from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class RunResult:
    success: bool
    error: str | None
    metrics: dict[str, Any]
