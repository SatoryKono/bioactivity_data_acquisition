from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Mapping, Optional


@dataclass
class StageMetrics:
    """Минимальный набор метрик по стадии."""

    stage_name: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    rows_read: Optional[int] = None
    rows_written: Optional[int] = None
    extra: Mapping[str, object] = field(default_factory=dict)


@dataclass
class RunResult:
    """Результат выполнения пайплайна."""

    run_id: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    stage_metrics: Iterable[StageMetrics] = field(default_factory=tuple)
    total_rows_read: Optional[int] = None
    total_rows_written: Optional[int] = None


@dataclass
class WriteResult:
    """Результат вывода данных."""

    run_id: str
    output_uri: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    rows_written: Optional[int] = None
    metadata: Mapping[str, object] = field(default_factory=dict)
