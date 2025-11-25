from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

import pandas as pd

from bioetl.core.pipeline.validation.interfaces import DQIssue, DQRuleABC


@dataclass
class MissingRateRule(DQRuleABC):
    """Проверяет долю пропусков по заданным столбцам."""

    columns: Sequence[str]
    max_missing_rate: float = 0.0
    severity: str = "warning"

    def name(self) -> str:
        return "missing_rate"

    def evaluate(self, data: Sequence[Mapping[str, object]]) -> Iterable[DQIssue]:
        df = pd.DataFrame(list(data))
        if df.empty:
            return []
        issues: list[DQIssue] = []
        for column in self.columns:
            if column not in df.columns:
                issues.append(
                    DQIssue(
                        rule_name=self.name(),
                        severity="error",
                        message=f"Column '{column}' absent for missing rate check",
                        context={"column": column},
                    )
                )
                continue
            missing_rate = df[column].isna().mean()
            if missing_rate > self.max_missing_rate:
                issues.append(
                    DQIssue(
                        rule_name=self.name(),
                        severity=self.severity,
                        message=f"Missing rate {missing_rate:.2%} exceeds {self.max_missing_rate:.2%}",
                        affected_rows=int(df[column].isna().sum()),
                        context={"column": column, "missing_rate": missing_rate},
                    )
                )
        return issues


@dataclass
class DuplicateRowsRule(DQRuleABC):
    """Правило для поиска дубликатов по набору столбцов."""

    subset: Sequence[str]
    severity: str = "error"

    def name(self) -> str:
        return "duplicate_rows"

    def evaluate(self, data: Sequence[Mapping[str, object]]) -> Iterable[DQIssue]:
        df = pd.DataFrame(list(data))
        if df.empty or not self.subset:
            return []
        duplicated = df[df.duplicated(subset=list(self.subset), keep=False)]
        if duplicated.empty:
            return []
        return [
            DQIssue(
                rule_name=self.name(),
                severity=self.severity,
                message="Found duplicate rows",
                affected_rows=int(duplicated.shape[0]),
                context={"subset": list(self.subset)},
            )
        ]
