from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import pandas as pd
from structlog.stdlib import BoundLogger


@dataclass(frozen=True)
class FlattenSpec:
    """Declarative specification for flattening nested columns."""

    source_column: str
    cols: Sequence[str] | None = None
    prefix: str | None = None
    drop_source: bool = False


class FlattenNestedMixin:
    """Mixin providing configurable flattening for nested object columns."""

    def nested_flatten_specs(self) -> Sequence[FlattenSpec]:
        """Return flattening specifications for nested columns.

        Pipelines should override this to configure the nested columns and
        target fields to extract.
        """

        return ()

    def _flatten_nested_structures(
        self, df: pd.DataFrame, log: BoundLogger
    ) -> pd.DataFrame:
        """Flatten configured nested columns using :class:`FlattenSpec` entries."""

        if df.empty:
            return df

        working = df.copy()
        for spec in self.nested_flatten_specs():
            if spec.source_column not in working.columns:
                continue

            normalized = pd.json_normalize(
                working[spec.source_column].tolist()
            )
            cols = spec.cols or list(normalized.columns)
            for col in cols:
                target_col = (
                    f"{spec.source_column}__{col}"
                    if spec.prefix is None
                    else f"{spec.prefix}{col}"
                )
                working[target_col] = normalized.get(col)

            if spec.drop_source:
                working = working.drop(columns=[spec.source_column])

        log.debug("flatten_completed", columns=list(working.columns))
        return working
