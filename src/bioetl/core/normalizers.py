"""Common normalization helpers shared across pipelines."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from re import Pattern

import pandas as pd

__all__ = [
    "IdentifierRule",
    "IdentifierStats",
    "StringNormalizationConfig",
    "StringRule",
    "StringStats",
    "normalize_identifier_columns",
    "normalize_string_columns",
    "normalize_string_columns_with_config",
]


@dataclass(frozen=True)
class IdentifierRule:
    """Configuration describing how to normalize identifier columns."""

    columns: Sequence[str]
    pattern: str | Pattern[str]
    uppercase: bool = True
    strip: bool = True
    empty_to_null: bool = True
    name: str | None = None


@dataclass
class IdentifierStats:
    """Aggregate metrics produced by identifier normalization."""

    normalized: int = 0
    invalid: int = 0
    per_column: dict[str, dict[str, int]] = field(default_factory=dict)

    def add(self, column: str, normalized_count: int, invalid_count: int) -> None:
        if normalized_count == 0 and invalid_count == 0:
            return
        self.per_column[column] = {
            "normalized": normalized_count,
            "invalid": invalid_count,
        }
        self.normalized += normalized_count
        self.invalid += invalid_count

    @property
    def has_changes(self) -> bool:
        return bool(self.per_column)


@dataclass(frozen=True)
class StringRule:
    """Configuration describing how to normalize textual columns."""

    trim: bool = True
    empty_to_null: bool = True
    title_case: bool = False
    uppercase: bool = False
    lowercase: bool = False
    max_length: int | None = None
    collapse_whitespace: bool = False


@dataclass(frozen=True)
class StringNormalizationConfig:
    """Declarative configuration for string normalization."""

    columns: Sequence[str] = field(default_factory=tuple)
    default_rule: StringRule = field(default_factory=StringRule)
    overrides: Mapping[str, StringRule] = field(default_factory=dict)


@dataclass
class StringStats:
    """Aggregate metrics produced by string normalization."""

    per_column: dict[str, int] = field(default_factory=dict)

    def add(self, column: str, processed_count: int) -> None:
        if processed_count == 0:
            return
        self.per_column[column] = processed_count

    @property
    def has_changes(self) -> bool:
        return bool(self.per_column)

    @property
    def processed(self) -> int:
        return int(sum(self.per_column.values()))


def _compile_pattern(pattern: str | Pattern[str]) -> Pattern[str]:
    return re.compile(pattern) if isinstance(pattern, str) else pattern


def normalize_identifier_columns(
    df: pd.DataFrame,
    rules: Iterable[IdentifierRule],
    *,
    copy: bool = True,
) -> tuple[pd.DataFrame, IdentifierStats]:
    """Normalize identifier columns according to ``rules``.

    Returns a new DataFrame (unless ``copy`` is False) and aggregated metrics.
    """

    result = df.copy() if copy else df
    stats = IdentifierStats()

    for rule in rules:
        compiled = _compile_pattern(rule.pattern)

        for column in rule.columns:
            if column not in result.columns:
                continue

            series = result[column]
            mask = series.notna()
            if not mask.any():
                continue

            normalized_series = series.loc[mask].astype(str)
            if rule.uppercase:
                normalized_series = normalized_series.str.upper()
            if rule.strip:
                normalized_series = normalized_series.str.strip()

            result.loc[mask, column] = normalized_series

            valid_mask = normalized_series.str.match(compiled, na=False)
            invalid_mask = mask.copy()
            invalid_mask.loc[mask] = ~valid_mask

            invalid_count = int(invalid_mask.sum())
            if invalid_count > 0 and rule.empty_to_null:
                result.loc[invalid_mask, column] = pd.NA

            normalized_count = int(mask.sum()) - invalid_count
            stats.add(column, normalized_count, invalid_count)

    return result, stats


def normalize_string_columns(
    df: pd.DataFrame,
    rules: Mapping[str, StringRule],
    *,
    copy: bool = True,
) -> tuple[pd.DataFrame, StringStats]:
    """Normalize textual columns based on per-column ``rules``."""

    result = df.copy() if copy else df
    stats = StringStats()

    for column, rule in rules.items():
        if column not in result.columns:
            continue

        series = result[column]
        mask = series.notna()
        if not mask.any():
            continue

        normalized_series = series.loc[mask].astype(str)

        if rule.trim:
            normalized_series = normalized_series.str.strip()
        if rule.collapse_whitespace:
            normalized_series = normalized_series.str.replace(r"\s+", " ", regex=True)
        if rule.uppercase:
            normalized_series = normalized_series.str.upper()
        if rule.lowercase:
            normalized_series = normalized_series.str.lower()
        if rule.title_case:
            normalized_series = normalized_series.str.title()
        if rule.max_length is not None:
            normalized_series = normalized_series.str.slice(stop=rule.max_length)

        result.loc[mask, column] = normalized_series

        if rule.empty_to_null:
            result[column] = result[column].replace("", pd.NA)

        processed_count = int(result.loc[mask, column].notna().sum())
        stats.add(column, processed_count)

    return result, stats


def _resolve_string_rules(config: StringNormalizationConfig) -> dict[str, StringRule]:
    rules: dict[str, StringRule] = {}

    for column in config.columns:
        rules[column] = config.overrides.get(column, config.default_rule)

    for column, rule in config.overrides.items():
        rules[column] = rule

    return rules


def normalize_string_columns_with_config(
    df: pd.DataFrame,
    config: StringNormalizationConfig,
    *,
    copy: bool = True,
) -> tuple[pd.DataFrame, StringStats]:
    """Normalize textual columns based on a configuration object."""

    rules = _resolve_string_rules(config)
    if not rules:
        return (df.copy() if copy else df), StringStats()

    return normalize_string_columns(df, rules, copy=copy)
