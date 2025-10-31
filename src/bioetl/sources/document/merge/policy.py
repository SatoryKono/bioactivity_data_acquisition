"""Merge policy definitions consolidating enriched document attributes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterable, Sequence

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.sources.crossref.merge import merge_crossref_with_base
from bioetl.sources.openalex.merge import merge_openalex_with_base
from bioetl.sources.pubmed.merge import merge_pubmed_with_base
from bioetl.sources.semantic_scholar.merge import merge_semantic_scholar_with_base

logger = UnifiedLogger.get(__name__)


__all__ = [
    "IDENTIFIER_KEYS",
    "MergeStrategy",
    "MergeCandidate",
    "MergeRule",
    "MergePolicy",
    "FIELD_PRECEDENCE",
    "CASTERS",
    "DOCUMENT_POLICY",
    "merge_with_precedence",
    "detect_conflicts",
]


class MergeStrategy(str, Enum):
    """Supported conflict resolution strategies for merge rules."""

    PREFER_SOURCE = "prefer_source"
    PREFER_FRESH = "prefer_fresh"
    CONCAT_UNIQUE = "concat_unique"
    SCORE_BASED = "score_based"


@dataclass(frozen=True)
class MergeCandidate:
    """Configuration describing a source column participating in a merge rule."""

    source: str
    column: str
    freshness_column: str | None = None
    score_column: str | None = None


@dataclass(frozen=True)
class MergeRule:
    """Declarative rule describing how to consolidate a single output field."""

    field: str
    candidates: Sequence[MergeCandidate]
    strategy: MergeStrategy = MergeStrategy.PREFER_SOURCE
    source_column: str | None = None
    extras_column: str | None = None
    caster: Callable[[pd.Series], pd.Series] | None = None
    joiner: str = "; "

    def __post_init__(self) -> None:  # pragma: no cover - defensive guard
        if not self.candidates:
            raise ValueError(f"MergeRule('{self.field}') requires at least one candidate")

    @property
    def resolved_source_column(self) -> str:
        """Return the column name used to record the winning source."""

        return self.source_column or f"{self.field}_source"

    @property
    def resolved_extras_column(self) -> str | None:
        """Return the column storing conflict details when configured."""

        if self.extras_column is False:  # pragma: no cover - compatibility guard
            return None
        return self.extras_column or f"{self.field}_extras"


IDENTIFIER_KEYS: tuple[str, ...] = (
    "doi",
    "pmid",
    "cid",
    "sid",
    "uniprot_id",
    "molecule_chembl_id",
    "assay_chembl_id",
    "target_chembl_id",
)


def _is_missing(value: object) -> bool:
    """Return ``True`` when ``value`` should be treated as null for merging."""

    if value is None:
        return True
    if value is pd.NA:  # type: ignore[comparison-overlap]
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:  # pragma: no cover - defensive guard for exotic objects
        pass
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _normalise_container(value: object) -> Sequence[str] | None:
    """Normalise list-like containers into a deterministic sequence of strings."""

    if isinstance(value, (list, tuple, set)):
        result: list[str] = []
        for item in value:
            if _is_missing(item):
                continue
            normalised = str(item).strip()
            if not normalised:
                continue
            result.append(normalised)
        if result:
            return tuple(result)
        return None
    return None


def _normalise_value(value: object) -> object | None:
    """Normalise raw candidate values into merge-friendly representations."""

    if _is_missing(value):
        return None
    container = _normalise_container(value)
    if container is not None:
        return container
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def _parse_timestamp(value: object) -> pd.Timestamp | None:
    """Attempt to coerce ``value`` into a timezone-aware ``Timestamp``."""

    if _is_missing(value):
        return None
    try:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
    except Exception:  # pragma: no cover - defensive guard
        return None
    if isinstance(parsed, pd.Timestamp) and not pd.isna(parsed):
        return parsed
    return None


def _coerce_score(value: object) -> float | None:
    """Coerce arbitrary objects to a floating point score when possible."""

    if _is_missing(value):
        return None
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(score):
        return None
    return score


class MergePolicy:
    """Apply declarative merge rules to consolidated enrichment datasets."""

    def __init__(self, rules: Iterable[MergeRule]):
        self._rules: list[MergeRule] = list(rules)
        self._rules_by_field = {rule.field: rule for rule in self._rules}

    @property
    def rules(self) -> Sequence[MergeRule]:
        return tuple(self._rules)

    def get_rule(self, field: str) -> MergeRule | None:
        return self._rules_by_field.get(field)

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the configured rules to ``df`` returning an enriched copy."""

        working = df.copy()

        if working.empty:
            for rule in self._rules:
                if rule.field not in working.columns:
                    working[rule.field] = pd.Series(dtype="object")
                source_column = rule.resolved_source_column
                if source_column not in working.columns:
                    working[source_column] = pd.Series(dtype="object")
                extras_column = rule.resolved_extras_column
                if extras_column and extras_column not in working.columns:
                    working[extras_column] = pd.Series(dtype="object")
            return working

        for rule in self._rules:
            values: list[object] = []
            sources: list[object] = []
            extras_list: list[object | None] = []

            for index, row in working.iterrows():
                value, source, extras = self._resolve_row(rule, row)
                values.append(value)
                sources.append(source)
                extras_list.append(extras)

            value_series = pd.Series(values, index=working.index, dtype="object")
            source_series = pd.Series(sources, index=working.index, dtype="object").astype("string")

            if rule.caster is not None:
                try:
                    value_series = rule.caster(value_series)
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.error("merge_policy_caster_failed", field=rule.field, error=str(exc))
                    raise

            working[rule.field] = value_series
            working[rule.resolved_source_column] = source_series

            extras_column = rule.resolved_extras_column
            if extras_column:
                if any(extra for extra in extras_list):
                    serialised = [self._serialise_extras(item) for item in extras_list]
                    working[extras_column] = pd.Series(serialised, index=working.index, dtype="object")
                elif extras_column in working.columns:
                    working = working.drop(columns=[extras_column])

        return working

    def _resolve_row(
        self,
        rule: MergeRule,
        row: pd.Series,
    ) -> tuple[object, object, list[dict[str, object]] | None]:
        candidates: list[dict[str, object]] = []
        for position, candidate in enumerate(rule.candidates):
            raw_value = row.get(candidate.column, pd.NA)
            normalised_value = _normalise_value(raw_value)
            if normalised_value is None:
                continue
            entry: dict[str, object] = {
                "index": position,
                "source": candidate.source,
                "raw_column": candidate.column,
                "value": normalised_value,
            }
            if candidate.freshness_column:
                entry["freshness"] = _parse_timestamp(row.get(candidate.freshness_column, pd.NA))
            if candidate.score_column:
                entry["score"] = _coerce_score(row.get(candidate.score_column, pd.NA))
            candidates.append(entry)

        if not candidates:
            return pd.NA, pd.NA, None

        if rule.strategy is MergeStrategy.CONCAT_UNIQUE:
            combined_value, primary_source = self._combine_unique(candidates, rule)
            return combined_value, primary_source, None

        if rule.strategy is MergeStrategy.PREFER_FRESH:
            selected = self._select_freshest(candidates)
        elif rule.strategy is MergeStrategy.SCORE_BASED:
            selected = self._select_highest_score(candidates)
        else:
            selected = candidates[0]

        selected_value = self._format_value(selected["value"], rule)
        extras: list[dict[str, object]] = []
        for entry in candidates:
            if entry is selected:
                continue
            formatted = self._format_value(entry["value"], rule)
            if formatted == selected_value:
                continue
            payload: dict[str, object] = {
                "source": entry["source"],
                "column": entry["raw_column"],
                "value": formatted,
            }
            freshness = entry.get("freshness")
            if isinstance(freshness, pd.Timestamp):
                payload["freshness"] = freshness.isoformat()
            score = entry.get("score")
            if isinstance(score, float):
                payload["score"] = score
            extras.append(payload)

        extras_payload = extras or None
        source_value = selected["source"]
        return selected_value, source_value, extras_payload

    def _format_value(self, value: object, rule: MergeRule) -> object:
        if isinstance(value, tuple):
            unique: list[str] = []
            seen: set[str] = set()
            for item in value:
                key = str(item)
                if key not in seen:
                    seen.add(key)
                    unique.append(str(item))
            return rule.joiner.join(unique)
        return value

    def _serialise_extras(self, extras: object | None) -> object:
        if extras is None:
            return pd.NA
        return extras

    @staticmethod
    def _select_freshest(candidates: Sequence[dict[str, object]]) -> dict[str, object]:
        dated = [candidate for candidate in candidates if isinstance(candidate.get("freshness"), pd.Timestamp)]
        if dated:
            return max(
                dated,
                key=lambda item: (
                    item.get("freshness"),
                    -int(item.get("index", 0)),
                ),
            )
        return candidates[0]

    @staticmethod
    def _select_highest_score(candidates: Sequence[dict[str, object]]) -> dict[str, object]:
        scored = [candidate for candidate in candidates if isinstance(candidate.get("score"), float)]
        if scored:
            return max(
                scored,
                key=lambda item: (
                    item.get("score"),
                    -int(item.get("index", 0)),
                ),
            )
        return candidates[0]

    def _combine_unique(
        self,
        candidates: Sequence[dict[str, object]],
        rule: MergeRule,
    ) -> tuple[object, object]:
        combined: list[str] = []
        seen: set[str] = set()
        primary_source: object = pd.NA

        for entry in candidates:
            value = entry["value"]
            values = value if isinstance(value, tuple) else (value,)
            has_contribution = False
            for item in values:
                token = str(item).strip()
                if not token:
                    continue
                if token not in seen:
                    seen.add(token)
                    combined.append(token)
                    if primary_source is pd.NA:
                        primary_source = entry["source"]
                    has_contribution = True
            if not has_contribution and primary_source is pd.NA and not _is_missing(entry["value"]):
                primary_source = entry["source"]

        if not combined:
            return pd.NA, primary_source

        formatted = rule.joiner.join(combined)
        return formatted, primary_source


def _int_nullable(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _bool_nullable(series: pd.Series) -> pd.Series:
    return series.astype("boolean")


DOCUMENT_RULES: tuple[MergeRule, ...] = (
    MergeRule(
        field="doi_clean",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("crossref", "crossref_doi"),
            MergeCandidate("pubmed", "pubmed_doi"),
            MergeCandidate("openalex", "openalex_doi"),
            MergeCandidate("semantic_scholar", "semantic_scholar_doi"),
            MergeCandidate("chembl", "chembl_doi"),
        ),
    ),
    MergeRule(
        field="pmid",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_pmid"),
            MergeCandidate("chembl", "chembl_pmid"),
            MergeCandidate("openalex", "openalex_pmid"),
            MergeCandidate("semantic_scholar", "semantic_scholar_pmid"),
        ),
        caster=_int_nullable,
    ),
    MergeRule(
        field="title",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_article_title"),
            MergeCandidate("chembl", "chembl_title"),
            MergeCandidate("openalex", "openalex_title"),
            MergeCandidate("crossref", "crossref_title"),
            MergeCandidate("semantic_scholar", "semantic_scholar_title"),
        ),
    ),
    MergeRule(
        field="abstract",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_abstract"),
            MergeCandidate("chembl", "chembl_abstract"),
            MergeCandidate("semantic_scholar", "semantic_scholar_abstract"),
        ),
    ),
    MergeRule(
        field="journal",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_journal"),
            MergeCandidate("crossref", "crossref_journal"),
            MergeCandidate("openalex", "openalex_journal"),
            MergeCandidate("chembl", "chembl_journal"),
            MergeCandidate("semantic_scholar", "semantic_scholar_journal"),
        ),
    ),
    MergeRule(
        field="journal_abbrev",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_journal_abbrev"),
            MergeCandidate("chembl", "chembl_journal_abbrev"),
        ),
    ),
    MergeRule(
        field="authors",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_authors"),
            MergeCandidate("crossref", "crossref_authors"),
            MergeCandidate("openalex", "openalex_authors"),
            MergeCandidate("chembl", "chembl_authors"),
            MergeCandidate("semantic_scholar", "semantic_scholar_authors"),
        ),
    ),
    MergeRule(
        field="year",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_year"),
            MergeCandidate("crossref", "crossref_year"),
            MergeCandidate("openalex", "openalex_year"),
            MergeCandidate("semantic_scholar", "semantic_scholar_year"),
            MergeCandidate("chembl", "chembl_year"),
        ),
        caster=_int_nullable,
    ),
    MergeRule(
        field="volume",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_volume"),
            MergeCandidate("crossref", "crossref_volume"),
            MergeCandidate("chembl", "chembl_volume"),
        ),
    ),
    MergeRule(
        field="issue",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_issue"),
            MergeCandidate("crossref", "crossref_issue"),
            MergeCandidate("chembl", "chembl_issue"),
        ),
    ),
    MergeRule(
        field="first_page",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_first_page"),
            MergeCandidate("crossref", "crossref_first_page"),
        ),
    ),
    MergeRule(
        field="last_page",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_last_page"),
            MergeCandidate("crossref", "crossref_last_page"),
        ),
    ),
    MergeRule(
        field="issn_print",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("crossref", "crossref_issn_print"),
            MergeCandidate("pubmed", "pubmed_issn_print"),
            MergeCandidate("openalex", "openalex_issn"),
            MergeCandidate("semantic_scholar", "semantic_scholar_issn"),
        ),
    ),
    MergeRule(
        field="issn_electronic",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("crossref", "crossref_issn_electronic"),
            MergeCandidate("pubmed", "pubmed_issn_electronic"),
            MergeCandidate("openalex", "openalex_issn"),
            MergeCandidate("semantic_scholar", "semantic_scholar_issn"),
        ),
    ),
    MergeRule(
        field="is_oa",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("openalex", "openalex_is_oa"),
            MergeCandidate("semantic_scholar", "semantic_scholar_is_oa"),
        ),
        caster=_bool_nullable,
    ),
    MergeRule(
        field="oa_status",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("openalex", "openalex_oa_status"),
            MergeCandidate("semantic_scholar", "semantic_scholar_oa_status"),
        ),
    ),
    MergeRule(
        field="oa_url",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("openalex", "openalex_oa_url"),
            MergeCandidate("semantic_scholar", "semantic_scholar_oa_url"),
        ),
    ),
    MergeRule(
        field="citation_count",
        strategy=MergeStrategy.SCORE_BASED,
        candidates=(
            MergeCandidate("semantic_scholar", "semantic_scholar_citation_count", score_column="semantic_scholar_citation_count"),
            MergeCandidate("openalex", "openalex_citation_count", score_column="openalex_citation_count"),
        ),
        caster=_int_nullable,
    ),
    MergeRule(
        field="influential_citations",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("semantic_scholar", "semantic_scholar_influential_citations"),
        ),
        caster=_int_nullable,
    ),
    MergeRule(
        field="fields_of_study",
        strategy=MergeStrategy.CONCAT_UNIQUE,
        candidates=(
            MergeCandidate("semantic_scholar", "semantic_scholar_fields_of_study"),
        ),
    ),
    MergeRule(
        field="concepts_top3",
        strategy=MergeStrategy.CONCAT_UNIQUE,
        candidates=(
            MergeCandidate("openalex", "openalex_concepts_top3"),
        ),
    ),
    MergeRule(
        field="mesh_terms",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_mesh_descriptors"),
        ),
    ),
    MergeRule(
        field="chemicals",
        strategy=MergeStrategy.PREFER_SOURCE,
        candidates=(
            MergeCandidate("pubmed", "pubmed_chemical_list"),
        ),
    ),
)


DOCUMENT_POLICY = MergePolicy(DOCUMENT_RULES)

FIELD_PRECEDENCE: dict[str, list[tuple[str, str]]] = {
    rule.field: [(candidate.source, candidate.column) for candidate in rule.candidates]
    for rule in DOCUMENT_RULES
}

CASTERS: dict[str, Callable[[pd.Series], pd.Series]] = {
    rule.field: rule.caster
    for rule in DOCUMENT_RULES
    if rule.caster is not None
}


def merge_with_precedence(
    chembl_df: pd.DataFrame,
    pubmed_df: pd.DataFrame | None = None,
    crossref_df: pd.DataFrame | None = None,
    openalex_df: pd.DataFrame | None = None,
    semantic_scholar_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Merge document payloads and resolve conflicts using :class:`MergePolicy`."""

    merged_df = chembl_df.copy()

    if pubmed_df is not None and not pubmed_df.empty:
        merged_df = merge_pubmed_with_base(
            merged_df,
            pubmed_df,
            base_pmid_column="chembl_pmid",
            base_doi_column="chembl_doi",
            conflict_detection=False,
        )

    if crossref_df is not None and not crossref_df.empty:
        merged_df = merge_crossref_with_base(
            merged_df,
            crossref_df,
            base_doi_column="chembl_doi",
        )

    if openalex_df is not None and not openalex_df.empty:
        merged_df = merge_openalex_with_base(
            merged_df,
            openalex_df,
            base_doi_column="chembl_doi",
            base_pmid_column="chembl_pmid",
            conflict_detection=False,
        )

    if semantic_scholar_df is not None and not semantic_scholar_df.empty:
        title_column = None
        if "_original_title" in merged_df.columns:
            title_column = "_original_title"
        elif "chembl_title" in merged_df.columns:
            title_column = "chembl_title"

        merged_df = merge_semantic_scholar_with_base(
            merged_df,
            semantic_scholar_df,
            base_title_column=title_column,
        )

    merged_df = DOCUMENT_POLICY.apply(merged_df)

    if merged_df.empty:
        merged_df["conflict_doi"] = pd.Series(dtype="boolean")
        merged_df["conflict_pmid"] = pd.Series(dtype="boolean")
        return merged_df

    merged_df = merged_df.apply(detect_conflicts, axis=1)
    merged_df["conflict_doi"] = merged_df["conflict_doi"].astype("boolean")
    merged_df["conflict_pmid"] = merged_df["conflict_pmid"].astype("boolean")

    return merged_df


def detect_conflicts(row: pd.Series) -> pd.Series:
    """Detect DOI/PMID conflicts between sources for observability metrics."""

    doi_columns = [
        col
        for col in [
            "chembl_doi",
            "pubmed_doi",
            "crossref_doi",
            "openalex_doi",
            "semantic_scholar_doi",
        ]
        if col in row.index
    ]
    doi_values = {
        str(row[col]).strip()
        for col in doi_columns
        if pd.notna(row[col]) and str(row[col]).strip()
    }
    row["conflict_doi"] = len(doi_values) > 1

    pmid_columns = [
        col
        for col in [
            "chembl_pmid",
            "pubmed_pmid",
            "openalex_pmid",
            "semantic_scholar_pmid",
        ]
        if col in row.index
    ]
    pmid_values: set[str] = set()
    for col in pmid_columns:
        value = row[col]
        if pd.isna(value):
            continue
        try:
            pmid_int = int(str(value).strip())
        except (TypeError, ValueError):
            continue
        if pmid_int == 0:
            continue
        pmid_values.add(str(pmid_int))
    row["conflict_pmid"] = len(pmid_values) > 1
    return row

