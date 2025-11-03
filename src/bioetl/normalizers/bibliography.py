"""Helpers for normalizing bibliographic metadata."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any, TypedDict, cast


class NormalizedBibliography(TypedDict, total=False):
    """Typed mapping for normalized bibliographic fields."""

    doi_clean: str
    title: str
    journal: str
    authors: str

from bioetl.normalizers.registry import registry

_DOI_URL_PATTERN = re.compile(r"^(?:https?://)?(?:dx\.)?doi\.org/", flags=re.IGNORECASE)
_DOI_LABEL_PATTERN = re.compile(r"^doi[:\s]+", flags=re.IGNORECASE)


def _first_non_empty_string(value: Any) -> str | None:
    """Extract the first non-empty string from value."""

    if isinstance(value, str):
        candidate = value.strip()
        return candidate or None

    if isinstance(value, Mapping):
        return None

    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray)):
        for item in value:
            candidate = _first_non_empty_string(item)
            if candidate:
                return candidate

    return None


def normalize_doi(value: Any) -> str | None:
    """Normalize DOI value using the identifier normalizer registry entry."""

    candidate = _first_non_empty_string(value)
    if not candidate:
        return None

    candidate = _DOI_URL_PATTERN.sub("", candidate)
    candidate = _DOI_LABEL_PATTERN.sub("", candidate).strip()
    if not candidate:
        return None

    normalized = registry.normalize("identifier", candidate)
    if normalized is None:
        return None
    return cast(str, normalized)


def normalize_title(value: Any) -> str | None:
    """Normalize bibliographic title values."""

    candidate = _first_non_empty_string(value)
    if not candidate:
        return None

    normalized = registry.normalize("string", candidate)
    if normalized is None:
        return None
    return cast(str, normalized)


def _normalize_author_mapping(author: Mapping[str, Any]) -> str | None:
    """Normalize a single author mapping to a display string."""

    nested = author.get("author")
    if isinstance(nested, Mapping):
        return _normalize_author_mapping(nested)

    for key in ("display_name", "displayName", "name"):
        value = author.get(key)
        if isinstance(value, str):
            normalized_value = registry.normalize("string", value)
            if normalized_value is None:
                return None
            return cast(str, normalized_value)

    family = author.get("family") or author.get("last") or author.get("last_name") or author.get("lastName")
    given = author.get("given") or author.get("first") or author.get("first_name") or author.get("firstName")

    parts = []
    if family:
        parts.append(str(family))
    if given:
        if parts:
            parts.append(str(given))
        else:
            parts.append(str(given))

    if not parts:
        return None

    if len(parts) == 2:
        name = f"{parts[0]}, {parts[1]}"
    else:
        name = parts[0]

    normalized_name = registry.normalize("string", name)
    if normalized_name is None:
        return None
    return cast(str, normalized_name)


def normalize_authors(value: Any) -> str | None:
    """Normalize authors information to a semicolon-delimited string."""

    if value is None:
        return None

    if isinstance(value, str):
        normalized_value = registry.normalize("string", value)
        if normalized_value is None:
            return None
        return cast(str, normalized_value)

    entries: Iterable[Any]
    if isinstance(value, Mapping):
        entries = [value]
    elif isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, str)):
        entries = value
    else:
        return None

    names: list[str] = []
    for entry in entries:
        normalized_name: str | None = None

        if isinstance(entry, str):
            raw_normalized = registry.normalize("string", entry)
            if raw_normalized is not None:
                normalized_name = cast(str, raw_normalized)
            else:
                normalized_name = None
        elif isinstance(entry, Mapping):
            normalized_name = _normalize_author_mapping(entry)

        if normalized_name:
            names.append(normalized_name)

    if not names:
        return None

    joined = "; ".join(names)
    normalized_joined = registry.normalize("string", joined)
    if normalized_joined is None:
        return None
    return cast(str, normalized_joined)


FieldExtractor = (
    str
    | Callable[[Mapping[str, Any]], Any]
    | Iterable[str | Callable[[Mapping[str, Any]], Any]]
)


def _resolve_field(record: Mapping[str, Any], spec: FieldExtractor | None) -> Any:
    """Resolve *spec* against *record* and return the resulting value."""

    if spec is None:
        return None

    if callable(spec):
        return spec(record)

    if isinstance(spec, str):
        return record.get(spec)

    if isinstance(spec, Iterable):
        for candidate in spec:
            value = _resolve_field(record, candidate)
            if value is not None:
                return value

    return None


def normalize_common_bibliography(
    record: Mapping[str, Any],
    *,
    doi: FieldExtractor | None = ("doi", "DOI"),
    title: FieldExtractor | None = ("title",),
    journal: FieldExtractor | None = ("journal", "container-title", "venue"),
    authors: FieldExtractor | None = ("authors", "author"),
    doi_normalizer: Callable[[Any], str | None] = normalize_doi,
    title_normalizer: Callable[[Any], str | None] = normalize_title,
    journal_normalizer: Callable[[Any], str | None] = normalize_title,
    authors_normalizer: Callable[[Any], str | None] = normalize_authors,
) -> NormalizedBibliography:
    """Normalize common bibliographic fields for *record*.

    Parameters
    ----------
    record:
        The source mapping containing bibliographic metadata.
    doi, title, journal, authors:
        Field selectors that specify how to extract the raw value from
        *record*. Selectors may be:

        * A direct key name (``str``)
        * A callable accepting the full record and returning a value
        * An iterable of either of the above which is processed in order

    ``*_normalizer``
        Callbacks used to normalize the extracted values.
    """

    if not isinstance(record, Mapping):
        return {}

    normalized: NormalizedBibliography = {}

    raw_doi = _resolve_field(record, doi)
    doi_clean = doi_normalizer(raw_doi)
    if doi_clean:
        normalized["doi_clean"] = doi_clean

    raw_title = _resolve_field(record, title)
    title_clean = title_normalizer(raw_title)
    if title_clean:
        normalized["title"] = title_clean

    raw_journal = _resolve_field(record, journal)
    journal_clean = journal_normalizer(raw_journal)
    if journal_clean:
        normalized["journal"] = journal_clean

    raw_authors = _resolve_field(record, authors)
    authors_clean = authors_normalizer(raw_authors)
    if authors_clean:
        normalized["authors"] = authors_clean

    return normalized
