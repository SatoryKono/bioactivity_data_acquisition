"""Utilities for parsing PubMed E-utilities responses."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any
from xml.etree import ElementTree as ET

__all__ = [
    "parse_esearch_response",
    "parse_efetch_response",
]


def parse_esearch_response(payload: Mapping[str, Any] | None) -> tuple[str | None, str | None, int]:
    """Extract pagination tokens from an ``esearch`` response.

    Parameters
    ----------
    payload:
        JSON payload returned by ``esearch.fcgi``.

    Returns
    -------
    tuple
        ``(webenv, query_key, count)`` where ``count`` defaults to ``0`` when the
        response does not expose the total number of records.
    """

    if not isinstance(payload, Mapping):
        return None, None, 0

    result = payload.get("esearchresult")
    if isinstance(result, Mapping):
        data = result
    else:
        data = payload

    def _first(*keys: str) -> Any:
        for key in keys:
            value = data.get(key)
            if value is None:
                continue
            if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
                value_list = list(value)
                if value_list:
                    return value_list[0]
                continue
            return value
        return None

    web_env = _first("webenv", "WebEnv")
    query_key = _first("querykey", "QueryKey")
    raw_count = _first("count", "Count", "recordsfound")

    try:
        count = int(raw_count) if raw_count is not None else 0
    except (TypeError, ValueError):  # pragma: no cover - defensive conversion
        count = 0

    web_env_str = str(web_env) if web_env not in (None, "") else None
    query_key_str = str(query_key) if query_key not in (None, "") else None

    return web_env_str, query_key_str, max(count, 0)


def parse_efetch_response(payload: str | bytes | None) -> list[dict[str, Any]]:
    """Parse an ``efetch`` XML response into dictionaries.

    The resulting dictionaries capture the bibliographic fields required by the
    downstream normalization pipeline while keeping the raw structure simple.
    """

    if payload is None:
        return []

    if isinstance(payload, bytes):
        xml_content = payload.decode("utf-8", errors="ignore")
    else:
        xml_content = str(payload)

    xml_content = xml_content.strip()
    if not xml_content:
        return []

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return []

    records: list[dict[str, Any]] = []

    for article in root.findall(".//PubmedArticle"):
        parsed = _parse_article(article)
        if parsed:
            records.append(parsed)

    return records


def _parse_article(article: ET.Element) -> dict[str, Any]:
    """Transform a ``PubmedArticle`` node into a dictionary."""

    record: dict[str, Any] = {}

    pmid = article.findtext(".//MedlineCitation/PMID")
    if pmid:
        try:
            record["pmid"] = int(pmid)
        except ValueError:
            record["pmid"] = pmid

    title = article.findtext(".//Article/ArticleTitle")
    if title:
        record["title"] = title

    abstract_parts: list[str] = []
    for abstract_node in article.findall(".//Abstract/AbstractText"):
        text = abstract_node.text or ""
        label = abstract_node.get("Label", "").strip()
        if label:
            abstract_parts.append(f"{label}: {text}".strip())
        else:
            abstract_parts.append(text.strip())
    abstract = " ".join(part for part in abstract_parts if part)
    if abstract:
        record["abstract"] = abstract

    journal = article.findtext(".//Journal/Title")
    if journal:
        record["journal"] = journal

    journal_abbrev = article.findtext(".//MedlineJournalInfo/MedlineTA")
    if journal_abbrev:
        record["journal_abbrev"] = journal_abbrev

    issn_values: list[str] = []
    for issn_node in article.findall(".//Journal/ISSN"):
        value = (issn_node.text or "").strip()
        if value:
            issn_values.append(value)
    if issn_values:
        record["issn"] = issn_values

    volume = article.findtext(".//JournalIssue/Volume")
    if volume:
        record["volume"] = volume

    issue = article.findtext(".//JournalIssue/Issue")
    if issue:
        record["issue"] = issue

    pages = article.findtext(".//Pagination/MedlinePgn")
    if pages:
        if "-" in pages:
            first, last = pages.split("-", 1)
            record["first_page"] = first
            record["last_page"] = last
        else:
            record["first_page"] = pages

    pub_date = article.find(".//Article/Journal/JournalIssue/PubDate")
    if pub_date is not None:
        year_text = pub_date.findtext("Year")
        month_text = pub_date.findtext("Month")
        day_text = pub_date.findtext("Day")

        if year_text:
            try:
                record["year"] = int(year_text)
            except ValueError:
                pass

        if month_text:
            record["month"] = _month_to_int(month_text)

        if day_text:
            try:
                record["day"] = int(day_text)
            except ValueError:
                pass

    doi_value = None
    for article_id in article.findall(".//PubmedData/ArticleIdList/ArticleId"):
        if article_id.get("IdType") == "doi" and article_id.text:
            doi_value = article_id.text.strip()
            break
    if not doi_value:
        for eid in article.findall(".//Article/ELocationID"):
            if eid.get("EIdType") == "doi" and eid.text:
                doi_value = eid.text.strip()
                break
    if doi_value:
        record["doi"] = doi_value

    authors: list[dict[str, str]] = []
    for author in article.findall(".//AuthorList/Author"):
        last = (author.findtext("LastName") or "").strip()
        fore = (author.findtext("ForeName") or "").strip()
        if not last and not fore:
            continue
        author_record: dict[str, str] = {}
        if last:
            author_record["last_name"] = last
        if fore:
            author_record["fore_name"] = fore
        authors.append(author_record)
    if authors:
        record["authors"] = authors

    mesh_terms: list[str] = []
    for heading in article.findall(".//MeshHeading"):
        descriptor = heading.findtext("DescriptorName")
        if descriptor:
            mesh_terms.append(descriptor.strip())
    if mesh_terms:
        record["mesh_terms"] = [term for term in mesh_terms if term]

    chemicals: list[str] = []
    for chem in article.findall(".//ChemicalList/Chemical/NameOfSubstance"):
        if chem.text:
            chemicals.append(chem.text.strip())
    if chemicals:
        record["chemicals"] = [chem for chem in chemicals if chem]

    return record


def _month_to_int(month: str) -> int:
    """Convert month names or abbreviations to an integer (1-12)."""

    month_normalized = month.strip().lower()
    if not month_normalized:
        return 1

    month_map = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "sept": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }

    key = month_normalized[:3]
    return month_map.get(key, 1)
