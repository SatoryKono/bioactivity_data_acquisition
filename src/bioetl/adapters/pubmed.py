"""PubMed E-utilities API adapter."""

import re
from typing import Any
from xml.etree import ElementTree as ET

from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.core.api_client import APIConfig
from bioetl.normalizers.bibliography import normalize_common_bibliography
from bioetl.sources.pubmed.request import PubMedRequestBuilder

NORMALIZER_ID, NORMALIZER_STRING = get_bibliography_normalizers()


class PubMedAdapter(ExternalAdapter):
    """Adapter for PubMed E-utilities API.

    :meth:`_fetch_batch` implements the PubMed-specific fetching that is reused
    via the shared batching helper.
    """

    DEFAULT_BATCH_SIZE = 200

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        """Initialize PubMed adapter."""
        super().__init__(api_config, adapter_config)

        self.tool = adapter_config.__dict__.get("tool", "bioactivity_etl")
        self.email = adapter_config.__dict__.get("email", "")
        self.api_key = adapter_config.__dict__.get("api_key", "")

        if not self.email:
            self.logger.warning("pubmed_email_missing", note="PubMed requires 'email' parameter")

        self.request_builder = PubMedRequestBuilder(api_config, adapter_config)
        self.api_client.session.headers.update(self.request_builder.base_headers)

    def _fetch_batch(self, pmids: list[str]) -> list[dict[str, Any]]:
        """Fetch a batch of PMIDs using EFetch directly."""
        request_spec = self.request_builder.efetch(pmids)

        try:
            xml_content = self.api_client.request_text(
                request_spec.url,
                params=request_spec.params,
                method="GET",
                headers=request_spec.headers,
            )
        except Exception as exc:
            self.logger.error(
                "fetch_batch_error",
                error=str(exc),
                pmids=pmids[:3],
                request_id=request_spec.metadata.get("request_id"),
            )
            return []

        if not isinstance(xml_content, str):
            self.logger.warning(
                "unexpected_streaming_payload",
                pmids=pmids[:3],
                payload_type=type(xml_content).__name__,
            )
            xml_content = "".join(str(chunk) for chunk in xml_content)

        return self._parse_xml(xml_content)

    def _parse_xml(self, xml_content: str) -> list[dict[str, Any]]:
        """Parse PubMed XML response."""
        try:
            root = ET.fromstring(xml_content)
            records = []

            # Find all PubmedArticle elements
            for article in root.findall(".//PubmedArticle"):
                record = self._parse_article(article)
                if record:
                    records.append(record)

            return records
        except ET.ParseError as e:
            self.logger.error("xml_parse_error", error=str(e))
            return []

    def _parse_article(self, article: ET.Element) -> dict[str, Any] | None:
        """Parse a single PubMed article element."""
        try:
            record = {}

            # PMID
            pmid_elem = article.find(".//MedlineCitation/PMID")
            if pmid_elem is not None and pmid_elem.text:
                record["pmid"] = pmid_elem.text

            # Article Title
            title_elem = article.find(".//Article/ArticleTitle")
            if title_elem is not None and title_elem.text:
                record["title"] = title_elem.text

            # Abstract - concatenate all AbstractText elements
            abstract_texts = article.findall(".//Abstract/AbstractText")
            if abstract_texts:
                abstract_parts = []
                for abs_elem in abstract_texts:
                    if abs_elem.text:
                        label = abs_elem.get("Label", "")
                        text = abs_elem.text
                        if label:
                            abstract_parts.append(f"{label}: {text}")
                        else:
                            abstract_parts.append(text)
                record["abstract"] = " ".join(abstract_parts)

            # Journal
            journal_elem = article.find(".//Journal/Title")
            if journal_elem is not None and journal_elem.text:
                record["journal"] = journal_elem.text

            # Journal abbreviation
            iso_abbrev = article.find(".//MedlineJournalInfo/MedlineTA")
            if iso_abbrev is not None and iso_abbrev.text:
                record["journal_abbrev"] = iso_abbrev.text

            # ISSN
            issn_list = article.findall(".//Journal/ISSN")
            if issn_list:
                record["issn"] = [elem.text for elem in issn_list if elem.text]

            # Volume, Issue
            volume_elem = article.find(".//JournalIssue/Volume")
            if volume_elem is not None and volume_elem.text:
                record["volume"] = volume_elem.text

            issue_elem = article.find(".//JournalIssue/Issue")
            if issue_elem is not None and issue_elem.text:
                record["issue"] = issue_elem.text

            # Pages
            pages_elem = article.find(".//Pagination/MedlinePgn")
            if pages_elem is not None and pages_elem.text:
                record["pages"] = pages_elem.text

            # Publication date
            pub_date = article.find(".//Article/Journal/JournalIssue/PubDate")
            if pub_date is not None:
                year = pub_date.find("Year")
                month = pub_date.find("Month")
                day = pub_date.find("Day")
                record["year"] = int(year.text) if year is not None and year.text else None
                record["month"] = month.text if month is not None and month.text else None
                record["day"] = day.text if day is not None and day.text else None
                record["pub_date"] = self._normalize_date(
                    record.get("year"), record.get("month"), record.get("day")
                )

            # DOI - priority: ArticleIdList > ELocationID
            doi = None
            # Try ArticleIdList first
            article_ids = article.findall(".//PubmedData/ArticleIdList/ArticleId")
            for article_id in article_ids:
                if article_id.get("IdType") == "doi":
                    doi = article_id.text
                    break

            # Fallback to ELocationID
            if not doi:
                eids = article.findall(".//Article/ELocationID")
                for eid in eids:
                    if eid.get("EIdType") == "doi":
                        doi = eid.text
                        break

            if doi:
                record["doi"] = doi

            # Authors
            authors_list = article.findall(".//AuthorList/Author")
            if authors_list:
                authors = []
                for author in authors_list:
                    last_name = author.find("LastName")
                    fore_name = author.find("ForeName")
                    if last_name is not None and last_name.text:
                        name = last_name.text
                        if fore_name is not None and fore_name.text:
                            name = f"{name}, {fore_name.text}"
                        authors.append(name)
                record["authors"] = "; ".join(authors)

            # MeSH descriptors and qualifiers
            mesh_headings = article.findall(".//MeshHeading")
            if mesh_headings:
                descriptors = []
                qualifiers = []
                for heading in mesh_headings:
                    desc = heading.find("DescriptorName")
                    if desc is not None and desc.text:
                        descriptors.append(desc.text)

                    qual_elems = heading.findall("QualifierName")
                    for qual in qual_elems:
                        if qual is not None and qual.text:
                            qualifiers.append(qual.text)

                record["mesh_descriptors"] = " | ".join(descriptors) if descriptors else None
                record["mesh_qualifiers"] = " | ".join(qualifiers) if qualifiers else None

            # Chemical list
            chemicals = article.findall(".//ChemicalList/Chemical/NameOfSubstance")
            if chemicals:
                chem_list = [chem.text for chem in chemicals if chem is not None and chem.text]
                record["chemical_list"] = " | ".join(chem_list) if chem_list else None

            # Publication types
            pub_types = article.findall(".//PublicationType")
            if pub_types:
                types_list = [pt.text for pt in pub_types if pt is not None and pt.text]
                record["doc_type"] = " | ".join(types_list) if types_list else None

            # Date completed
            date_completed = article.find(".//DateCompleted")
            if date_completed is not None:
                year = date_completed.find("Year")
                month = date_completed.find("Month")
                day = date_completed.find("Day")
                record["year_completed"] = int(year.text) if year is not None and year.text else None
                record["month_completed"] = int(month.text) if month is not None and month.text else None
                record["day_completed"] = int(day.text) if day is not None and day.text else None

            # Date revised
            date_revised = article.find(".//DateRevised")
            if date_revised is not None:
                year = date_revised.find("Year")
                month = date_revised.find("Month")
                day = date_revised.find("Day")
                record["year_revised"] = int(year.text) if year is not None and year.text else None
                record["month_revised"] = int(month.text) if month is not None and month.text else None
                record["day_revised"] = int(day.text) if day is not None and day.text else None

            return record

        except Exception as e:
            self.logger.error("article_parse_error", error=str(e))
            return None

    def _normalize_date(self, year: int | None, month: str | None, day: str | None) -> str | None:
        """Normalize date to ISO YYYY-MM-DD format."""
        if not year:
            return None

        year_str = str(year).zfill(4)
        month_str = "00"
        day_str = "00"

        if month:
            month_int = self._month_to_int(month)
            month_str = str(month_int).zfill(2)

        if day:
            day_str = str(day).zfill(2)

        return f"{year_str}-{month_str}-{day_str}"

    def _month_to_int(self, month: str) -> int:
        """Convert month name or abbreviation to int."""
        months_map = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }
        month_lower = month.lower()[:3]
        return months_map.get(month_lower, 1)

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize PubMed record."""
        common = normalize_common_bibliography(
            record,
            doi="doi",
            title="title",
            journal="journal",
            authors="authors",
            journal_normalizer=lambda value: (
                NORMALIZER_STRING.normalize(value) if value is not None else None
            ),
        )

        normalized = dict(common)

        # PMID
        if "pmid" in record:
            pubmed_id = NORMALIZER_ID.normalize(str(record["pmid"]))
            normalized["pubmed_id"] = pubmed_id
            # Also add pubmed_pmid field (duplicate for merge logic)
            normalized["pubmed_pmid"] = int(record["pmid"]) if record["pmid"] else None

        doi_clean = common.get("doi_clean")
        if doi_clean:
            normalized["pubmed_doi"] = doi_clean

        # Abstract
        if "abstract" in record:
            normalized["abstract"] = NORMALIZER_STRING.normalize(record["abstract"])

        if "journal_abbrev" in record:
            normalized["journal_abbrev"] = NORMALIZER_STRING.normalize(record["journal_abbrev"])

        # ISSN
        if "issn" in record and isinstance(record["issn"], list):
            normalized["issn_print"] = record["issn"][0] if len(record["issn"]) > 0 else None
            normalized["issn_electronic"] = record["issn"][1] if len(record["issn"]) > 1 else None
            # Also add combined ISSN field
            normalized["pubmed_issn"] = "; ".join(record["issn"]) if record["issn"] else None

        # Volume, Issue
        if "volume" in record:
            normalized["volume"] = record["volume"]
        if "issue" in record:
            normalized["issue"] = record["issue"]

        # Pages
        if "pages" in record:
            pages = record["pages"]
            # Parse "123-145" format
            match = re.search(r"(\d+)-(\d+)", pages)
            if match:
                normalized["first_page"] = match.group(1)
                normalized["last_page"] = match.group(2)
            else:
                normalized["first_page"] = pages

        # Date
        if "pub_date" in record:
            normalized["year"] = record.get("year")
        if "year" in record:
            normalized["year"] = record["year"]

        # MeSH descriptors
        if "mesh_descriptors" in record:
            normalized["mesh_descriptors"] = record["mesh_descriptors"]

        # MeSH qualifiers
        if "mesh_qualifiers" in record:
            normalized["mesh_qualifiers"] = record["mesh_qualifiers"]

        # Chemical list
        if "chemical_list" in record:
            normalized["chemical_list"] = record["chemical_list"]

        # Doc type
        if "doc_type" in record:
            normalized["doc_type"] = record["doc_type"]

        # Dates
        if "year_completed" in record:
            normalized["year_completed"] = record["year_completed"]
        if "month_completed" in record:
            normalized["month_completed"] = record["month_completed"]
        if "day_completed" in record:
            normalized["day_completed"] = record["day_completed"]
        if "year_revised" in record:
            normalized["year_revised"] = record["year_revised"]
        if "month_revised" in record:
            normalized["month_revised"] = record["month_revised"]
        if "day_revised" in record:
            normalized["day_revised"] = record["day_revised"]

        return normalized

