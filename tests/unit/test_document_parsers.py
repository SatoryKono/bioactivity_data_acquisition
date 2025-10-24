#!/usr/bin/env python3
"""Тесты для парсинга документов из различных источников."""

import pytest
import json
from unittest.mock import Mock

# Тесты для PubMed парсинга
def test_pubmed_mesh_parsing():
    """Тест парсинга MeSH данных из PubMed XML."""
    from src.library.clients.pubmed import PubMedClient
    from src.library.config import APIClientConfig
    
    # Создаем мок клиента
    config = APIClientConfig(
        name="pubmed",
        base_url="https://eutils.ncbi.nlm.nih.gov",
        timeout_sec=30.0,
        headers={"User-Agent": "test/1.0"}
    )
    client = PubMedClient(config)
    
    # Тестовые XML данные
    xml_content = """
    <PubmedArticle>
        <MedlineCitation>
            <MeshHeadingList>
                <MeshHeading>
                    <DescriptorName MajorTopicYN="N">Dopamine</DescriptorName>
                    <QualifierName MajorTopicYN="Y">pharmacology</QualifierName>
                </MeshHeading>
                <MeshHeading>
                    <DescriptorName MajorTopicYN="Y">Receptors, Dopamine D3</DescriptorName>
                </MeshHeading>
            </MeshHeadingList>
            <ChemicalList>
                <Chemical>
                    <NameOfSubstance>Dopamine</NameOfSubstance>
                </Chemical>
            </ChemicalList>
        </MedlineCitation>
    </PubmedArticle>
    """
    
    # Тестируем парсинг
    mesh_descriptors, mesh_qualifiers = client._parse_mesh_data(xml_content)
    chemical_list = client._parse_chemical_list(xml_content)
    
    assert "Dopamine" in mesh_descriptors
    assert "Receptors, Dopamine D3" in mesh_descriptors
    assert "pharmacology" in mesh_qualifiers
    assert "Dopamine" in chemical_list

def test_crossref_field_extraction():
    """Тест извлечения полей из Crossref."""
    from src.library.clients.crossref import CrossrefClient
    from src.library.config import APIClientConfig
    
    config = APIClientConfig(
        name="crossref",
        base_url="https://api.crossref.org",
        timeout_sec=30.0,
        headers={"User-Agent": "test/1.0"}
    )
    client = CrossrefClient(config)
    
    # Тестовые данные Crossref
    work_data = {
        "DOI": "10.1016/j.bmc.2007.08.038",
        "title": ["Test Article"],
        "container-title": ["Bioorganic & Medicinal Chemistry"],
        "ISSN": ["0968-0896", "1464-3391"],
        "type": "journal-article",
        "subject": ["Medicinal Chemistry"],
        "author": [
            {"given": "John", "family": "Doe"},
            {"given": "Jane", "family": "Smith"}
        ]
    }
    
    # Тестируем парсинг
    result = client._parse_work(work_data)
    
    assert result["crossref_doi"] == "10.1016/j.bmc.2007.08.038"
    assert result["crossref_title"] == "Test Article"
    assert result["crossref_journal"] == "Bioorganic & Medicinal Chemistry"
    assert "0968-0896" in result["crossref_issn"]
    assert result["crossref_doc_type"] == "journal-article"

def test_openalex_field_extraction():
    """Тест извлечения полей из OpenAlex."""
    from src.library.clients.openalex import OpenAlexClient
    from src.library.config import APIClientConfig
    
    config = APIClientConfig(
        name="openalex",
        base_url="https://api.openalex.org",
        timeout_sec=30.0,
        headers={"User-Agent": "test/1.0"}
    )
    client = OpenAlexClient(config)
    
    # Тестовые данные OpenAlex
    work_data = {
        "id": "https://openalex.org/W1234567890",
        "doi": "https://doi.org/10.1016/j.bmc.2007.08.038",
        "title": "Test Article",
        "type": "journal-article",
        "type_crossref": "journal-article",
        "host_venue": {
            "display_name": "Bioorganic & Medicinal Chemistry",
            "issn": ["0968-0896"]
        },
        "authorships": [
            {
                "author": {
                    "display_name": "John Doe"
                }
            },
            {
                "author": {
                    "display_name": "Jane Smith"
                }
            }
        ],
        "biblio": {
            "volume": "15",
            "issue": "23",
            "first_page": "7248",
            "last_page": "7257"
        }
    }
    
    # Тестируем парсинг
    result = client._parse_work(work_data)
    
    assert result["openalex_doi"] == "10.1016/j.bmc.2007.08.038"
    assert result["openalex_title"] == "Test Article"
    assert result["openalex_journal"] == "Bioorganic & Medicinal Chemistry"
    assert result["openalex_volume"] == "15"
    assert result["openalex_issue"] == "23"
    assert result["openalex_first_page"] == "7248"
    assert result["openalex_last_page"] == "7257"

def test_semantic_scholar_field_extraction():
    """Тест извлечения полей из Semantic Scholar."""
    from src.library.clients.semantic_scholar import SemanticScholarClient
    from src.library.config import APIClientConfig
    
    config = APIClientConfig(
        name="semantic_scholar",
        base_url="https://api.semanticscholar.org",
        timeout_sec=30.0,
        headers={"User-Agent": "test/1.0"}
    )
    client = SemanticScholarClient(config)
    
    # Тестовые данные Semantic Scholar
    paper_data = {
        "paperId": "1234567890",
        "title": "Test Article",
        "year": 2007,
        "externalIds": {
            "DOI": "10.1016/j.bmc.2007.08.038",
            "PMID": "17827018"
        },
        "authors": [
            {"name": "John Doe"},
            {"name": "Jane Smith"}
        ],
        "venue": {
            "name": "Bioorganic & Medicinal Chemistry",
            "issn": "0968-0896"
        },
        "publicationTypes": ["JournalArticle"]
    }
    
    # Тестируем парсинг
    result = client._parse_paper(paper_data)
    
    assert result["semantic_scholar_semantic_scholar_id"] == "1234567890"
    assert result["semantic_scholar_title"] == "Test Article"
    assert result["semantic_scholar_doi"] == "10.1016/j.bmc.2007.08.038"
    assert result["semantic_scholar_journal"] == "Bioorganic & Medicinal Chemistry"
    assert result["semantic_scholar_issn"] == "0968-0896"
    assert result["semantic_scholar_doc_type"] == "JournalArticle"

def test_empty_field_handling():
    """Тест обработки пустых полей."""
    from src.library.clients.pubmed import PubMedClient
    from src.library.config import APIClientConfig
    
    config = APIClientConfig(
        name="pubmed",
        base_url="https://eutils.ncbi.nlm.nih.gov",
        timeout_sec=30.0,
        headers={"User-Agent": "test/1.0"}
    )
    client = PubMedClient(config)
    
    # Тестируем создание пустой записи
    empty_record = client._create_empty_record("12345", "Test error")
    
    # Проверяем, что все поля возвращают пустые строки, а не None
    assert empty_record["pubmed_doi"] == ""
    assert empty_record["pubmed_title"] == ""
    assert empty_record["pubmed_abstract"] == ""
    assert empty_record["pubmed_journal"] == ""
    assert empty_record["pubmed_issn"] == ""
    assert empty_record["pubmed_error"] == "Test error"

if __name__ == "__main__":
    pytest.main([__file__])
