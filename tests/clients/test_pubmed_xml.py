"""Tests for PubMed XML parsing with lxml."""

import pytest
from unittest.mock import Mock, patch
from lxml import etree

from library.clients.pubmed import PubMedClient
from library.xml import make_xml_parser


@pytest.fixture
def pubmed_client():
    """Create a PubMedClient instance for testing."""
    config = Mock()
    config.api_key = None
    config.headers = {}
    config.base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    return PubMedClient(config)


@pytest.fixture
def valid_xml_fixture():
    """Load valid PubMed XML fixture."""
    with open("tests/parsing/fixtures/pubmed_article_valid.xml", "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def broken_xml_fixture():
    """Load broken PubMed XML fixture."""
    with open("tests/parsing/fixtures/pubmed_article_broken.xml", "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def no_doi_xml_fixture():
    """Load PubMed XML fixture without DOI."""
    with open("tests/parsing/fixtures/pubmed_article_no_doi.xml", "r", encoding="utf-8") as f:
        return f.read()


def test_enhance_with_efetch_valid_xml(pubmed_client, valid_xml_fixture):
    """Test efetch enhancement with valid XML."""
    record = {}
    
    # Mock the requests.get call
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = valid_xml_fixture
    
    with patch('requests.get', return_value=mock_response):
        enhanced = pubmed_client._enhance_with_efetch(record, "12345678")
        
        # Check extracted data
        assert enhanced["pubmed_doi"] == "10.1234/example.2023.001"
        assert "background section" in enhanced["pubmed_abstract"]
        assert "methods section" in enhanced["pubmed_abstract"]
        assert "Research" in enhanced["pubmed_mesh_descriptors"]
        assert "methods" in enhanced["pubmed_mesh_qualifiers"]
        assert "Example Chemical" in enhanced["pubmed_chemical_list"]


def test_enhance_with_efetch_broken_xml(pubmed_client, broken_xml_fixture):
    """Test efetch enhancement with broken XML (recover=True should handle it)."""
    record = {}
    
    # Mock the requests.get call
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = broken_xml_fixture
    
    with patch('requests.get', return_value=mock_response):
        enhanced = pubmed_client._enhance_with_efetch(record, "12345678")
        
        # Should still extract some data despite broken XML
        assert enhanced["pubmed_doi"] == "10.1234/example.2023.001"
        assert "Example Article Title" in enhanced.get("pubmed_abstract", "")


def test_enhance_with_efetch_no_doi(pubmed_client, no_doi_xml_fixture):
    """Test efetch enhancement with XML that has no DOI."""
    record = {}
    
    # Mock the requests.get call
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = no_doi_xml_fixture
    
    with patch('requests.get', return_value=mock_response):
        enhanced = pubmed_client._enhance_with_efetch(record, "12345678")
        
        # Should not have DOI
        assert "pubmed_doi" not in enhanced or enhanced["pubmed_doi"] == ""
        # Should still extract other data
        assert "This article has no DOI" in enhanced["pubmed_abstract"]


def test_extract_doi_method(pubmed_client, valid_xml_fixture):
    """Test _extract_doi method directly."""
    parser = make_xml_parser(recover=True)
    root = etree.fromstring(valid_xml_fixture.encode('utf-8'), parser)
    
    record = {}
    pubmed_client._extract_doi(root, record)
    
    assert record["pubmed_doi"] == "10.1234/example.2023.001"


def test_extract_abstract_method(pubmed_client, valid_xml_fixture):
    """Test _extract_abstract method directly."""
    parser = make_xml_parser(recover=True)
    root = etree.fromstring(valid_xml_fixture.encode('utf-8'), parser)
    
    record = {}
    pubmed_client._extract_abstract(root, record)
    
    abstract = record["pubmed_abstract"]
    assert "background section" in abstract
    assert "methods section" in abstract
    assert "results section" in abstract
    assert "conclusions section" in abstract


def test_extract_mesh_terms_method(pubmed_client, valid_xml_fixture):
    """Test _extract_mesh_terms method directly."""
    parser = make_xml_parser(recover=True)
    root = etree.fromstring(valid_xml_fixture.encode('utf-8'), parser)
    
    record = {}
    pubmed_client._extract_mesh_terms(root, record)
    
    assert "Research" in record["pubmed_mesh_descriptors"]
    assert "Data Analysis" in record["pubmed_mesh_descriptors"]
    assert "methods" in record["pubmed_mesh_qualifiers"]


def test_extract_chemicals_method(pubmed_client, valid_xml_fixture):
    """Test _extract_chemicals method directly."""
    parser = make_xml_parser(recover=True)
    root = etree.fromstring(valid_xml_fixture.encode('utf-8'), parser)
    
    record = {}
    pubmed_client._extract_chemicals(root, record)
    
    assert "Example Chemical" in record["pubmed_chemical_list"]
    assert "Another Chemical" in record["pubmed_chemical_list"]


def test_enhance_with_efetch_network_error(pubmed_client):
    """Test efetch enhancement with network error."""
    record = {}
    
    with patch('requests.get', side_effect=Exception("Network error")):
        enhanced = pubmed_client._enhance_with_efetch(record, "12345678")
        
        # Should return original record unchanged
        assert enhanced == record


def test_enhance_with_efetch_non_200_response(pubmed_client):
    """Test efetch enhancement with non-200 response."""
    record = {}
    
    # Mock the requests.get call
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.text = "Not found"
    
    with patch('requests.get', return_value=mock_response):
        enhanced = pubmed_client._enhance_with_efetch(record, "12345678")
        
        # Should return original record unchanged
        assert enhanced == record
