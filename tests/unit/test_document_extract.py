"""Unit тесты для модуля извлечения данных документов."""

from unittest.mock import Mock

from library.documents.extract import extract_from_chembl
from library.documents.extract import extract_from_crossref
from library.documents.extract import extract_from_openalex
from library.documents.extract import extract_from_pubmed
from library.documents.extract import extract_from_semantic_scholar


class TestExtractFromPubMed:
    """Тесты для извлечения данных из PubMed."""
    
    def test_extract_from_pubmed_empty_list(self):
        """Тест с пустым списком PMID."""
        client = Mock()
        result = extract_from_pubmed(client, [], 100)
        assert result.empty
    
    def test_extract_from_pubmed_success(self):
        """Тест успешного извлечения данных."""
        client = Mock()
        client.fetch_by_pmids.return_value = {
            "12345": {
                "PubMed.PMID": "12345",
                "PubMed.Title": "Test Article",
                "PubMed.Abstract": "Test abstract",
                "PubMed.Authors": "Test Author"
            }
        }
        
        result = extract_from_pubmed(client, ["12345"], 100)
        
        assert not result.empty
        assert len(result) == 1
        assert result.iloc[0]["PubMed.PMID"] == "12345"
        assert result.iloc[0]["PubMed.Title"] == "Test Article"
    
    def test_extract_from_pubmed_batch_processing(self):
        """Тест батч-обработки."""
        client = Mock()
        client.fetch_by_pmids.return_value = {
            "12345": {"PubMed.PMID": "12345", "PubMed.Title": "Test 1"},
            "12346": {"PubMed.PMID": "12346", "PubMed.Title": "Test 2"}
        }
        
        result = extract_from_pubmed(client, ["12345", "12346"], 1)  # batch_size=1
        
        assert not result.empty
        assert len(result) == 2
        assert client.fetch_by_pmids.call_count == 2
    
    def test_extract_from_pubmed_error_handling(self):
        """Тест обработки ошибок."""
        client = Mock()
        client.fetch_by_pmids.side_effect = Exception("API Error")
        
        result = extract_from_pubmed(client, ["12345"], 100)
        
        assert not result.empty
        assert len(result) == 1
        assert result.iloc[0]["PubMed.PMID"] == "12345"
        assert "API Error" in result.iloc[0]["PubMed.Error"]


class TestExtractFromCrossref:
    """Тесты для извлечения данных из Crossref."""
    
    def test_extract_from_crossref_empty_list(self):
        """Тест с пустым списком DOI."""
        client = Mock()
        result = extract_from_crossref(client, [], 100)
        assert result.empty
    
    def test_extract_from_crossref_success(self):
        """Тест успешного извлечения данных."""
        client = Mock()
        client.fetch_by_dois_batch.return_value = {
            "10.1000/test": {
                "crossref.DOI": "10.1000/test",
                "crossref.Title": "Test Article",
                "crossref.Authors": "Test Author"
            }
        }
        
        result = extract_from_crossref(client, ["10.1000/test"], 100)
        
        assert not result.empty
        assert len(result) == 1
        assert result.iloc[0]["crossref.DOI"] == "10.1000/test"
        assert result.iloc[0]["crossref.Title"] == "Test Article"
    
    def test_extract_from_crossref_fallback_to_single(self):
        """Тест fallback к одиночным запросам."""
        client = Mock()
        client.fetch_by_doi.return_value = {
            "crossref.DOI": "10.1000/test",
            "crossref.Title": "Test Article"
        }
        
        result = extract_from_crossref(client, ["10.1000/test"], 100)
        
        assert not result.empty
        assert len(result) == 1
        client.fetch_by_doi.assert_called_once_with("10.1000/test")


class TestExtractFromOpenAlex:
    """Тесты для извлечения данных из OpenAlex."""
    
    def test_extract_from_openalex_success(self):
        """Тест успешного извлечения данных."""
        client = Mock()
        client.fetch_by_pmids_batch.return_value = {
            "12345": {
                "OpenAlex.PMID": "12345",
                "OpenAlex.Title": "Test Article",
                "OpenAlex.Year": "2023"
            }
        }
        
        result = extract_from_openalex(client, ["12345"], 100)
        
        assert not result.empty
        assert len(result) == 1
        assert result.iloc[0]["OpenAlex.PMID"] == "12345"
        assert result.iloc[0]["OpenAlex.Title"] == "Test Article"


class TestExtractFromSemanticScholar:
    """Тесты для извлечения данных из Semantic Scholar."""
    
    def test_extract_from_semantic_scholar_success(self):
        """Тест успешного извлечения данных."""
        client = Mock()
        client.fetch_by_pmids_batch.return_value = {
            "12345": {
                "scholar.PMID": "12345",
                "scholar.Title": "Test Article",
                "scholar.CitationCount": "10"
            }
        }
        
        result = extract_from_semantic_scholar(client, ["12345"], 100)
        
        assert not result.empty
        assert len(result) == 1
        assert result.iloc[0]["scholar.PMID"] == "12345"
        assert result.iloc[0]["scholar.Title"] == "Test Article"


class TestExtractFromChEMBL:
    """Тесты для извлечения данных из ChEMBL."""
    
    def test_extract_from_chembl_success(self):
        """Тест успешного извлечения данных."""
        client = Mock()
        client.fetch_documents_batch.return_value = {
            "CHEMBL123": {
                "ChEMBL.document_chembl_id": "CHEMBL123",
                "ChEMBL.Title": "Test Article",
                "ChEMBL.Journal": "Test Journal"
            }
        }
        
        result = extract_from_chembl(client, ["CHEMBL123"], 100)
        
        assert not result.empty
        assert len(result) == 1
        assert result.iloc[0]["ChEMBL.document_chembl_id"] == "CHEMBL123"
        assert result.iloc[0]["ChEMBL.Title"] == "Test Article"
