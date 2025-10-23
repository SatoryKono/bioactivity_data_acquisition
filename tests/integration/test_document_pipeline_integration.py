"""Интеграционные тесты для пайплайна документов."""

import pandas as pd
import pytest
from unittest.mock import Mock, patch

from library.documents.pipeline import DocumentPipeline
from library.documents.config import DocumentConfig


class TestDocumentPipelineIntegration:
    """Интеграционные тесты для DocumentPipeline."""
    
    @pytest.fixture
    def sample_input_data(self):
        """Тестовые входные данные."""
        return pd.DataFrame({
            "document_chembl_id": ["CHEMBL1137491", "CHEMBL1155082"],
            "doi": ["10.1016/j.bmc.2007.08.038", "10.1021/jm800092x"],
            "title": ["Test Article 1", "Test Article 2"],
            "document_pubmed_id": ["17827018", "18578478"],
            "abstract": ["Abstract 1", "Abstract 2"],
            "authors": ["Author 1", "Author 2"],
            "journal": ["Journal 1", "Journal 2"],
            "year": [2008, 2008]
        })
    
    @pytest.fixture
    def mock_config(self):
        """Мок конфигурации."""
        config = Mock(spec=DocumentConfig)
        config.sources = {
            "pubmed": Mock(enabled=True, batch_size=200),
            "crossref": Mock(enabled=True, batch_size=100),
            "openalex": Mock(enabled=True, batch_size=50),
            "semanticscholar": Mock(enabled=True, batch_size=100)
        }
        config.runtime = Mock(allow_incomplete_sources=True, limit=None)
        config.http = Mock(global_=Mock(timeout_sec=60.0))
        return config
    
    @pytest.fixture
    def mock_clients(self):
        """Мок клиентов API."""
        clients = {}
        
        # PubMed client
        pubmed_client = Mock()
        pubmed_client.fetch_by_pmids.return_value = {
            "17827018": {
                "PubMed.PMID": "17827018",
                "PubMed.Title": "PubMed Title 1",
                "PubMed.Abstract": "PubMed Abstract 1",
                "PubMed.Authors": "PubMed Author 1",
                "PubMed.Journal": "PubMed Journal 1",
                "PubMed.Year": "2008"
            },
            "18578478": {
                "PubMed.PMID": "18578478",
                "PubMed.Title": "PubMed Title 2",
                "PubMed.Abstract": "PubMed Abstract 2",
                "PubMed.Authors": "PubMed Author 2",
                "PubMed.Journal": "PubMed Journal 2",
                "PubMed.Year": "2008"
            }
        }
        clients["pubmed"] = pubmed_client
        
        # Crossref client
        crossref_client = Mock()
        crossref_client.fetch_by_dois_batch.return_value = {
            "10.1016/j.bmc.2007.08.038": {
                "crossref.DOI": "10.1016/j.bmc.2007.08.038",
                "crossref.Title": "Crossref Title 1",
                "crossref.Authors": "Crossref Author 1",
                "crossref.Journal": "Crossref Journal 1",
                "crossref.Year": "2008"
            },
            "10.1021/jm800092x": {
                "crossref.DOI": "10.1021/jm800092x",
                "crossref.Title": "Crossref Title 2",
                "crossref.Authors": "Crossref Author 2",
                "crossref.Journal": "Crossref Journal 2",
                "crossref.Year": "2008"
            }
        }
        clients["crossref"] = crossref_client
        
        # OpenAlex client
        openalex_client = Mock()
        openalex_client.fetch_by_pmids_batch.return_value = {
            "17827018": {
                "OpenAlex.PMID": "17827018",
                "OpenAlex.Title": "OpenAlex Title 1",
                "OpenAlex.Year": "2008"
            },
            "18578478": {
                "OpenAlex.PMID": "18578478",
                "OpenAlex.Title": "OpenAlex Title 2",
                "OpenAlex.Year": "2008"
            }
        }
        clients["openalex"] = openalex_client
        
        # Semantic Scholar client
        semantic_client = Mock()
        semantic_client.fetch_by_pmids_batch.return_value = {
            "17827018": {
                "scholar.PMID": "17827018",
                "scholar.Title": "Scholar Title 1",
                "scholar.CitationCount": "15"
            },
            "18578478": {
                "scholar.PMID": "18578478",
                "scholar.Title": "Scholar Title 2",
                "scholar.CitationCount": "25"
            }
        }
        clients["semanticscholar"] = semantic_client
        
        return clients
    
    def test_extract_with_all_sources(self, sample_input_data, mock_config, mock_clients):
        """Тест извлечения данных из всех источников."""
        pipeline = DocumentPipeline(mock_config)
        pipeline.clients = mock_clients
        
        # Мокаем нормализацию
        with patch.object(pipeline, '_normalize_columns', return_value=sample_input_data):
            result = pipeline.extract(sample_input_data)
        
        # Проверяем, что данные извлечены из всех источников
        assert not result.empty
        assert len(result) == 2
        
        # Проверяем поля из PubMed
        assert "PubMed.Title" in result.columns
        assert "PubMed.Abstract" in result.columns
        assert "PubMed.Authors" in result.columns
        
        # Проверяем поля из Crossref
        assert "crossref.Title" in result.columns
        assert "crossref.Authors" in result.columns
        
        # Проверяем поля из OpenAlex
        assert "OpenAlex.Title" in result.columns
        assert "OpenAlex.Year" in result.columns
        
        # Проверяем поля из Semantic Scholar
        assert "scholar.Title" in result.columns
        assert "scholar.CitationCount" in result.columns
    
    def test_extract_with_partial_failure(self, sample_input_data, mock_config, mock_clients):
        """Тест извлечения с частичными ошибками."""
        # Настраиваем один клиент для ошибки
        mock_clients["pubmed"].fetch_by_pmids.side_effect = Exception("PubMed API Error")
        
        pipeline = DocumentPipeline(mock_config)
        pipeline.clients = mock_clients
        
        with patch.object(pipeline, '_normalize_columns', return_value=sample_input_data):
            result = pipeline.extract(sample_input_data)
        
        # Проверяем, что обработка продолжилась несмотря на ошибку PubMed
        assert not result.empty
        assert len(result) == 2
        
        # Проверяем, что данные из других источников извлечены
        assert "crossref.Title" in result.columns
        assert "OpenAlex.Title" in result.columns
        assert "scholar.Title" in result.columns
        
        # Проверяем, что ошибка PubMed записана
        assert "PubMed.Error" in result.columns
        assert "PubMed API Error" in result["PubMed.Error"].iloc[0]
    
    def test_merge_source_data_pubmed(self, sample_input_data, mock_config):
        """Тест объединения данных из PubMed."""
        pipeline = DocumentPipeline(mock_config)
        
        source_data = pd.DataFrame({
            "PubMed.PMID": ["17827018", "18578478"],
            "PubMed.Title": ["PubMed Title 1", "PubMed Title 2"],
            "PubMed.Abstract": ["PubMed Abstract 1", "PubMed Abstract 2"]
        })
        
        result = pipeline._merge_source_data(sample_input_data, source_data, "pubmed")
        
        assert not result.empty
        assert len(result) == 2
        assert "PubMed.Title" in result.columns
        assert "PubMed.Abstract" in result.columns
        
        # Проверяем, что данные объединены правильно
        assert result.iloc[0]["PubMed.Title"] == "PubMed Title 1"
        assert result.iloc[1]["PubMed.Title"] == "PubMed Title 2"
    
    def test_merge_source_data_crossref(self, sample_input_data, mock_config):
        """Тест объединения данных из Crossref."""
        pipeline = DocumentPipeline(mock_config)
        
        source_data = pd.DataFrame({
            "crossref.DOI": ["10.1016/j.bmc.2007.08.038", "10.1021/jm800092x"],
            "crossref.Title": ["Crossref Title 1", "Crossref Title 2"],
            "crossref.Authors": ["Crossref Author 1", "Crossref Author 2"]
        })
        
        result = pipeline._merge_source_data(sample_input_data, source_data, "crossref")
        
        assert not result.empty
        assert len(result) == 2
        assert "crossref.Title" in result.columns
        assert "crossref.Authors" in result.columns
        
        # Проверяем, что данные объединены правильно
        assert result.iloc[0]["crossref.Title"] == "Crossref Title 1"
        assert result.iloc[1]["crossref.Title"] == "Crossref Title 2"
    
    def test_normalize_with_publication_date(self, sample_input_data, mock_config):
        """Тест нормализации с вычислением даты публикации."""
        pipeline = DocumentPipeline(mock_config)
        
        # Добавляем данные с датами
        sample_input_data["pubmed_year"] = [2008, 2008]
        sample_input_data["pubmed_month"] = [1, 8]
        sample_input_data["pubmed_day"] = [1, 1]
        
        with patch.object(pipeline.normalizer, 'normalize_documents', return_value=sample_input_data):
            with patch('library.documents.pipeline.normalize_journal_columns', return_value=sample_input_data):
                with patch('library.documents.pipeline.add_citation_column', return_value=sample_input_data):
                    result = pipeline.normalize(sample_input_data)
        
        assert "publication_date" in result.columns
        assert "document_sortorder" in result.columns
        
        # Проверяем вычисленные даты
        assert result.iloc[0]["publication_date"] == "2008-01-01"
        assert result.iloc[1]["publication_date"] == "2008-08-01"
    
    def test_create_error_dataframe(self, sample_input_data, mock_config):
        """Тест создания DataFrame с ошибками."""
        pipeline = DocumentPipeline(mock_config)
        
        result = pipeline._create_error_dataframe(sample_input_data, "pubmed", "Test Error")
        
        assert not result.empty
        assert len(result) == 2
        assert "pubmed_error" in result.columns
        assert "PubMed.PMID" in result.columns
        assert "PubMed.Error" in result.columns
        
        # Проверяем, что ошибка записана
        assert result.iloc[0]["pubmed_error"] == "Test Error"
        assert result.iloc[0]["PubMed.Error"] == "Test Error"
