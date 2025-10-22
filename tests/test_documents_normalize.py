"""Tests for documents normalize module."""

import pandas as pd

from library.documents.normalize import DocumentNormalizer


class TestDocumentNormalizer:
    """Test cases for DocumentNormalizer class."""

    def test_init(self):
        """Test normalizer initialization."""
        normalizer = DocumentNormalizer()
        assert normalizer.config == {}
        
        config = {"test": "value"}
        normalizer = DocumentNormalizer(config)
        assert normalizer.config == config

    def test_normalize_documents_empty_dataframe(self):
        """Test normalization with empty DataFrame."""
        normalizer = DocumentNormalizer()
        df = pd.DataFrame()
        result = normalizer.normalize_documents(df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_normalize_documents_basic(self):
        """Test basic document normalization."""
        normalizer = DocumentNormalizer()
        
        # Create test data
        df = pd.DataFrame({
            'document_chembl_id': ['CHEMBL123', 'CHEMBL456'],
            'title': ['Test Title 1', 'Test Title 2'],
            'doi': ['10.1000/test1', '10.1000/test2'],
            'year': [2020, 2021]
        })
        
        result = normalizer.normalize_documents(df)
        
        # Check that result is a DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        # Check that required columns are present
        assert 'document_chembl_id' in result.columns
        assert 'title' in result.columns
        assert 'doi' in result.columns
        assert 'year' in result.columns
        
        # Check that computed columns are added
        assert 'publication_date' in result.columns
        assert 'document_sortorder' in result.columns

    def test_initialize_all_columns(self):
        """Test column initialization."""
        normalizer = DocumentNormalizer()
        
        df = pd.DataFrame({
            'document_chembl_id': ['CHEMBL123'],
            'title': ['Test Title']
        })
        
        result = normalizer._initialize_all_columns(df)
        
        # Check that all expected columns are present
        expected_columns = {
            'document_chembl_id', 'title', 'doi', 'document_pubmed_id',
            'journal', 'year', 'abstract', 'pubmed_authors',
            'publication_date', 'document_sortorder'
        }
        
        for col in expected_columns:
            assert col in result.columns

    def test_add_publication_date_column(self):
        """Test publication date column addition."""
        normalizer = DocumentNormalizer()
        
        df = pd.DataFrame({
            'pubmed_year': [2020, 2021, None],
            'pubmed_month': [1, 2, None],
            'pubmed_day': [15, 20, None],
            'year': [2019, None, 2022]
        })
        
        result = normalizer._add_publication_date_column(df)
        
        assert 'publication_date' in result.columns
        assert result['publication_date'].iloc[0] == '2020-01-15'
        assert result['publication_date'].iloc[1] == '2021-02-20'
        assert result['publication_date'].iloc[2] == '2022-01-01'

    def test_determine_document_sortorder(self):
        """Test document sort order determination."""
        normalizer = DocumentNormalizer()
        
        row = pd.Series({
            'pubmed_issn': '1234-5678',
            'publication_date': '2020-01-15'
        }, name=0)
        
        result = normalizer._determine_document_sortorder(row)
        
        assert isinstance(result, str)
        assert '1234-5678' in result
        assert '2020-01-15' in result
        assert '000000' in result  # index 0 padded to 6 digits

    def test_normalize_documents_with_config(self):
        """Test document normalization with configuration."""
        config = {'chembl_release': 'v30'}
        normalizer = DocumentNormalizer(config)
        
        df = pd.DataFrame({
            'document_chembl_id': ['CHEMBL123', 'CHEMBL456'],
            'title': ['Test Title 1', 'Test Title 2'],
            'doi': ['10.1000/test1', '10.1000/test2'],
            'year': [2020, 2021]
        })
        
        result = normalizer.normalize_documents(df)
        
        # Check that result is a DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        # Check that required columns are present
        assert 'document_chembl_id' in result.columns
        assert 'title' in result.columns
        assert 'doi' in result.columns
        assert 'year' in result.columns
        
        # Check that computed columns are added
        assert 'publication_date' in result.columns
        assert 'document_sortorder' in result.columns
        
        # Check that config is preserved
        assert normalizer.config == config
