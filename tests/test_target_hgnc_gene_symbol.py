"""Tests for HGNC and gene symbol extraction in target pipeline."""

from src.library.target.chembl_adapter import _parse_hgnc
from src.library.target.uniprot_adapter import extract_gene_symbol


class TestHGNCExtraction:
    """Test HGNC data extraction from ChEMBL cross-references."""
    
    def test_parse_hgnc_with_valid_data(self):
        """Should extract HGNC name and ID from cross-references."""
        xrefs = [
            {"xref_src_db": "HGNC", "xref_name": "ARG1", "xref_id": "HGNC:663"}
        ]
        hgnc_name, hgnc_id = _parse_hgnc(xrefs)
        assert hgnc_name == "ARG1"
        assert hgnc_id == "663"
    
    def test_parse_hgnc_with_no_hgnc(self):
        """Should return empty strings when HGNC not found."""
        xrefs = [
            {"xref_src_db": "UniProt", "xref_name": "P05089", "xref_id": "P05089"}
        ]
        hgnc_name, hgnc_id = _parse_hgnc(xrefs)
        assert hgnc_name == ""
        assert hgnc_id == ""
    
    def test_parse_hgnc_with_empty_list(self):
        """Should handle empty cross-reference list."""
        hgnc_name, hgnc_id = _parse_hgnc([])
        assert hgnc_name == ""
        assert hgnc_id == ""
    
    def test_parse_hgnc_with_multiple_xrefs(self):
        """Should extract HGNC from list with multiple cross-references."""
        xrefs = [
            {"xref_src_db": "UniProt", "xref_name": "P05089", "xref_id": "P05089"},
            {"xref_src_db": "HGNC", "xref_name": "LRRK2", "xref_id": "HGNC:18618"},
            {"xref_src_db": "Ensembl", "xref_name": "ENSG00000188906", "xref_id": "ENSG00000188906"}
        ]
        hgnc_name, hgnc_id = _parse_hgnc(xrefs)
        assert hgnc_name == "LRRK2"
        assert hgnc_id == "18618"
    
    def test_parse_hgnc_with_malformed_id(self):
        """Should handle malformed HGNC ID gracefully."""
        xrefs = [
            {"xref_src_db": "HGNC", "xref_name": "TEST", "xref_id": "INVALID_FORMAT"}
        ]
        hgnc_name, hgnc_id = _parse_hgnc(xrefs)
        assert hgnc_name == "TEST"
        assert hgnc_id == "INVALID_FORMAT"  # Should return the whole string if no colon


class TestGeneSymbolExtraction:
    """Test gene symbol extraction from UniProt data."""
    
    def test_extract_gene_symbol_from_genes_list(self):
        """Should extract gene symbol from genes list."""
        uniprot_data = {
            "genes": [
                {"geneName": {"value": "ARG1"}}
            ]
        }
        gene_symbol = extract_gene_symbol(uniprot_data)
        assert gene_symbol == "ARG1"
    
    def test_extract_gene_symbol_with_results_wrapper(self):
        """Should handle UniProt response with results wrapper."""
        uniprot_data = {
            "results": [
                {
                    "genes": [
                        {"geneName": {"value": "LRRK2"}}
                    ]
                }
            ]
        }
        gene_symbol = extract_gene_symbol(uniprot_data)
        assert gene_symbol == "LRRK2"
    
    def test_extract_gene_symbol_with_no_genes(self):
        """Should return None when no genes found."""
        uniprot_data = {"genes": []}
        gene_symbol = extract_gene_symbol(uniprot_data)
        assert gene_symbol is None
    
    def test_extract_gene_symbol_with_multiple_genes(self):
        """Should return first gene symbol when multiple genes present."""
        uniprot_data = {
            "genes": [
                {"geneName": {"value": "GENE1"}},
                {"geneName": {"value": "GENE2"}}
            ]
        }
        gene_symbol = extract_gene_symbol(uniprot_data)
        assert gene_symbol == "GENE1"
    
    def test_extract_gene_symbol_with_malformed_data(self):
        """Should handle malformed gene data gracefully."""
        uniprot_data = {
            "genes": [
                {"geneName": "INVALID_FORMAT"}  # Should be dict, not string
            ]
        }
        gene_symbol = extract_gene_symbol(uniprot_data)
        assert gene_symbol is None
    
    def test_extract_gene_symbol_with_empty_value(self):
        """Should return None when gene name value is empty."""
        uniprot_data = {
            "genes": [
                {"geneName": {"value": ""}}
            ]
        }
        gene_symbol = extract_gene_symbol(uniprot_data)
        assert gene_symbol is None
    
    def test_extract_gene_symbol_with_list_input(self):
        """Should handle list input format."""
        uniprot_data = [
            {
                "genes": [
                    {"geneName": {"value": "TEST_GENE"}}
                ]
            }
        ]
        gene_symbol = extract_gene_symbol(uniprot_data)  # type: ignore
        assert gene_symbol == "TEST_GENE"
    
    def test_extract_gene_symbol_with_none_input(self):
        """Should handle None input gracefully."""
        gene_symbol = extract_gene_symbol(None)  # type: ignore
        assert gene_symbol is None
    
    def test_extract_gene_symbol_with_invalid_structure(self):
        """Should handle completely invalid input structure."""
        gene_symbol = extract_gene_symbol("invalid_string")  # type: ignore
        assert gene_symbol is None
