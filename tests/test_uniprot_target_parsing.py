"""
Unit tests for UniProt target parsing functionality.

Tests the _parse_uniprot_entry function for correct extraction of
signal_peptide and ubiquitination fields.
"""

import pytest
from src.library.pipelines.target.uniprot_target import _parse_uniprot_entry


class TestUniprotTargetParsing:
    """Test cases for UniProt entry parsing."""

    def test_signal_peptide_with_description(self):
        """Test signal peptide extraction with description."""
        entry = {
            "primaryAccession": "P12345",
            "features": [
                {
                    "type": "Signal peptide",
                    "description": "Signal peptide (1-24)",
                    "location": {
                        "start": {"value": 1},
                        "end": {"value": 24}
                    }
                }
            ]
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["signal_peptide"] == "Signal peptide (1-24)"
        assert result["features_signal_peptide"] == "true"

    def test_signal_peptide_without_description_uses_coordinates(self):
        """Test signal peptide extraction without description uses coordinates."""
        entry = {
            "primaryAccession": "P12345",
            "features": [
                {
                    "type": "Signal",
                    "description": "",
                    "location": {
                        "start": {"value": 1},
                        "end": {"value": 24}
                    }
                }
            ]
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["signal_peptide"] == "1-24"
        assert result["features_signal_peptide"] == "true"

    def test_signal_peptide_no_coordinates_fallback(self):
        """Test signal peptide with no description or coordinates uses fallback."""
        entry = {
            "primaryAccession": "P12345",
            "features": [
                {
                    "type": "Signal peptide",
                    "description": "",
                    "location": {}
                }
            ]
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["signal_peptide"] == "present"
        assert result["features_signal_peptide"] == "true"

    def test_signal_peptide_case_insensitive(self):
        """Test signal peptide type matching is case insensitive."""
        entry = {
            "primaryAccession": "P12345",
            "features": [
                {
                    "type": "SIGNAL PEPTIDE",
                    "description": "Signal peptide (1-20)"
                }
            ]
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["signal_peptide"] == "Signal peptide (1-20)"
        assert result["features_signal_peptide"] == "true"

    def test_no_signal_peptide(self):
        """Test when no signal peptide features are present."""
        entry = {
            "primaryAccession": "P12345",
            "features": [
                {
                    "type": "Transmembrane region",
                    "description": "Helical"
                }
            ]
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["signal_peptide"] == ""
        assert result["features_signal_peptide"] == "false"

    def test_ubiquitination_standard_keywords(self):
        """Test ubiquitination detection with standard keywords."""
        entry = {
            "primaryAccession": "P12345",
            "features": [
                {
                    "type": "Modified residue",
                    "description": "Ubiquitination at Lys-48"
                }
            ]
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["ubiquitination"] == "Ubiquitination at Lys-48"

    def test_ubiquitination_extended_keywords(self):
        """Test ubiquitination detection with extended keywords."""
        test_cases = [
            ("Ubiquitinated at Lys-63", "ubiquitinat"),
            ("Ubiquitylation site", "ubiquitylat"),
            ("Ubl conjugation", "ubl conjugation"),
            ("GlyGly modification", "glygly")
        ]
        
        for description, keyword in test_cases:
            entry = {
                "primaryAccession": "P12345",
                "features": [
                    {
                        "type": "Modified residue",
                        "description": description
                    }
                ]
            }
            
            result = _parse_uniprot_entry(entry)
            
            assert result["ubiquitination"] == description, f"Failed for keyword: {keyword}"

    def test_ubiquitination_feature_type(self):
        """Test ubiquitination detection via feature type."""
        entry = {
            "primaryAccession": "P12345",
            "features": [
                {
                    "type": "Ubiquitination",
                    "description": "Ubiquitination site"
                }
            ]
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["ubiquitination"] == "Ubiquitination site"

    def test_ubiquitination_case_insensitive(self):
        """Test ubiquitination detection is case insensitive."""
        entry = {
            "primaryAccession": "P12345",
            "features": [
                {
                    "type": "Modified residue",
                    "description": "UBIQUITINATION at Lys-48"
                }
            ]
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["ubiquitination"] == "UBIQUITINATION at Lys-48"

    def test_no_ubiquitination(self):
        """Test when no ubiquitination features are present."""
        entry = {
            "primaryAccession": "P12345",
            "features": [
                {
                    "type": "Modified residue",
                    "description": "Phosphorylation at Ser-123"
                }
            ]
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["ubiquitination"] == ""

    def test_multiple_signal_peptides(self):
        """Test handling of multiple signal peptide features."""
        entry = {
            "primaryAccession": "P12345",
            "features": [
                {
                    "type": "Signal peptide",
                    "description": "Signal peptide (1-20)"
                },
                {
                    "type": "Signal",
                    "description": "Alternative signal (1-15)"
                }
            ]
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["signal_peptide"] == "Signal peptide (1-20)|Alternative signal (1-15)"
        assert result["features_signal_peptide"] == "true"

    def test_multiple_ubiquitination_sites(self):
        """Test handling of multiple ubiquitination sites."""
        entry = {
            "primaryAccession": "P12345",
            "features": [
                {
                    "type": "Modified residue",
                    "description": "Ubiquitination at Lys-48"
                },
                {
                    "type": "Modified residue",
                    "description": "GlyGly modification at Lys-63"
                }
            ]
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["ubiquitination"] == "Ubiquitination at Lys-48|GlyGly modification at Lys-63"

    def test_empty_features_list(self):
        """Test handling of empty features list."""
        entry = {
            "primaryAccession": "P12345",
            "features": []
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["signal_peptide"] == ""
        assert result["features_signal_peptide"] == "false"
        assert result["ubiquitination"] == ""

    def test_missing_features_key(self):
        """Test handling when features key is missing."""
        entry = {
            "primaryAccession": "P12345"
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["signal_peptide"] == ""
        assert result["features_signal_peptide"] == "false"
        assert result["ubiquitination"] == ""
