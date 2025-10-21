"""Unit tests for UniProt adapter functionality."""

from src.library.target.uniprot_adapter import _collect_ec_numbers, _parse_uniprot_entry


class TestCollectEcNumbers:
    """Test cases for _collect_ec_numbers function."""

    def test_ec_number_as_string(self):
        """Test EC number extraction when provided as a string."""
        reaction_obj = {"ecNumber": "1.1.1.1"}
        result = _collect_ec_numbers(reaction_obj)
        assert result == ["1.1.1.1"]

    def test_ec_number_as_dict_with_value(self):
        """Test EC number extraction when provided as dict with value."""
        reaction_obj = {"ecNumber": {"value": "2.3.4.5"}}
        result = _collect_ec_numbers(reaction_obj)
        assert result == ["2.3.4.5"]

    def test_ec_numbers_as_list_of_strings(self):
        """Test EC numbers extraction when provided as list of strings."""
        reaction_obj = {"ecNumbers": ["1.1.1.1", "2.2.2.2"]}
        result = _collect_ec_numbers(reaction_obj)
        assert result == ["1.1.1.1", "2.2.2.2"]

    def test_ec_numbers_as_list_of_dicts(self):
        """Test EC numbers extraction when provided as list of dicts with value."""
        reaction_obj = {
            "ecNumbers": [
                {"value": "1.1.1.1"},
                {"value": "2.2.2.2"}
            ]
        }
        result = _collect_ec_numbers(reaction_obj)
        assert result == ["1.1.1.1", "2.2.2.2"]

    def test_mixed_ec_numbers_format(self):
        """Test EC numbers extraction with mixed formats in list."""
        reaction_obj = {
            "ecNumbers": [
                "1.1.1.1",
                {"value": "2.2.2.2"},
                "3.3.3.3"
            ]
        }
        result = _collect_ec_numbers(reaction_obj)
        assert result == ["1.1.1.1", "2.2.2.2", "3.3.3.3"]

    def test_ec_numbers_priority_over_ec_number(self):
        """Test that ecNumbers takes priority over ecNumber."""
        reaction_obj = {
            "ecNumber": "1.1.1.1",
            "ecNumbers": ["2.2.2.2", "3.3.3.3"]
        }
        result = _collect_ec_numbers(reaction_obj)
        assert result == ["2.2.2.2", "3.3.3.3"]

    def test_empty_values(self):
        """Test handling of empty or None values."""
        # Empty string
        reaction_obj = {"ecNumber": ""}
        result = _collect_ec_numbers(reaction_obj)
        assert result == []

        # None value
        reaction_obj = {"ecNumber": None}
        result = _collect_ec_numbers(reaction_obj)
        assert result == []

        # Empty dict
        reaction_obj = {"ecNumber": {}}
        result = _collect_ec_numbers(reaction_obj)
        assert result == []

        # Empty list
        reaction_obj = {"ecNumbers": []}
        result = _collect_ec_numbers(reaction_obj)
        assert result == []

    def test_invalid_input(self):
        """Test handling of invalid input types."""
        # Not a dict
        result = _collect_ec_numbers("not a dict")
        assert result == []

        result = _collect_ec_numbers(None)
        assert result == []

        result = _collect_ec_numbers([])
        assert result == []

    def test_missing_ec_fields(self):
        """Test handling when neither ecNumber nor ecNumbers are present."""
        reaction_obj = {"name": "Some reaction"}
        result = _collect_ec_numbers(reaction_obj)
        assert result == []


class TestParseUniprotEntry:
    """Test cases for _parse_uniprot_entry function."""

    def test_catalytic_activity_with_ec_numbers(self):
        """Test parsing entry with CATALYTIC ACTIVITY comments containing EC numbers."""
        entry = {
            "primaryAccession": "P12345",
            "comments": [
                {
                    "commentType": "CATALYTIC ACTIVITY",
                    "reaction": {
                        "name": {"value": "ATP + H2O = ADP + phosphate"},
                        "ecNumber": "3.6.1.3"
                    }
                },
                {
                    "commentType": "CATALYTIC ACTIVITY", 
                    "reaction": {
                        "name": {"value": "Another reaction"},
                        "ecNumbers": ["1.1.1.1", "2.2.2.2"]
                    }
                }
            ],
            "entryAudit": {
                "lastAnnotationUpdateDate": "2023-01-01",
                "sequenceVersion": 1
            }
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["reactions"] == "ATP + H2O = ADP + phosphate|Another reaction"
        assert result["reaction_ec_numbers"] == "3.6.1.3|1.1.1.1|2.2.2.2"
        assert result["uniprot_id_primary"] == "P12345"

    def test_catalytic_activity_without_ec_numbers(self):
        """Test parsing entry with CATALYTIC ACTIVITY comments without EC numbers."""
        entry = {
            "primaryAccession": "P12345",
            "comments": [
                {
                    "commentType": "CATALYTIC ACTIVITY",
                    "reaction": {
                        "name": {"value": "ATP + H2O = ADP + phosphate"}
                    }
                }
            ],
            "entryAudit": {
                "lastAnnotationUpdateDate": "2023-01-01",
                "sequenceVersion": 1
            }
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["reactions"] == "ATP + H2O = ADP + phosphate"
        assert result["reaction_ec_numbers"] == ""

    def test_no_catalytic_activity_comments(self):
        """Test parsing entry without CATALYTIC ACTIVITY comments."""
        entry = {
            "primaryAccession": "P12345",
            "comments": [
                {
                    "commentType": "FUNCTION",
                    "text": [{"value": "Some function description"}]
                }
            ],
            "entryAudit": {
                "lastAnnotationUpdateDate": "2023-01-01",
                "sequenceVersion": 1
            }
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["reactions"] == ""
        assert result["reaction_ec_numbers"] == ""

    def test_empty_comments(self):
        """Test parsing entry with empty comments list."""
        entry = {
            "primaryAccession": "P12345",
            "comments": [],
            "entryAudit": {
                "lastAnnotationUpdateDate": "2023-01-01",
                "sequenceVersion": 1
            }
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["reactions"] == ""
        assert result["reaction_ec_numbers"] == ""

    def test_missing_comments(self):
        """Test parsing entry without comments field."""
        entry = {
            "primaryAccession": "P12345",
            "entryAudit": {
                "lastAnnotationUpdateDate": "2023-01-01",
                "sequenceVersion": 1
            }
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["reactions"] == ""
        assert result["reaction_ec_numbers"] == ""

    def test_reaction_name_as_string(self):
        """Test parsing when reaction name is provided as string instead of dict."""
        entry = {
            "primaryAccession": "P12345",
            "comments": [
                {
                    "commentType": "CATALYTIC ACTIVITY",
                    "reaction": {
                        "name": "ATP + H2O = ADP + phosphate",
                        "ecNumber": "3.6.1.3"
                    }
                }
            ],
            "entryAudit": {
                "lastAnnotationUpdateDate": "2023-01-01",
                "sequenceVersion": 1
            }
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["reactions"] == "ATP + H2O = ADP + phosphate"
        assert result["reaction_ec_numbers"] == "3.6.1.3"

    def test_complex_ec_numbers_structure(self):
        """Test parsing with complex EC numbers structure."""
        entry = {
            "primaryAccession": "P12345",
            "comments": [
                {
                    "commentType": "CATALYTIC ACTIVITY",
                    "reaction": {
                        "name": {"value": "Complex reaction"},
                        "ecNumbers": [
                            "1.1.1.1",
                            {"value": "2.2.2.2"},
                            "3.3.3.3"
                        ]
                    }
                }
            ],
            "entryAudit": {
                "lastAnnotationUpdateDate": "2023-01-01",
                "sequenceVersion": 1
            }
        }
        
        result = _parse_uniprot_entry(entry)
        
        assert result["reactions"] == "Complex reaction"
        assert result["reaction_ec_numbers"] == "1.1.1.1|2.2.2.2|3.3.3.3"
