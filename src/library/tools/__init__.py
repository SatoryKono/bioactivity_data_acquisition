"""Scripts for bioactivity data acquisition.

This module contains utility scripts for:
- API health checking and monitoring
- API key management
- Configuration management
- Rate limiting testing
- Citation formatting
- Journal name normalization
"""

# from .check_semantic_scholar_status import main as check_semantic_scholar_status  # File not found
from .citation_formatter import add_citation_column, format_citation

# from .monitor_pubmed import main as monitor_pubmed  # File not found
# from .monitor_semantic_scholar import main as monitor_semantic_scholar  # File not found
# from .quick_api_check import main as quick_api_check  # File not found
# from .toggle_semantic_scholar import main as toggle_semantic_scholar  # File not found
from .data_validator import (
    validate_all_fields,
    validate_doi_fields,
    validate_issue_fields,
    validate_journal_fields,
    validate_volume_fields,
    validate_year_fields,
)
from .journal_normalizer import (
    get_journal_columns,
    normalize_journal_columns,
    normalize_journal_name,
)

__all__ = [
    # "check_semantic_scholar_status",  # File not found
    "format_citation",
    "add_citation_column",
    "normalize_journal_name",
    "normalize_journal_columns",
    "get_journal_columns",
    # "monitor_pubmed",  # File not found
    # "monitor_semantic_scholar",  # File not found
    # "quick_api_check",  # File not found
    # "toggle_semantic_scholar",  # File not found
    "validate_all_fields",
    "validate_doi_fields",
    "validate_journal_fields",
    "validate_year_fields",
    "validate_volume_fields",
    "validate_issue_fields",
]
