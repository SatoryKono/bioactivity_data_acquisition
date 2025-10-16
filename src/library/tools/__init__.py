"""Scripts for bioactivity data acquisition.

This module contains utility scripts for:
- API health checking and monitoring
- API key management
- Configuration management
- Rate limiting testing
- Citation formatting
- Journal name normalization
"""

from .api_health_check import main as api_health_check
from .check_api_limits import main as check_api_limits
from .check_semantic_scholar_status import main as check_semantic_scholar_status
from .check_specific_limits import main as check_specific_limits
from .citation_formatter import format_citation, add_citation_column
from .journal_normalizer import (
    normalize_journal_name,
    normalize_journal_columns,
    get_journal_columns,
)
from .get_pubmed_api_key import main as get_pubmed_api_key
from .get_semantic_scholar_api_key import main as get_semantic_scholar_api_key
from .monitor_api import main as monitor_api
from .monitor_pubmed import main as monitor_pubmed
from .monitor_semantic_scholar import main as monitor_semantic_scholar
from .quick_api_check import main as quick_api_check
from .toggle_semantic_scholar import main as toggle_semantic_scholar
from .data_validator import (
    validate_all_fields,
    validate_doi_fields,
    validate_journal_fields,
    validate_year_fields,
    validate_volume_fields,
    validate_issue_fields,
)

__all__ = [
    "api_health_check",
    "check_api_limits", 
    "check_semantic_scholar_status",
    "check_specific_limits",
    "format_citation",
    "add_citation_column",
    "normalize_journal_name",
    "normalize_journal_columns",
    "get_journal_columns",
    "get_pubmed_api_key",
    "get_semantic_scholar_api_key",
    "monitor_api",
    "monitor_pubmed",
    "monitor_semantic_scholar",
    "quick_api_check",
    "toggle_semantic_scholar",
    "validate_all_fields",
    "validate_doi_fields",
    "validate_journal_fields",
    "validate_year_fields",
    "validate_volume_fields",
    "validate_issue_fields",
]
