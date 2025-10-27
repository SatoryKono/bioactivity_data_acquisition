"""Graceful degradation strategies for critical API failures."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from library.clients.exceptions import ApiClientError
from library.logging_setup import get_logger


@dataclass
class DegradationConfig:
    """Configuration for graceful degradation."""
    
    enabled: bool = True
    max_retries: int = 3
    fallback_data_source: str | None = None
    cache_fallback: bool = True
    partial_data_acceptable: bool = True


class DegradationStrategy(ABC):
    """Abstract base class for degradation strategies."""
    
    def __init__(self, config: DegradationConfig):
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
    
    @abstractmethod
    def should_degrade(self, error: ApiClientError) -> bool:
        """Determine if we should degrade gracefully for this error."""
        pass
    
    @abstractmethod
    def get_fallback_data(self, original_request: dict[str, Any], error: ApiClientError) -> dict[str, Any]:
        """Get fallback data when degradation is needed."""
        pass
    
    @abstractmethod
    def get_degraded_response(self, partial_data: list[dict[str, Any]], error: ApiClientError) -> list[dict[str, Any]]:
        """Process partial data when some sources fail."""
        pass


class ChEMBLDegradationStrategy(DegradationStrategy):
    """Degradation strategy for ChEMBL API."""
    
    def should_degrade(self, error: ApiClientError) -> bool:
        """Degrade for ChEMBL-specific errors."""
        # Degrade for rate limiting, server errors, and timeouts
        if error.status_code in [429, 500, 502, 503, 504]:
            return True
        # Degrade for connection errors
        if "connection" in str(error).lower():
            return True
        return False
    
    def get_fallback_data(self, original_request: dict[str, Any], error: ApiClientError) -> dict[str, Any]:
        """Get fallback data for ChEMBL."""
        return {
            "source": "fallback",
            "api": "chembl",
            "error": str(error),
            "fallback_reason": "chembl_unavailable",
            "request_data": original_request,
            "degraded": True
        }
    
    def get_degraded_response(self, partial_data: list[dict[str, Any]], error: ApiClientError) -> list[dict[str, Any]]:
        """Process partial ChEMBL data."""
        # Add metadata about degradation
        for item in partial_data:
            item["degradation_info"] = {
                "source": "chembl",
                "partial": True,
                "error": str(error)
            }
        return partial_data


class CrossrefDegradationStrategy(DegradationStrategy):
    """Degradation strategy for Crossref API."""
    
    def should_degrade(self, error: ApiClientError) -> bool:
        """Degrade for Crossref-specific errors."""
        # Crossref is less critical, so degrade more aggressively
        if error.status_code in [401, 403, 429, 500, 502, 503, 504]:
            return True
        if "timeout" in str(error).lower():
            return True
        if "access denied" in str(error).lower():
            return True
        return False
    
    def get_fallback_data(self, original_request: dict[str, Any], error: ApiClientError) -> dict[str, Any]:
        """Get fallback data for Crossref."""
        # Determine the specific reason for fallback
        if error.status_code == 401:
            error_message = "Crossref API key not configured or invalid"
        elif error.status_code == 403:
            error_message = "Crossref API access forbidden"
        elif error.status_code == 429:
            error_message = "Crossref API rate limited"
        else:
            error_message = str(error)
        
        return {
            "source": "crossref",
            "doi_key": original_request.get("doi"),
            "crossref_doi": original_request.get("doi"),
            "crossref_title": None,
            "crossref_doc_type": None,
            "crossref_subject": None,
            "crossref_pmid": None,
            "crossref_abstract": None,
            "crossref_issn": None,
            "crossref_authors": None,
            "crossref_error": error_message,
            "degraded": True
        }
    
    def get_degraded_response(self, partial_data: list[dict[str, Any]], error: ApiClientError) -> list[dict[str, Any]]:
        """Process partial Crossref data."""
        for item in partial_data:
            item["degradation_info"] = {
                "source": "crossref",
                "partial": True,
                "error": str(error)
            }
        return partial_data


class OpenAlexDegradationStrategy(DegradationStrategy):
    """Degradation strategy for OpenAlex API."""
    
    def should_degrade(self, error: ApiClientError) -> bool:
        """Degrade for OpenAlex-specific errors."""
        # OpenAlex has moderate rate limits, degrade on rate limiting
        if error.status_code == 429:
            return True
        if error.status_code in [500, 502, 503, 504]:
            return True
        return False
    
    def get_fallback_data(self, original_request: dict[str, Any], error: ApiClientError) -> dict[str, Any]:
        """Return fallback data for OpenAlex."""
        return {
            "source": "fallback",
            "api": "openalex",
            "error": str(error),
            "fallback_reason": "openalex_unavailable",
            "openalex_doi": None,
            "openalex_title": None,
            "openalex_doc_type": None,
            "openalex_crossref_doc_type": None,
            "openalex_year": None,
            "openalex_pmid": None,
            "openalex_abstract": None,
            "openalex_issn": None,
            "openalex_authors": None,
            "openalex_journal": None,
            "openalex_volume": None,
            "openalex_issue": None,
            "openalex_first_page": None,
            "openalex_last_page": None,
            "openalex_error": str(error),
            "degraded": True
        }
    
    def get_degraded_response(self, partial_data: list[dict[str, Any]], error: ApiClientError) -> list[dict[str, Any]]:
        """Process partial OpenAlex data."""
        for item in partial_data:
            item["degradation_info"] = {
                "source": "openalex",
                "partial": True,
                "error": str(error)
            }
        return partial_data


class SemanticScholarDegradationStrategy(DegradationStrategy):
    """Degradation strategy for Semantic Scholar API."""
    
    def should_degrade(self, error: ApiClientError) -> bool:
        """Degrade for Semantic Scholar-specific errors."""
        # Semantic Scholar has strict rate limits, degrade quickly
        if error.status_code == 429:
            return True
        if error.status_code in [500, 502, 503, 504]:
            return True
        return False
    
    def get_fallback_data(self, original_request: dict[str, Any], error: ApiClientError) -> dict[str, Any]:
        """Get fallback data for Semantic Scholar."""
        return {
            "source": "fallback",
            "api": "semantic_scholar",
            "error": str(error),
            "fallback_reason": "semantic_scholar_unavailable",
            "paper_id": original_request.get("paper_id"),
            "degraded": True
        }
    
    def get_degraded_response(self, partial_data: list[dict[str, Any]], error: ApiClientError) -> list[dict[str, Any]]:
        """Process partial Semantic Scholar data."""
        for item in partial_data:
            item["degradation_info"] = {
                "source": "semantic_scholar",
                "partial": True,
                "error": str(error)
            }
        return partial_data


class GracefulDegradationManager:
    """Manages graceful degradation for multiple API clients."""
    
    def __init__(self):
        self.strategies: dict[str, DegradationStrategy] = {}
        self.logger = get_logger(self.__class__.__name__)
        self._setup_default_strategies()
    
    def _setup_default_strategies(self):
        """Setup default degradation strategies."""
        default_config = DegradationConfig()
        
        self.strategies["chembl"] = ChEMBLDegradationStrategy(default_config)
        self.strategies["crossref"] = CrossrefDegradationStrategy(default_config)
        self.strategies["openalex"] = OpenAlexDegradationStrategy(default_config)
        self.strategies["semantic_scholar"] = SemanticScholarDegradationStrategy(default_config)
    
    def register_strategy(self, api_name: str, strategy: DegradationStrategy):
        """Register a degradation strategy for an API."""
        self.strategies[api_name] = strategy
        # Экранируем символы % в сообщениях для безопасного логирования
        api_name_msg = str(api_name).replace("%", "%%")
        self.logger.info(f"Registered degradation strategy for {api_name_msg}")
    
    def should_degrade(self, api_name: str, error: ApiClientError) -> bool:
        """Check if we should degrade for this API and error."""
        if api_name not in self.strategies:
            return False
        
        strategy = self.strategies[api_name]
        return strategy.config.enabled and strategy.should_degrade(error)
    
    def get_fallback_data(self, api_name: str, original_request: dict[str, Any], error: ApiClientError) -> dict[str, Any]:
        """Get fallback data for an API."""
        if api_name not in self.strategies:
            return {
                "source": "fallback",
                "api": api_name,
                "error": str(error),
                "fallback_reason": "no_strategy",
                "degraded": True
            }
        
        return self.strategies[api_name].get_fallback_data(original_request, error)
    
    def get_degraded_response(self, api_name: str, partial_data: list[dict[str, Any]], error: ApiClientError) -> list[dict[str, Any]]:
        """Process partial data for an API."""
        if api_name not in self.strategies:
            return partial_data
        
        return self.strategies[api_name].get_degraded_response(partial_data, error)
    
    def get_degradation_summary(self) -> dict[str, Any]:
        """Get a summary of degradation configuration."""
        return {
            "enabled_strategies": list(self.strategies.keys()),
            "total_strategies": len(self.strategies),
            "configurations": {
                name: {
                    "enabled": strategy.config.enabled,
                    "max_retries": strategy.config.max_retries,
                    "cache_fallback": strategy.config.cache_fallback,
                    "partial_data_acceptable": strategy.config.partial_data_acceptable
                }
                for name, strategy in self.strategies.items()
            }
        }


# Global instance for easy access
_global_degradation_manager: GracefulDegradationManager | None = None


def get_degradation_manager() -> GracefulDegradationManager:
    """Get the global degradation manager instance."""
    global _global_degradation_manager
    if _global_degradation_manager is None:
        _global_degradation_manager = GracefulDegradationManager()
    return _global_degradation_manager


__all__ = [
    "DegradationConfig",
    "DegradationStrategy",
    "ChEMBLDegradationStrategy",
    "CrossrefDegradationStrategy", 
    "SemanticScholarDegradationStrategy",
    "GracefulDegradationManager",
    "get_degradation_manager"
]
