"""Fallback strategies for handling API rate limiting and errors."""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from library.clients.exceptions import ApiClientError
from library.logger import get_logger


@dataclass
class FallbackConfig:
    """Configuration for fallback strategies."""
    
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True


class FallbackStrategy(ABC):
    """Abstract base class for fallback strategies."""
    
    def __init__(self, config: FallbackConfig):
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
    
    @abstractmethod
    def should_retry(self, error: ApiClientError, attempt: int) -> bool:
        """Determine if we should retry after this error."""
        pass
    
    @abstractmethod
    def get_delay(self, error: ApiClientError, attempt: int) -> float:
        """Calculate delay before next retry."""
        pass
    
    @abstractmethod
    def get_fallback_data(self, error: ApiClientError) -> dict[str, Any]:
        """Get fallback data when all retries are exhausted."""
        pass


class RateLimitFallbackStrategy(FallbackStrategy):
    """Fallback strategy specifically for rate limiting errors (429)."""
    
    def should_retry(self, error: ApiClientError, attempt: int) -> bool:
        """Retry for rate limiting errors up to max_retries."""
        if error.status_code == 429:
            return attempt < self.config.max_retries
        return False
    
    def get_delay(self, error: ApiClientError, attempt: int) -> float:
        """Calculate exponential backoff delay with jitter."""
        if error.status_code == 429:
            # Exponential backoff with jitter
            delay = min(
                self.config.base_delay * (self.config.backoff_multiplier ** attempt),
                self.config.max_delay
            )
            
            if self.config.jitter:
                # Add random jitter (±25%)
                import random  # noqa: S311
                jitter_factor = random.uniform(0.75, 1.25)
                delay *= jitter_factor
            
            return delay
        
        return self.config.base_delay
    
    def get_fallback_data(self, error: ApiClientError) -> dict[str, Any]:
        """Return empty record with error information."""
        return {
            "source": "fallback",
            "error": f"Rate limited after {self.config.max_retries} attempts: {str(error)}",
            "rate_limited": True,
            "retry_count": self.config.max_retries
        }


class SemanticScholarFallbackStrategy(FallbackStrategy):
    """Specialized fallback strategy for Semantic Scholar API with very strict rate limits."""
    
    def should_retry(self, error: ApiClientError, attempt: int) -> bool:
        """Very conservative retry logic for Semantic Scholar."""
        if error.status_code == 429:
            # For rate limiting, only retry once with very long delay
            return attempt < 1
        elif error.status_code in [500, 502, 503, 504]:
            # For server errors, retry once
            return attempt < 1
        else:
            # For other errors, don't retry
            return False
    
    def get_delay(self, error: ApiClientError, attempt: int) -> float:
        """Very long delays for Semantic Scholar rate limiting."""
        if error.status_code == 429:
            # Very long delay for rate limiting (2-5 minutes)
            # Увеличиваем базовую задержку для Semantic Scholar
            delay = min(180.0 + (attempt * 120.0), 600.0)  # 3-10 минут
        elif error.status_code in [500, 502, 503, 504]:
            # Moderate delay for server errors
            delay = min(30.0 + (attempt * 15.0), 120.0)
        else:
            # Standard delay
            delay = self.config.base_delay
        
        if self.config.jitter:
            import random  # noqa: S311
            jitter_factor = random.uniform(0.8, 1.2)  # Больше вариативности
            delay *= jitter_factor
        
        return delay
    
    def get_fallback_data(self, error: ApiClientError) -> dict[str, Any]:
        """Return fallback data for Semantic Scholar."""
        return {
            "source": "fallback",
            "fallback_used": True,
            "error": str(error),
            "fallback_reason": f"Semantic Scholar API error: {error.status_code}",
            "retry_count": 1
        }


class AdaptiveFallbackStrategy(FallbackStrategy):
    """Adaptive fallback strategy that learns from API behavior."""
    
    def __init__(self, config: FallbackConfig):
        super().__init__(config)
        self.api_behavior = {}  # Track API behavior per endpoint
    
    def should_retry(self, error: ApiClientError, attempt: int) -> bool:
        """Adaptive retry logic based on API behavior."""
        if error.status_code == 429:
            # For rate limiting, be more conservative
            return attempt < min(self.config.max_retries, 5)
        elif error.status_code in [500, 502, 503, 504]:
            # For server errors, retry more aggressively
            return attempt < self.config.max_retries * 2
        else:
            # For other errors, use standard retry logic
            return attempt < self.config.max_retries
    
    def get_delay(self, error: ApiClientError, attempt: int) -> float:
        """Adaptive delay calculation."""
        if error.status_code == 429:
            # Longer delays for rate limiting
            delay = min(
                self.config.base_delay * (self.config.backoff_multiplier ** attempt) * 2,
                self.config.max_delay
            )
        elif error.status_code in [500, 502, 503, 504]:
            # Shorter delays for server errors
            delay = min(
                self.config.base_delay * (self.config.backoff_multiplier ** attempt) * 0.5,
                self.config.max_delay
            )
        else:
            # Standard delay
            delay = min(
                self.config.base_delay * (self.config.backoff_multiplier ** attempt),
                self.config.max_delay
            )
        
        if self.config.jitter:
            import random  # noqa: S311
            jitter_factor = random.uniform(0.8, 1.2)
            delay *= jitter_factor
        
        return delay
    
    def get_fallback_data(self, error: ApiClientError) -> dict[str, Any]:
        """Return appropriate fallback data based on error type."""
        if error.status_code == 429:
            return {
                "source": "fallback",
                "error": f"Rate limited: {str(error)}",
                "rate_limited": True,
                "fallback_reason": "api_rate_limit"
            }
        elif error.status_code in [500, 502, 503, 504]:
            return {
                "source": "fallback",
                "error": f"Server error: {str(error)}",
                "server_error": True,
                "fallback_reason": "server_unavailable"
            }
        else:
            return {
                "source": "fallback",
                "error": f"API error: {str(error)}",
                "api_error": True,
                "fallback_reason": "unknown_error"
            }


class FallbackManager:
    """Manages fallback strategies for API clients."""
    
    def __init__(self, strategy: FallbackStrategy):
        self.strategy = strategy
        self.logger = get_logger(self.__class__.__name__)
    
    def execute_with_fallback(self, func, *args, **kwargs) -> dict[str, Any]:
        """Execute function with fallback strategy."""
        last_error = None
        
        for attempt in range(self.strategy.config.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    self.logger.info(f"Success after {attempt} retries")
                return result
            except ApiClientError as e:
                last_error = e
                
                if not self.strategy.should_retry(e, attempt):
                    self.logger.warning(
                    f"Max retries reached for error: {e}"
                )
                    break
                
                delay = self.strategy.get_delay(e, attempt)
                self.logger.info(f"Retry {attempt + 1}/{self.strategy.config.max_retries} after {delay:.2f}s delay")
                time.sleep(delay)
        
        # All retries exhausted, return fallback data
        self.logger.warning(
            f"Using fallback data after {self.strategy.config.max_retries} failed attempts"
        )
        return self.strategy.get_fallback_data(last_error)


__all__ = [
    "FallbackConfig",
    "FallbackStrategy", 
    "RateLimitFallbackStrategy",
    "SemanticScholarFallbackStrategy",
    "AdaptiveFallbackStrategy",
    "FallbackManager"
]
