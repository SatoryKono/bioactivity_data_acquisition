"""HTTP client for Guide to Pharmacology (GtoPdb) API.

This module provides a client for interacting with the GtoPdb API to enrich
target data with information about natural ligands, interactions, and functions.
It includes circuit breaker functionality and rate limiting to handle API
constraints gracefully.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Any

import requests
from structlog import BoundLogger

from library.clients.base import BaseApiClient
from library.config import APIClientConfig


@dataclass
class CircuitBreaker:
    """Circuit breaker implementation for API protection."""
    
    failure_threshold: int = 5
    holdoff_seconds: int = 300
    _failures: int = 0
    _last_failure_time: float = 0
    _lock: threading.RLock = None
    
    def __post_init__(self) -> None:
        if self._lock is None:
            self._lock = threading.RLock()
    
    def record_success(self) -> None:
        """Record a successful request."""
        with self._lock:
            self._failures = 0
    
    def record_failure(self) -> bool:
        """Record a failed request and return True if circuit should open."""
        with self._lock:
            self._failures += 1
            self._last_failure_time = time.time()
            if self._failures >= self.failure_threshold:
                return True
            return False
    
    def is_open(self) -> bool:
        """Check if circuit breaker is open."""
        with self._lock:
            if self._failures < self.failure_threshold:
                return False
            return time.time() - self._last_failure_time < self.holdoff_seconds


@dataclass
class RateLimiter:
    """Rate limiter for API requests."""
    
    rps: float = 1.0  # Requests per second
    burst: int = 2
    _last_request_time: float = 0
    _lock: threading.RLock = None
    
    def __post_init__(self) -> None:
        if self._lock is None:
            self._lock = threading.RLock()
    
    def acquire(self) -> None:
        """Acquire permission to make a request."""
        with self._lock:
            now = time.time()
            time_since_last = now - self._last_request_time
            min_interval = 1.0 / self.rps
            
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                time.sleep(sleep_time)
            
            self._last_request_time = time.time()


class GtoPdbClient(BaseApiClient):
    """Client for Guide to Pharmacology API."""
    
    def __init__(
        self,
        config: APIClientConfig,
        logger: BoundLogger | None = None,
        circuit_breaker_config: dict[str, Any] | None = None,
        rate_limit_config: dict[str, Any] | None = None,
        **kwargs: Any
    ) -> None:
        """Initialize GtoPdb client.
        
        Args:
            config: API client configuration
            logger: Structured logger instance
            circuit_breaker_config: Circuit breaker configuration
            rate_limit_config: Rate limiting configuration
            **kwargs: Additional arguments passed to base client
        """
        super().__init__(config, **kwargs)
        self.logger = logger or self.logger
        
        # Initialize circuit breaker and rate limiter with provided configs
        cb_config = circuit_breaker_config or {}
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=cb_config.get('failure_threshold', 5),
            holdoff_seconds=cb_config.get('holdoff_seconds', 300)
        )
        
        rl_config = rate_limit_config or {}
        self.rate_limiter = RateLimiter(
            rps=rl_config.get('rps', 1.0),
            burst=rl_config.get('burst', 2)
        )
        
        # Cache for failed requests to avoid repeated failures
        self._failed_cache: set[str] = set()
        self._cache_lock = threading.RLock()
    
    def _build_url(self, gtop_id: str, endpoint: str) -> str:
        """Build URL for GtoPdb API endpoint.
        
        Args:
            gtop_id: Guide to Pharmacology target ID
            endpoint: API endpoint name
            
        Returns:
            Complete URL for the API request
        """
        base_url = str(self.config.base_url).rstrip('/')
        return f"{base_url}/{endpoint}/{gtop_id}"
    
    def _fetch_endpoint(self, gtop_id: str, endpoint: str) -> list[dict[str, Any]] | None:
        """Fetch data from a specific GtoPdb endpoint.
        
        Args:
            gtop_id: Guide to Pharmacology target ID
            endpoint: API endpoint name
            
        Returns:
            List of data items or None if request failed
        """
        # Check circuit breaker
        if self.circuit_breaker.is_open():
            self.logger.warning(
                "gtop_circuit_open",
                extra={
                    "gtop_id": gtop_id,
                    "endpoint": endpoint,
                    "failures": self.circuit_breaker._failures
                }
            )
            return None
        
        # Check failed cache
        cache_key = f"{gtop_id}:{endpoint}"
        with self._cache_lock:
            if cache_key in self._failed_cache:
                self.logger.debug(
                    "gtop_cached_failure",
                    extra={"gtop_id": gtop_id, "endpoint": endpoint}
                )
                return None
        
        # Apply rate limiting
        self.rate_limiter.acquire()
        
        url = self._build_url(gtop_id, endpoint)
        
        try:
            response = self.session.get(url, timeout=self.config.timeout)
            
            # Check for empty response
            content_type = response.headers.get("Content-Type", "")
            if response.headers.get("Content-Length") == "0" or not response.text.strip():
                self.logger.info(
                    "gtop_empty_response",
                    extra={
                        "gtop_id": gtop_id,
                        "endpoint": endpoint,
                        "content_type": content_type
                    }
                )
                self.circuit_breaker.record_success()
                return []
            
            # Validate JSON response
            if "application/json" not in content_type.lower():
                self.logger.warning(
                    "gtop_non_json_response",
                    extra={
                        "gtop_id": gtop_id,
                        "endpoint": endpoint,
                        "content_type": content_type
                    }
                )
            
            response.raise_for_status()
            data = response.json()
            
            # Record success
            self.circuit_breaker.record_success()
            
            return data if isinstance(data, list) else []
            
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            
            # Handle 404 as expected (no data available)
            if status_code == 404:
                self.logger.info(
                    "gtop_endpoint_missing",
                    extra={
                        "gtop_id": gtop_id,
                        "endpoint": endpoint,
                        "status_code": status_code
                    }
                )
                self.circuit_breaker.record_success()
                with self._cache_lock:
                    self._failed_cache.add(cache_key)
                return None
            
            # Log error and record failure
            self.logger.warning(
                "gtop_request_failed",
                extra={
                    "gtop_id": gtop_id,
                    "endpoint": endpoint,
                    "error": str(exc),
                    "status_code": status_code
                }
            )
            
            # Record failure in circuit breaker
            if status_code is None or status_code >= 500:
                circuit_opened = self.circuit_breaker.record_failure()
                if circuit_opened:
                    self.logger.warning(
                        "gtop_circuit_opened",
                        extra={
                            "gtop_id": gtop_id,
                            "endpoint": endpoint,
                            "holdoff_seconds": self.circuit_breaker.holdoff_seconds,
                            "failure_threshold": self.circuit_breaker.failure_threshold
                        }
                    )
            
            # Cache the failure
            with self._cache_lock:
                self._failed_cache.add(cache_key)
            
            return None
        
        except (json.JSONDecodeError, ValueError) as exc:
            self.logger.warning(
                "gtop_json_decode_failed",
                extra={
                    "gtop_id": gtop_id,
                    "endpoint": endpoint,
                    "error": str(exc)
                }
            )
            self.circuit_breaker.record_failure()
            with self._cache_lock:
                self._failed_cache.add(cache_key)
            return None
    
    def fetch_natural_ligands(self, gtop_id: str) -> list[dict[str, Any]]:
        """Fetch natural ligands for a GtoPdb target.
        
        Args:
            gtop_id: Guide to Pharmacology target ID
            
        Returns:
            List of natural ligand data
        """
        return self._fetch_endpoint(gtop_id, "naturalLigands") or []
    
    def fetch_interactions(self, gtop_id: str) -> list[dict[str, Any]]:
        """Fetch interactions for a GtoPdb target.
        
        Args:
            gtop_id: Guide to Pharmacology target ID
            
        Returns:
            List of interaction data
        """
        return self._fetch_endpoint(gtop_id, "interactions") or []
    
    def fetch_function(self, gtop_id: str) -> list[dict[str, Any]]:
        """Fetch function information for a GtoPdb target.
        
        Args:
            gtop_id: Guide to Pharmacology target ID
            
        Returns:
            List of function data
        """
        return self._fetch_endpoint(gtop_id, "function") or []
    
    def summarize_function(self, function_data: list[dict[str, Any]]) -> str:
        """Extract a concise textual summary from function data.
        
        Args:
            function_data: List of function entries from GtoPdb API
            
        Returns:
            Concise text summary or empty string
        """
        if not function_data:
            return ""
        
        for entry in function_data:
            if not isinstance(entry, dict):
                continue
            
            # Try to extract meaningful description
            description = entry.get("description", "")
            property_text = entry.get("property", "")
            
            if property_text and isinstance(property_text, str):
                property_text = property_text.strip()
                if property_text:
                    if description and isinstance(description, str):
                        description = description.strip()
                        if description:
                            return f"{description}: {property_text}"
                    return property_text
            
            if description and isinstance(description, str):
                description = description.strip()
                if description:
                    return description
            
            # Fallback to tissue information
            tissue = entry.get("tissue", "")
            if tissue and isinstance(tissue, str):
                tissue = tissue.strip()
                if tissue:
                    return tissue
        
        return ""
