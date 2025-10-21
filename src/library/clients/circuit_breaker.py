"""Circuit breaker implementation for API clients."""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from library.clients.exceptions import ApiClientError
from library.logging_setup import get_logger


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit is open, failing fast
    HALF_OPEN = "half_open"  # Testing if service is back


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    
    failure_threshold: int = 5  # Number of failures before opening circuit
    recovery_timeout: float = 60.0  # Time to wait before trying half-open
    success_threshold: int = 3  # Number of successes needed to close circuit
    timeout: float = 30.0  # Request timeout
    expected_exception: type[Exception] = ApiClientError  # Exception type to track


class CircuitBreaker:
    """Circuit breaker implementation for API reliability."""
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        
        # State management
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._lock = threading.Lock()
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state
    
    def call(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    self.logger.info("Circuit breaker transitioning to HALF_OPEN")
                else:
                    raise ApiClientError(
                        f"Circuit breaker is OPEN. Service unavailable. "
                        f"Last failure: {self._last_failure_time}"
                    )
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.config.expected_exception:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset the circuit."""
        if self._last_failure_time is None:
            return True
        
        return time.time() - self._last_failure_time >= self.config.recovery_timeout
    
    def _on_success(self):
        """Handle successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self.logger.info("Circuit breaker transitioning to CLOSED")
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0
    
    def _on_failure(self):
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                # Any failure in half-open state opens the circuit
                self._state = CircuitState.OPEN
                self.logger.warning("Circuit breaker transitioning to OPEN from HALF_OPEN")
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    self._state = CircuitState.OPEN
                    self.logger.warning(
                        f"Circuit breaker transitioning to OPEN after {self._failure_count} failures"
                    )
    
    def reset(self):
        """Manually reset the circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            self.logger.info("Circuit breaker manually reset to CLOSED")
    
    def get_metrics(self) -> dict[str, Any]:
        """Get circuit breaker metrics."""
        with self._lock:
            return {
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
                "is_healthy": self._state == CircuitState.CLOSED
            }


class APICircuitBreaker(CircuitBreaker):
    """Specialized circuit breaker for API clients."""
    
    def __init__(self, api_name: str, config: CircuitBreakerConfig | None = None):
        if config is None:
            # API-specific configurations
            if "semanticscholar" in api_name.lower():
                config = CircuitBreakerConfig(
                    failure_threshold=3,  # More sensitive for Semantic Scholar
                    recovery_timeout=300.0,  # 5 minutes
                    success_threshold=2
                )
            elif "pubmed" in api_name.lower():
                config = CircuitBreakerConfig(
                    failure_threshold=5,
                    recovery_timeout=120.0,  # 2 minutes
                    success_threshold=3
                )
            else:
                config = CircuitBreakerConfig()  # Default config
        
        super().__init__(config)
        self.api_name = api_name
        self.logger = get_logger(f"{self.__class__.__name__}.{api_name}")
    
    def call(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Execute function with API-specific circuit breaker protection."""
        try:
            return super().call(func, *args, **kwargs)
        except ApiClientError as e:
            # Add API context to error
            e.api_name = self.api_name
            e.circuit_state = self._state.value
            raise


__all__ = [
    "CircuitState",
    "CircuitBreakerConfig", 
    "CircuitBreaker",
    "APICircuitBreaker"
]
