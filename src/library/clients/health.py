"""Health check functionality for API clients."""

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Literal
from enum import Enum

import typer
from rich.console import Console
from rich.table import Table

import requests
from library.clients.circuit_breaker import CircuitState
from library.config import APIClientConfig
from library.logging_setup import get_logger


class HealthCheckStrategy(Enum):
    """Strategy for health checking."""
    BASE_URL = "base_url"  # Check base URL directly
    CUSTOM_ENDPOINT = "custom_endpoint"  # Check custom health endpoint
    DEFAULT_HEALTH = "default_health"  # Check /health endpoint (legacy)


class SimpleHealthClient:
    """Simple client for health checking."""
    
    def __init__(self, config: APIClientConfig):
        self.config = config
        self.base_url = str(config.base_url)
        self.session = requests.Session()
        self.session.headers.update(config.headers)
    
    def _make_url(self, path: str = "") -> str:
        """Make a URL from base URL and path."""
        if path:
            return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        return self.base_url
    
    def get_health_check_url(self) -> str:
        """Get the URL to use for health checking based on configuration."""
        if self.config.health_endpoint:
            return self._make_url(self.config.health_endpoint)
        else:
            # Use base URL for health checks (more reliable for external APIs)
            return self.base_url
    
    def get_health_check_strategy(self) -> HealthCheckStrategy:
        """Determine the health check strategy based on configuration."""
        if self.config.health_endpoint:
            return HealthCheckStrategy.CUSTOM_ENDPOINT
        else:
            return HealthCheckStrategy.BASE_URL


@dataclass
class HealthStatus:
    """Health status for an API client."""
    
    name: str
    is_healthy: bool
    response_time_ms: Optional[float] = None
    error_message: Optional[str] = None
    circuit_state: Optional[str] = None
    last_check: Optional[float] = None


class HealthChecker:
    """Health checker for API clients."""
    
    def __init__(self, clients: Dict[str, Any]):
        self.clients = clients
        self.logger = get_logger(self.__class__.__name__)
        self.console = Console()
    
    def check_all(self, timeout: float = 10.0) -> List[HealthStatus]:
        """Check health of all registered clients."""
        results = []
        
        for name, client in self.clients.items():
            try:
                start_time = time.time()
                status = self._check_client_health(client, name, timeout)
                status.last_check = time.time()
                results.append(status)
            except Exception as e:
                self.logger.error(f"Health check failed for {name}: {e}")
                results.append(HealthStatus(
                    name=name,
                    is_healthy=False,
                    error_message=str(e),
                    last_check=time.time()
                ))
        
        return results
    
    def _check_client_health(self, client: Any, name: str, timeout: float) -> HealthStatus:
        """Check health of a single client."""
        start_time = time.time()
        
        try:
            # Get circuit breaker state
            circuit_state = None
            if hasattr(client, 'circuit_breaker'):
                circuit_state = client.circuit_breaker.state.value
                if circuit_state == CircuitState.OPEN.value:
                    return HealthStatus(
                        name=name,
                        is_healthy=False,
                        circuit_state=circuit_state,
                        error_message="Circuit breaker is OPEN"
                    )
            
            # Determine health check strategy and URL
            if hasattr(client, 'get_health_check_url'):
                test_url = client.get_health_check_url()
                strategy = client.get_health_check_strategy()
            else:
                # Fallback for legacy clients
                test_url = client._make_url("health") if hasattr(client, '_make_url') else None
                strategy = HealthCheckStrategy.DEFAULT_HEALTH
            
            if test_url:
                # Make a simple HEAD request to check connectivity
                response = client.session.head(test_url, timeout=timeout)
                response_time_ms = (time.time() - start_time) * 1000
                
                # For external APIs, 404 might be acceptable if the base URL is reachable
                if strategy == HealthCheckStrategy.BASE_URL:
                    # For base URL checks, any response (including 404) indicates the service is reachable
                    if response.status_code in [200, 404, 405]:  # 405 = Method Not Allowed (common for HEAD)
                        return HealthStatus(
                            name=name,
                            is_healthy=True,
                            response_time_ms=response_time_ms,
                            circuit_state=circuit_state
                        )
                    elif response.status_code < 500:  # 4xx errors (except 404) might indicate issues
                        return HealthStatus(
                            name=name,
                            is_healthy=False,
                            response_time_ms=response_time_ms,
                            circuit_state=circuit_state,
                            error_message=f"HTTP {response.status_code}"
                        )
                    else:  # 5xx errors indicate server issues
                        return HealthStatus(
                            name=name,
                            is_healthy=False,
                            response_time_ms=response_time_ms,
                            circuit_state=circuit_state,
                            error_message=f"HTTP {response.status_code}"
                        )
                else:
                    # For custom endpoints, expect 200-299 for healthy status
                    if response.status_code < 400:
                        return HealthStatus(
                            name=name,
                            is_healthy=True,
                            response_time_ms=response_time_ms,
                            circuit_state=circuit_state
                        )
                    else:
                        return HealthStatus(
                            name=name,
                            is_healthy=False,
                            response_time_ms=response_time_ms,
                            circuit_state=circuit_state,
                            error_message=f"HTTP {response.status_code}"
                        )
            
            # If no specific health endpoint, just check if client is properly configured
            if hasattr(client, 'base_url') and client.base_url:
                response_time_ms = (time.time() - start_time) * 1000
                return HealthStatus(
                    name=name,
                    is_healthy=True,
                    response_time_ms=response_time_ms,
                    circuit_state=circuit_state
                )
            else:
                return HealthStatus(
                    name=name,
                    is_healthy=False,
                    circuit_state=circuit_state,
                    error_message="Client not properly configured"
                )
                
        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return HealthStatus(
                name=name,
                is_healthy=False,
                response_time_ms=response_time_ms,
                circuit_state=circuit_state,
                error_message=str(e)
            )
    
    def print_health_report(self, statuses: List[HealthStatus]) -> None:
        """Print a formatted health report."""
        table = Table(title="API Health Status")
        table.add_column("API", style="cyan", no_wrap=True)
        table.add_column("Status", justify="center")
        table.add_column("Response Time", justify="right")
        table.add_column("Circuit State", justify="center")
        table.add_column("Error", style="red")
        
        for status in statuses:
            # Status column
            if status.is_healthy:
                status_text = "[green]✓ Healthy[/green]"
            else:
                status_text = "[red]✗ Unhealthy[/red]"
            
            # Response time column
            if status.response_time_ms is not None:
                response_time = f"{status.response_time_ms:.1f}ms"
            else:
                response_time = "N/A"
            
            # Circuit state column
            circuit_state = status.circuit_state or "N/A"
            if circuit_state == "open":
                circuit_state = f"[red]{circuit_state}[/red]"
            elif circuit_state == "half_open":
                circuit_state = f"[yellow]{circuit_state}[/yellow]"
            elif circuit_state == "closed":
                circuit_state = f"[green]{circuit_state}[/green]"
            
            # Error column
            error = status.error_message or ""
            
            table.add_row(
                status.name,
                status_text,
                response_time,
                circuit_state,
                error
            )
        
        self.console.print(table)
        
        # Summary
        healthy_count = sum(1 for s in statuses if s.is_healthy)
        total_count = len(statuses)
        
        if healthy_count == total_count:
            self.console.print(f"\n[green]All {total_count} APIs are healthy![/green]")
        else:
            unhealthy_count = total_count - healthy_count
            self.console.print(f"\n[yellow]{unhealthy_count} of {total_count} APIs are unhealthy[/yellow]")
    
    def get_health_summary(self, statuses: List[HealthStatus]) -> Dict[str, Any]:
        """Get a summary of health status."""
        healthy_count = sum(1 for s in statuses if s.is_healthy)
        total_count = len(statuses)
        
        # Calculate average response time
        response_times = [s.response_time_ms for s in statuses if s.response_time_ms is not None]
        avg_response_time = sum(response_times) / len(response_times) if response_times else None
        
        # Count circuit breaker states
        circuit_states: dict[CircuitState, int] = {}
        for status in statuses:
            if status.circuit_state:
                circuit_states[status.circuit_state] = circuit_states.get(status.circuit_state, 0) + 1
        
        return {
            "total_apis": total_count,
            "healthy_apis": healthy_count,
            "unhealthy_apis": total_count - healthy_count,
            "health_percentage": (healthy_count / total_count * 100) if total_count > 0 else 0,
            "average_response_time_ms": avg_response_time,
            "circuit_breaker_states": circuit_states,
            "timestamp": time.time()
        }


def create_health_checker_from_config(config: Dict[str, APIClientConfig]) -> HealthChecker:
    """Create a health checker from API client configurations."""
    clients = {}
    
    for name, api_config in config.items():
        try:
            # Create a simple client for health checking
            client = SimpleHealthClient(api_config)
            clients[name] = client
        except Exception as e:
            # Log error but continue with other clients
            logger = get_logger("HealthChecker")
            logger.warning(f"Failed to create health checker for {name}: {e}")
    
    return HealthChecker(clients)


__all__ = [
    "HealthCheckStrategy",
    "HealthStatus",
    "HealthChecker", 
    "create_health_checker_from_config"
]
