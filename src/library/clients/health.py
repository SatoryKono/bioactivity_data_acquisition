"""Health check functionality for API clients."""

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import typer
from rich.console import Console
from rich.table import Table

from library.clients.base import BaseApiClient
from library.clients.circuit_breaker import CircuitState
from library.config import APIClientConfig
from library.logger import get_logger


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
    
    def __init__(self, clients: Dict[str, BaseApiClient]):
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
    
    def _check_client_health(self, client: BaseApiClient, name: str, timeout: float) -> HealthStatus:
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
            
            # Try to make a simple request to check connectivity
            # Use a lightweight endpoint if available
            if hasattr(client, '_make_url'):
                # Try to access a simple endpoint
                test_url = client._make_url("health") if hasattr(client, '_make_url') else None
                
                if test_url:
                    # Make a simple HEAD request to check connectivity
                    response = client.session.head(test_url, timeout=timeout)
                    response_time_ms = (time.time() - start_time) * 1000
                    
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
        circuit_states = {}
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
            # Create a basic client for health checking
            client = BaseApiClient(api_config)
            clients[name] = client
        except Exception as e:
            # Log error but continue with other clients
            logger = get_logger("HealthChecker")
            logger.warning(f"Failed to create health checker for {name}: {e}")
    
    return HealthChecker(clients)


__all__ = [
    "HealthStatus",
    "HealthChecker", 
    "create_health_checker_from_config"
]
