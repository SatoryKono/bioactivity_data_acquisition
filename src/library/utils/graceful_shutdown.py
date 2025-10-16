"""Graceful shutdown utilities for long-running operations."""

import signal
import sys
import threading
from typing import Callable, Optional

from library.logger import get_logger


class GracefulShutdownManager:
    """Manages graceful shutdown for long-running operations."""
    
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self._shutdown_requested = False
        self._shutdown_handlers: list[Callable[[], None]] = []
        self._lock = threading.Lock()
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum: int, frame) -> None:  # noqa: ANN001
            signal_name = signal.Signals(signum).name
            self.logger.info(f"Received signal {signal_name}, initiating graceful shutdown...")
            self._request_shutdown()
        
        # Handle SIGTERM and SIGINT
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        # On Windows, also handle SIGBREAK
        if sys.platform == "win32":
            signal.signal(signal.SIGBREAK, signal_handler)
    
    def register_shutdown_handler(self, handler: Callable[[], None]) -> None:
        """Register a handler to be called during shutdown."""
        with self._lock:
            self._shutdown_handlers.append(handler)
    
    def _request_shutdown(self) -> None:
        """Request shutdown and call all registered handlers."""
        with self._lock:
            if self._shutdown_requested:
                return  # Already shutting down
            
            self._shutdown_requested = True
            self.logger.info(f"Calling {len(self._shutdown_handlers)} shutdown handlers...")
            
            # Call all registered handlers
            for handler in self._shutdown_handlers:
                try:
                    handler()
                except Exception as e:
                    self.logger.error(f"Error in shutdown handler: {e}")
    
    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_requested
    
    def wait_for_shutdown(self, timeout: Optional[float] = None) -> bool:
        """Wait for shutdown to complete or timeout.
        
        Returns:
            True if shutdown completed normally, False if timeout occurred.
        """
        if not self._shutdown_requested:
            return False
        
        # Wait for all handlers to complete
        start_time = threading.current_thread()
        if timeout:
            # Simple timeout implementation
            import time
            end_time = time.time() + timeout
            while time.time() < end_time and self._shutdown_requested:
                time.sleep(0.1)
            return not self._shutdown_requested
        
        return True


# Global instance for easy access
_global_shutdown_manager: Optional[GracefulShutdownManager] = None


def get_shutdown_manager() -> GracefulShutdownManager:
    """Get the global shutdown manager instance."""
    global _global_shutdown_manager
    if _global_shutdown_manager is None:
        _global_shutdown_manager = GracefulShutdownManager()
    return _global_shutdown_manager


def register_shutdown_handler(handler: Callable[[], None]) -> None:
    """Register a shutdown handler with the global manager."""
    get_shutdown_manager().register_shutdown_handler(handler)


def is_shutdown_requested() -> bool:
    """Check if shutdown has been requested."""
    return get_shutdown_manager().is_shutdown_requested()


def request_shutdown() -> None:
    """Request graceful shutdown."""
    get_shutdown_manager()._request_shutdown()


class ShutdownContext:
    """Context manager for graceful shutdown handling."""
    
    def __init__(self, timeout: Optional[float] = None):
        self.timeout = timeout
        self.shutdown_manager = get_shutdown_manager()
    
    def __enter__(self):
        return self.shutdown_manager
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.shutdown_manager.is_shutdown_requested():
            self.shutdown_manager.wait_for_shutdown(self.timeout)


__all__ = [
    "GracefulShutdownManager",
    "get_shutdown_manager", 
    "register_shutdown_handler",
    "is_shutdown_requested",
    "request_shutdown",
    "ShutdownContext"
]
