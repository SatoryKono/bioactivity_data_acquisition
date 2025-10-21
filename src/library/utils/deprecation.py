"""Utilities for deprecation warnings and lifecycle management."""

import functools
import warnings
from typing import Any, Callable


def deprecated(
    reason: str,
    version: str,
    removal_version: str,
    replacement: str | None = None
) -> Callable:
    """
    Decorator for marking deprecated functions.
    
    Args:
        reason: Reason for deprecation
        version: Version in which the function is marked as deprecated
        removal_version: Version in which the function will be removed
        replacement: Replacement function name (optional)
    
    Returns:
        Decorated function that emits DeprecationWarning when called
        
    Example:
        @deprecated(
            reason="Function renamed for clarity",
            version="0.2.0",
            removal_version="0.3.0",
            replacement="new_function"
        )
        def old_function(x: int) -> int:
            return new_function(x)
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            message = (
                f"{func.__name__} is deprecated since version {version} and will be removed in {removal_version}. "
                f"{reason}"
            )
            if replacement:
                message += f" Use {replacement} instead."
            
            warnings.warn(
                message, 
                DeprecationWarning, 
                stacklevel=2  # Show the caller, not this line
            )
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def deprecated_class(
    reason: str,
    version: str,
    removal_version: str,
    replacement: str | None = None
) -> Callable:
    """
    Decorator for marking deprecated classes.
    
    Args:
        reason: Reason for deprecation
        version: Version in which the class is marked as deprecated
        removal_version: Version in which the class will be removed
        replacement: Replacement class name (optional)
    
    Returns:
        Decorated class that emits DeprecationWarning when instantiated
        
    Example:
        @deprecated_class(
            reason="XXX",
            version="0.2.0",
            removal_version="0.3.0",
            replacement="NewClass"
        )
        class OldClass:
            pass
    """
    def decorator(cls: type) -> type:
        original_init = cls.__init__
        
        @functools.wraps(original_init)
        def new_init(self, *args: Any, **kwargs: Any) -> None:
            message = (
                f"{cls.__name__} is deprecated since version {version} and will be removed in {removal_version}. "
                f"{reason}"
            )
            if replacement:
                message += f" Use {replacement} instead."
            
            warnings.warn(
                message, 
                DeprecationWarning, 
                stacklevel=2  # Show the caller, not this line
            )
            original_init(self, *args, **kwargs)
        
        cls.__init__ = new_init
        return cls
    
    return decorator


def deprecation_warning(
    message: str,
    version: str,
    removal_version: str,
    replacement: str | None = None
) -> None:
    """
    Emit a deprecation warning with consistent formatting.
    
    Args:
        message: Deprecation message
        version: Version in which the deprecation was introduced
        removal_version: Version in which the feature will be removed
        replacement: Replacement feature (optional)
    """
    full_message = (
        f"{message} (deprecated since {version}, will be removed in {removal_version})"
    )
    if replacement:
        full_message += f". Use {replacement} instead."
    
    warnings.warn(full_message, DeprecationWarning, stacklevel=2)
