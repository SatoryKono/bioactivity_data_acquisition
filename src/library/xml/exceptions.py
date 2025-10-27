"""DEPRECATED: XML parsing specific exceptions.

This module is deprecated and will be removed in a future version.
Use library.common.exceptions instead.
"""

import warnings

from library.common.exceptions import XMLParseError as _XMLParseError
from library.common.exceptions import XMLValidationError as _XMLValidationError
from library.common.exceptions import XPathError as _XPathError

warnings.warn("library.xml.exceptions is deprecated. Use library.common.exceptions instead.", DeprecationWarning, stacklevel=2)

# Re-export from the new unified exceptions system for backward compatibility


# Legacy compatibility classes
class XMLParseError(_XMLParseError):
    """Legacy compatibility wrapper for XMLParseError."""

    def __init__(self, message: str) -> None:
        warnings.warn("XMLParseError from library.xml.exceptions is deprecated. Use library.common.exceptions.XMLParseError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message)


class XMLValidationError(_XMLValidationError):
    """Legacy compatibility wrapper for XMLValidationError."""

    def __init__(self, message: str) -> None:
        warnings.warn("XMLValidationError from library.xml.exceptions is deprecated. Use library.common.exceptions.XMLValidationError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message)


class XPathError(_XPathError):
    """Legacy compatibility wrapper for XPathError."""

    def __init__(self, message: str) -> None:
        warnings.warn("XPathError from library.xml.exceptions is deprecated. Use library.common.exceptions.XPathError instead.", DeprecationWarning, stacklevel=3)
        super().__init__(message)
