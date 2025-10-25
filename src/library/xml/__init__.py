"""XML parsing utilities with lxml.etree for safe and robust XML/HTML processing."""

from .exceptions import XMLParseError, XMLValidationError, XPathError
from .namespaces import COMMON_NS, PUBCHEM_NS, PUBMED_NS, UNIPROT_NS
from .parser_factory import make_html_parser, make_xml_parser
from .selectors import attr, select_many, select_one, text

__all__ = [
    # Parser factory
    "make_xml_parser",
    "make_html_parser",
    # Selectors
    "select_one",
    "select_many",
    "text",
    "attr",
    # Namespaces
    "PUBMED_NS",
    "UNIPROT_NS",
    "PUBCHEM_NS",
    "COMMON_NS",
    # Exceptions
    "XMLParseError",
    "XMLValidationError",
    "XPathError",
]
