"""Common API client factory for all pipelines."""

from __future__ import annotations  # noqa: I001

import os
import re
from typing import Any

from library.clients.crossref import CrossrefClient
from library.clients.openalex import OpenAlexClient
from library.clients.pubmed import PubMedClient
from library.clients.semantic_scholar import SemanticScholarClient
from library.settings import APIClientConfig, RateLimitSettings, RetrySettings


def _get_headers(source: str) -> dict[str, str]:
    """Get default headers for a source."""
    headers = {
        "User-Agent": "BioactivityDataAcquisition/1.0 (https://github.com/your-org/bioactivity_data_acquisition)",
        "Accept": "application/json",
    }

    if source == "crossref":
        headers["Accept"] = "application/vnd.crossref.unixref+json"
    elif source == "openalex":
        headers["Accept"] = "application/json"
    elif source == "pubmed":
        headers["Accept"] = "application/json"
    elif source == "semantic_scholar":
        headers["Accept"] = "application/json"

    return headers


def _get_base_url(source: str) -> str:
    """Get default base URL for a source."""
    urls = {
        "chembl": "https://www.ebi.ac.uk/chembl/api/data",
        "crossref": "https://api.crossref.org",
        "openalex": "https://api.openalex.org",
        "pubmed": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
        "semantic_scholar": "https://api.semanticscholar.org/graph/v1",
    }
    return urls.get(source, "")


def create_api_client(source: str, config: Any, client_type: str = "generic") -> Any:
    """Create an API client for the specified source using unified configuration.

    Args:
        source: Source name (chembl, crossref, openalex, pubmed, semantic_scholar)
        config: Pipeline configuration object with http.global_ and sources[source] settings
        client_type: Type of client to create ("generic", "assay", "activity", "document")

    Returns:
        Configured API client instance

    Raises:
        ValueError: If source is not supported or not found in configuration
    """
    # Get source-specific configuration
    source_config = config.sources.get(source)
    if not source_config:
        raise ValueError(f"Source '{source}' not found in configuration")

    # Use source-specific timeout or fallback to global
    timeout = source_config.http.timeout_sec or config.http.global_.timeout_sec
    if source == "chembl":
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL

    # Merge headers: default + global + source-specific
    default_headers = _get_headers(source)
    headers = {**default_headers, **config.http.global_.headers, **source_config.http.headers}

    # Process secret placeholders in headers
    processed_headers = {}
    for key, value in headers.items():
        if isinstance(value, str):

            def replace_placeholder(match):
                secret_name = match.group(1)
                env_var = os.environ.get(secret_name.upper())
                return env_var if env_var is not None else match.group(0)

            processed_value = re.sub(r"\{([^}]+)\}", replace_placeholder, value)
            # Only include header if the value is not empty after processing and not a placeholder
            if processed_value and processed_value.strip() and not processed_value.startswith("{") and not processed_value.endswith("}"):
                processed_headers[key] = processed_value
        else:
            processed_headers[key] = value
    headers = processed_headers

    # Use source-specific base_url or fallback to default
    base_url = source_config.http.base_url or _get_base_url(source)

    # Use source-specific retry settings or fallback to global
    retry_settings = RetrySettings(
        total=source_config.http.retries.get("total", config.http.global_.retries.total),
        backoff_multiplier=source_config.http.retries.get("backoff_multiplier", config.http.global_.retries.backoff_multiplier),
    )

    # Create rate limit settings if configured
    rate_limit = None
    if hasattr(source_config, 'rate_limit') and source_config.rate_limit:
        # Convert various rate limit formats to max_calls/period
        max_calls = source_config.rate_limit.get("max_calls")
        period = source_config.rate_limit.get("period")

        # If not in max_calls/period format, try to convert from other formats
        if max_calls is None or period is None:
            requests_per_second = source_config.rate_limit.get("requests_per_second")
            if requests_per_second is not None:
                max_calls = 1
                period = 1.0 / requests_per_second
            else:
                # Skip rate limiting if we can't determine the format
                rate_limit = None

        # Create RateLimitSettings object if we have valid max_calls and period
        if max_calls is not None and period is not None:
            rate_limit = RateLimitSettings(max_calls=max_calls, period=period)

    # Create base API client config
    api_config = APIClientConfig(
        name=source,
        base_url=base_url,
        headers=headers,
        timeout=timeout,
        retries=retry_settings,
        rate_limit=rate_limit,
        endpoint=source_config.endpoint or "",
    )

    # Create and return the appropriate client
    if source == "chembl":
        if client_type == "assay":
            # TODO: Implement AssayChEMBLClient or import from correct module
            # from library.assay.client import AssayChEMBLClient
            # return AssayChEMBLClient(api_config)
            raise NotImplementedError("AssayChEMBLClient is not implemented yet")
        elif client_type == "activity":
            from library.clients.chembl import ChEMBLClient

            return ChEMBLClient(api_config)
        elif client_type == "document":
            from library.clients.chembl import ChEMBLClient

            return ChEMBLClient(api_config)
        else:
            # Default to generic ChEMBL client
            from library.clients.chembl import ChEMBLClient

            return ChEMBLClient(api_config)
    elif source == "crossref":
        return CrossrefClient(api_config, timeout=timeout)
    elif source == "openalex":
        return OpenAlexClient(api_config, timeout=timeout)
    elif source == "pubmed":
        return PubMedClient(api_config, timeout=timeout)
    elif source == "semantic_scholar":
        return SemanticScholarClient(api_config, timeout=timeout)
    else:
        raise ValueError(f"Unsupported source: {source}")


__all__ = ["create_api_client"]
