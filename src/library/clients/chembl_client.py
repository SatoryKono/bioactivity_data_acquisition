"""HTTP client for ChEMBL API endpoints with enhanced status handling."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from library.clients.base import BaseApiClient
from library.clients.chembl import ChEMBLClient, TestitemChEMBLClient
from library.config import APIClientConfig

logger = logging.getLogger(__name__)


class ChEMBLStatusClient(BaseApiClient):
    """HTTP client for ChEMBL status and version information."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def get_chembl_status(self) -> dict[str, Any]:
        """Get ChEMBL status and release information from /status endpoint.
        
        Returns:
            Dictionary with ChEMBL version and release information:
            - chembl_db_version: ChEMBL database version (e.g., "ChEMBL_33")
            - chembl_release_date: Release date in ISO format (YYYY-MM-DD)
            - status: API status
            - timestamp: When the status was retrieved
        """
        try:
            payload = self._request("GET", "status")
            
            # Extract version information from ChEMBL status response
            chembl_release = payload.get("chembl_release", "")
            chembl_db_version = payload.get("chembl_db_version", "")
            
            # Parse release date if available
            chembl_release_date = None
            if chembl_release:
                # Try to extract date from release string
                # ChEMBL release format is typically "ChEMBL_XX (YYYY-MM-DD)"
                import re
                date_match = re.search(r'\((\d{4}-\d{2}-\d{2})\)', chembl_release)
                if date_match:
                    chembl_release_date = date_match.group(1)
            
            return {
                "chembl_db_version": chembl_db_version or chembl_release,
                "chembl_release_date": chembl_release_date,
                "status": payload.get("status", "unknown"),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            logger.warning("Failed to get ChEMBL status: %s", e)
            return {
                "chembl_db_version": None,
                "chembl_release_date": None,
                "status": "error",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "error": str(e)
            }


# Re-export existing classes for backward compatibility

__all__ = ["ChEMBLStatusClient", "TestitemChEMBLClient", "ChEMBLClient"]
