"""ChEMBL API client for activity data extraction."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from library.clients.base import BaseApiClient
from library.config import APIClientConfig

logger = logging.getLogger(__name__)


class ActivityChEMBLClient(BaseApiClient):
    """HTTP client for ChEMBL activity endpoints."""

    def __init__(self, config: APIClientConfig, cache_dir: Path | None = None) -> None:
        super().__init__(config)
        self.cache_dir = cache_dir or Path("data/cache/activity/raw")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_chembl_status(self) -> dict[str, Any]:
        """Get ChEMBL version and release information."""
        try:
            response = self._request("GET", "status.json")
            # Parse JSON response
            payload = response.json()
            return {
                "version": payload.get("chembl_db_version", "unknown"),
                "release_date": payload.get("chembl_release_date"),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            logger.warning(f"Failed to get ChEMBL version: {e}")
            return {
                "version": "unknown",
                "release_date": None,
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }

    def fetch_activities_paginated(
        self,
        limit: int = 1000,
        offset: int = 0,
        activity_ids: list[str] | None = None,
        assay_ids: list[str] | None = None,
        molecule_ids: list[str] | None = None,
        target_ids: list[str] | None = None,
        use_cache: bool = True
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch activities with pagination and caching."""
        
        # Build query parameters
        effective_limit = 1000 if limit is None else min(limit, 1000)
        params = {
            "limit": effective_limit,  # ChEMBL max limit is 1000
            "offset": offset,
            "format": "json"
        }
        
        # Add filters if provided
        if activity_ids:
            # ChEMBL expects numeric filter via __in with comma-separated list
            params["activity_id__in"] = ",".join(activity_ids)
        
        if assay_ids:
            params["assay_chembl_id"] = "|".join(assay_ids)
        
        if molecule_ids:
            params["molecule_chembl_id"] = "|".join(molecule_ids)
        
        if target_ids:
            params["target_chembl_id"] = "|".join(target_ids)
        
        # Generate cache key
        cache_key = self._generate_cache_key(params)
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        # Check cache first
        if use_cache and cache_file.exists():
            logger.info(f"Loading activities from cache: {cache_file}")
            try:
                with open(cache_file) as f:
                    cached_data = json.load(f)
                    for activity in cached_data.get("activities", []):
                        # Ensure consistent shape with API path
                        yield self._parse_activity(activity)
                    return
            except Exception as e:
                logger.warning(f"Failed to load from cache: {e}")
        
        # Fetch from API
        logger.info(f"Fetching activities from API: offset={offset}, limit={limit}")
        
        try:
            # Build URL with parameters
            query_string = urlencode(params)
            url = f"activity?{query_string}"
            
            payload = self._request("GET", url).json()
            
            # Cache the response
            if use_cache:
                try:
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        json.dump(payload, f, indent=2, ensure_ascii=False)
                    logger.debug(f"Cached response to: {cache_file}")
                except Exception as e:
                    logger.warning(f"Failed to cache response: {e}")
            
            # Yield activities
            activities = payload.get("activities", [])
            for activity in activities:
                yield self._parse_activity(activity)
                
            # Log pagination info
            page_meta = payload.get("page_meta", {})
            logger.info(
                f"Fetched {len(activities)} activities. "
                f"Total: {page_meta.get('total_count', 'unknown')}, "
                f"Next: {page_meta.get('next', 'none')}"
            )
            
        except Exception as e:
            logger.error(f"Failed to fetch activities: {e}")
            raise

    def fetch_activities_batch(
        self,
        *,
        filter_params: dict[str, Any],
        batch_size: int = 1000,
        use_cache: bool = True,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Fetch activities in batches using pagination.

        Yields lists (batches) of activity records with size up to batch_size.
        """
        offset = 0
        while True:
            page = list(
                self.fetch_activities_paginated(
                    limit=batch_size,
                    offset=offset,
                    activity_ids=filter_params.get("activity_ids"),
                    assay_ids=filter_params.get("assay_ids"),
                    molecule_ids=filter_params.get("molecule_ids"),
                    target_ids=filter_params.get("target_ids"),
                    use_cache=use_cache,
                )
            )
            if not page:
                break
            yield page
            if len(page) < batch_size:
                break
            offset += batch_size

    def fetch_all_activities(
        self,
        limit: int = 1000,
        activity_ids: list[str] | None = None,
        assay_ids: list[str] | None = None,
        molecule_ids: list[str] | None = None,
        target_ids: list[str] | None = None,
        max_pages: int | None = None,
        use_cache: bool = True
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch all activities with automatic pagination."""
        
        # If explicit list of activity_ids provided, batch them to respect URL length
        if activity_ids:
            chunk_size = 200  # Increased from 10 to allow more activities per request
            yielded = 0
            page_limit = 1000 if limit is None else min(limit, 1000)
            for i in range(0, len(activity_ids), chunk_size):
                chunk = activity_ids[i:i + chunk_size]
                # For id-filtered queries, pagination is rarely needed, but keep page_limit
                for activity in self.fetch_activities_paginated(
                    limit=page_limit,
                    offset=0,
                    activity_ids=chunk,
                    assay_ids=None,
                    molecule_ids=None,
                    target_ids=None,
                    use_cache=use_cache,
                ):
                    yield activity
                    yielded += 1
                    if limit is not None and yielded >= limit:
                        return
            return

        offset = 0
        page_count = 0
        total_activities = 0
        page_limit = 1000 if limit is None else min(limit, 1000)
        
        while True:
            # Check max pages limit
            if max_pages is not None and page_count >= max_pages:
                logger.info(f"Reached max pages limit: {max_pages}")
                break
            
            # Fetch current page
            page_activities = list(self.fetch_activities_paginated(
                limit=page_limit,
                offset=offset,
                activity_ids=activity_ids,
                assay_ids=assay_ids,
                molecule_ids=molecule_ids,
                target_ids=target_ids,
                use_cache=use_cache
            ))
            
            if not page_activities:
                logger.info("No more activities to fetch")
                break
            
            # Yield activities from this page
            for activity in page_activities:
                yield activity
                total_activities += 1
            
            # Check if we should continue
            if len(page_activities) < page_limit:
                logger.info("Reached end of data (partial page)")
                break
            
            # Move to next page
            offset += page_limit
            page_count += 1
            
            logger.info(f"Completed page {page_count}, total activities: {total_activities}")
        
        logger.info(f"Finished fetching activities. Total: {total_activities}, Pages: {page_count}")

    def _generate_cache_key(self, params: dict[str, Any]) -> str:
        """Generate a cache key from request parameters."""
        # Sort parameters for consistent keys
        sorted_params = sorted(params.items())
        param_string = json.dumps(sorted_params, sort_keys=True)
        
        # Create hash
        hash_obj = hashlib.md5(param_string.encode('utf-8'))
        return hash_obj.hexdigest()

    def _join_property_values(self, properties: list[dict], field: str) -> str | None:
        """Объединить значения поля из массива свойств через '|'."""
        values = [str(p.get(field, "")) for p in properties if p.get(field) is not None]
        return "|".join(values) if values else None

    def _parse_activity(self, activity_data: dict[str, Any]) -> dict[str, Any]:
        """Parse activity data from ChEMBL API response."""
        
        # Extract ligand efficiency from nested object
        ligand_eff = activity_data.get("ligand_efficiency") or {}
        
        # Extract activity properties from array
        props = activity_data.get("activity_properties", [])
        
        # Log missing optional fields
        missing_fields = []
        if not ligand_eff:
            missing_fields.append("ligand_efficiency")
        if not activity_data.get("uo_units"):
            missing_fields.append("uo_units")
        if not activity_data.get("qudt_units"):
            missing_fields.append("qudt_units")
        
        if missing_fields:
            logger.debug(f"Missing optional fields in activity {activity_data.get('activity_id')}: {missing_fields}")
        
        return {
            # Primary identifiers
            "activity_chembl_id": str(activity_data.get("activity_id", "")),
            "assay_chembl_id": activity_data.get("assay_chembl_id"),
            "molecule_chembl_id": activity_data.get("molecule_chembl_id"),
            "target_chembl_id": activity_data.get("target_chembl_id"),
            "document_chembl_id": activity_data.get("document_chembl_id"),
            "record_id": activity_data.get("record_id"),
            
            # Published values (original from source)
            "type": activity_data.get("type"),
            "relation": activity_data.get("relation"),
            "value": activity_data.get("value"),
            "units": activity_data.get("units"),
            "text_value": activity_data.get("text_value"),
            "upper_value": activity_data.get("upper_value"),
            
            # Standardized values (main for analytics)
            "standard_type": activity_data.get("standard_type"),
            "standard_relation": activity_data.get("standard_relation"),
            "standard_value": activity_data.get("standard_value"),
            "standard_units": activity_data.get("standard_units"),
            "standard_flag": activity_data.get("standard_flag"),
            "standard_text_value": activity_data.get("standard_text_value"),
            "standard_upper_value": activity_data.get("standard_upper_value"),
            
            # Published values (original from source) - duplicate for compatibility
            "published_type": activity_data.get("type"),
            "published_relation": activity_data.get("relation"),
            "published_value": activity_data.get("value"),
            "published_units": activity_data.get("units"),
            
            # Additional fields
            "pchembl_value": activity_data.get("pchembl_value"),
            "data_validity_comment": activity_data.get("data_validity_comment"),
            "activity_comment": activity_data.get("activity_comment"),
            "potential_duplicate": activity_data.get("potential_duplicate"),
            
            # Unit ontologies
            "uo_units": activity_data.get("uo_units"),
            "qudt_units": activity_data.get("qudt_units"),
            
            # Source and action
            "src_id": activity_data.get("src_id"),
            "action_type": activity_data.get("action_type"),
            
            # Ligand efficiency (from nested object)
            "bei": ligand_eff.get("bei"),
            "sei": ligand_eff.get("sei"),
            "le": ligand_eff.get("le"),
            "lle": ligand_eff.get("lle"),
            
            # BAO attributes
            "bao_endpoint": activity_data.get("bao_endpoint"),
            "bao_format": activity_data.get("bao_format"),
            "bao_label": activity_data.get("bao_label"),
            
            # Activity properties (from array, joined with "|")
            "activity_prop_type": self._join_property_values(props, "type"),
            "activity_prop_relation": self._join_property_values(props, "relation"),
            "activity_prop_value": self._join_property_values(props, "value"),
            "activity_prop_units": self._join_property_values(props, "units"),
            "activity_prop_text_value": self._join_property_values(props, "text_value"),
            "activity_prop_standard_type": self._join_property_values(props, "standard_type"),
            "activity_prop_standard_relation": self._join_property_values(props, "standard_relation"),
            "activity_prop_standard_value": self._join_property_values(props, "standard_value"),
            "activity_prop_standard_units": self._join_property_values(props, "standard_units"),
            "activity_prop_standard_text_value": self._join_property_values(props, "standard_text_value"),
            "activity_prop_comments": self._join_property_values(props, "comments"),
            "activity_prop_result_flag": self._join_property_values(props, "result_flag"),
            
            # Metadata
            "source_system": "ChEMBL",
            "retrieved_at": datetime.utcnow().isoformat() + "Z"
        }
