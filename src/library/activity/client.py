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
        """Get ChEMBL status and release information."""
        try:
            payload = self._request("GET", "status")
            return {
                "chembl_release": payload.get("chembl_release", "unknown"),
                "status": payload.get("status", "unknown"),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            logger.warning(f"Failed to get ChEMBL status: {e}")
            return {
                "chembl_release": "unknown",
                "status": "error",
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
        """Извлечение данных активности из ChEMBL API с пагинацией и фильтрацией.
        
        Выполняет пагинированные запросы к ChEMBL API для получения данных
        о биологической активности с возможностью фильтрации по различным
        идентификаторам. Поддерживает кэширование и обработку ошибок.
        
        Args:
            limit: Количество записей на страницу (максимум 1000)
            offset: Смещение для пагинации
            activity_ids: Список ID активностей для фильтрации
            assay_ids: Список ID ассеев для фильтрации
            molecule_ids: Список ID молекул для фильтрации
            target_ids: Список ID мишеней для фильтрации
            use_cache: Использовать кэширование для ускорения повторных запросов
            
        Yields:
            dict: Словарь с данными активности из ChEMBL API
            
        Raises:
            HTTPError: При ошибках HTTP запросов
            ValueError: При невалидных параметрах запроса
            
        Example:
            >>> client = ActivityChEMBLClient(config)
            >>> for activity in client.fetch_activities_paginated(assay_ids=["CHEMBL123"]):
            ...     print(activity["activity_chembl_id"])
        """
        
        # Build query parameters
        effective_limit = 1000 if limit is None else min(limit, 1000)
        params = {
            "limit": effective_limit,  # ChEMBL max limit is 1000
            "offset": offset,
            "format": "json"
        }
        
        # Add filters if provided (limit to avoid HTTP 413 errors)
        max_filters_per_type = 10  # Limit filters to prevent URL too long
        
        if activity_ids:
            # Limit activity IDs to prevent URL too long
            limited_activity_ids = activity_ids[:max_filters_per_type]
            # ChEMBL expects numeric filter via __in with comma-separated list
            params["activity_id__in"] = ",".join(limited_activity_ids)
            if len(activity_ids) > max_filters_per_type:
                logger.warning(f"Limited activity_ids to {max_filters_per_type} to prevent HTTP 413 error")
        
        if assay_ids:
            # Limit assay IDs to prevent URL too long
            limited_assay_ids = assay_ids[:max_filters_per_type]
            params["assay_chembl_id"] = "|".join(limited_assay_ids)
            if len(assay_ids) > max_filters_per_type:
                logger.warning(f"Limited assay_ids to {max_filters_per_type} to prevent HTTP 413 error")
        
        if molecule_ids:
            # Limit molecule IDs to prevent URL too long
            limited_molecule_ids = molecule_ids[:max_filters_per_type]
            params["molecule_chembl_id"] = "|".join(limited_molecule_ids)
            if len(molecule_ids) > max_filters_per_type:
                logger.warning(f"Limited molecule_ids to {max_filters_per_type} to prevent HTTP 413 error")
        
        if target_ids:
            # Limit target IDs to prevent URL too long
            limited_target_ids = target_ids[:max_filters_per_type]
            params["target_chembl_id"] = "|".join(limited_target_ids)
            if len(target_ids) > max_filters_per_type:
                logger.warning(f"Limited target_ids to {max_filters_per_type} to prevent HTTP 413 error")
        
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
            
            payload = self._request("GET", url)
            
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
        
        # If explicit list of activity_ids provided, batch them by 10 to respect URL length
        if activity_ids:
            chunk_size = 10
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
        hash_obj = hashlib.sha256(param_string.encode('utf-8'))
        return hash_obj.hexdigest()

    def _parse_activity(self, activity_data: dict[str, Any]) -> dict[str, Any]:
        """Parse activity data from ChEMBL API response."""
        return {
            # Primary identifiers
            "activity_chembl_id": str(activity_data.get("activity_id", "")),
            "assay_chembl_id": activity_data.get("assay_chembl_id"),
            "molecule_chembl_id": activity_data.get("molecule_chembl_id"),
            "target_chembl_id": activity_data.get("target_chembl_id"),
            "document_chembl_id": activity_data.get("document_chembl_id"),
            
            # Published values (original from source)
            "published_type": activity_data.get("type"),
            "published_relation": activity_data.get("relation"),
            "published_value": activity_data.get("value"),
            "published_units": activity_data.get("units"),
            
            # Standardized values (main for analytics)
            "standard_type": activity_data.get("standard_type"),
            "standard_relation": activity_data.get("standard_relation"),
            "standard_value": activity_data.get("standard_value"),
            "standard_units": activity_data.get("standard_units"),
            "standard_flag": activity_data.get("standard_flag"),
            
            # Additional fields
            "pchembl_value": activity_data.get("pchembl_value"),
            "data_validity_comment": activity_data.get("data_validity_comment"),
            "activity_comment": activity_data.get("activity_comment"),
            
            # BAO attributes
            "bao_endpoint": activity_data.get("bao_endpoint"),
            "bao_format": activity_data.get("bao_format"),
            "bao_label": activity_data.get("bao_label"),
            
            # Metadata
            "source_system": "ChEMBL",
            "retrieved_at": datetime.utcnow().isoformat() + "Z"
        }
