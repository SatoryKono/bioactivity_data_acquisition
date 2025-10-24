"""Simple cache manager for API responses."""

import hashlib
import json
import time
from pathlib import Path
from typing import Any

import logging

logger = logging.getLogger(__name__)


class CacheManager:
    """Simple file-based cache manager with TTL support."""
    
    def __init__(self, cache_dir: str = "data/cache", default_ttl: int = 3600):
        """Initialize cache manager.
        
        Args:
            cache_dir: Directory for cache files
            default_ttl: Default TTL in seconds (1 hour)
        """
        self.cache_dir = Path(cache_dir)
        self.default_ttl = default_ttl
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories for different data types
        (self.cache_dir / "assays").mkdir(exist_ok=True)
        (self.cache_dir / "targets").mkdir(exist_ok=True)
        (self.cache_dir / "molecules").mkdir(exist_ok=True)
    
    def _get_cache_key(self, endpoint: str, params: dict[str, Any]) -> str:
        """Generate cache key from endpoint and parameters."""
        # Create deterministic key from endpoint and sorted params
        key_data = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _get_cache_path(self, cache_type: str, cache_key: str) -> Path:
        """Get cache file path."""
        return self.cache_dir / cache_type / f"{cache_key}.json"
    
    def get(self, cache_type: str, endpoint: str, params: dict[str, Any]) -> dict[str, Any] | None:
        """Get cached data if available and not expired.
        
        Args:
            cache_type: Type of cache (assays, targets, molecules)
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            Cached data or None if not found/expired
        """
        cache_key = self._get_cache_key(endpoint, params)
        cache_path = self._get_cache_path(cache_type, cache_key)
        
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Check TTL
            if time.time() > cache_data.get('expires_at', 0):
                logger.debug(f"Cache expired for {cache_type}:{cache_key}")
                cache_path.unlink()  # Remove expired cache
                return None
            
            logger.debug(f"Cache hit for {cache_type}:{cache_key}")
            return cache_data.get('data')
            
        except (json.JSONDecodeError, KeyError, OSError) as e:
            logger.warning(f"Failed to read cache file {cache_path}: {e}")
            # Remove corrupted cache file
            try:
                cache_path.unlink()
            except OSError:
                pass
            return None
    
    def set(self, cache_type: str, endpoint: str, params: dict[str, Any], 
            data: dict[str, Any], ttl: int | None = None) -> None:
        """Cache data with TTL.
        
        Args:
            cache_type: Type of cache (assays, targets, molecules)
            endpoint: API endpoint
            params: Request parameters
            data: Data to cache
            ttl: TTL in seconds (uses default if None)
        """
        cache_key = self._get_cache_key(endpoint, params)
        cache_path = self._get_cache_path(cache_type, cache_key)
        
        ttl = ttl or self.default_ttl
        expires_at = time.time() + ttl
        
        cache_data = {
            'data': data,
            'expires_at': expires_at,
            'cached_at': time.time(),
            'endpoint': endpoint,
            'params': params
        }
        
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Cached {cache_type}:{cache_key} (expires in {ttl}s)")
            
        except OSError as e:
            logger.warning(f"Failed to write cache file {cache_path}: {e}")
    
    def clear(self, cache_type: str | None = None) -> None:
        """Clear cache.
        
        Args:
            cache_type: Specific cache type to clear, or None for all
        """
        if cache_type:
            cache_type_dir = self.cache_dir / cache_type
            if cache_type_dir.exists():
                for cache_file in cache_type_dir.glob("*.json"):
                    cache_file.unlink()
                logger.info(f"Cleared {cache_type} cache")
        else:
            # Clear all caches
            for cache_type_dir in self.cache_dir.iterdir():
                if cache_type_dir.is_dir():
                    for cache_file in cache_type_dir.glob("*.json"):
                        cache_file.unlink()
            logger.info("Cleared all caches")
    
    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        stats = {
            'total_files': 0,
            'total_size': 0,
            'by_type': {}
        }
        
        for cache_type_dir in self.cache_dir.iterdir():
            if cache_type_dir.is_dir():
                type_name = cache_type_dir.name
                files = list(cache_type_dir.glob("*.json"))
                type_size = sum(f.stat().st_size for f in files)
                
                stats['by_type'][type_name] = {
                    'files': len(files),
                    'size_bytes': type_size
                }
                stats['total_files'] += len(files)
                stats['total_size'] += type_size
        
        return stats
