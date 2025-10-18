"""Configuration management for activity data extraction."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from library.config import APIClientConfig


@dataclass
class ActivityConfig:
    """Configuration for activity data extraction from ChEMBL."""
    
    # API settings
    chembl_base_url: str = "https://www.ebi.ac.uk/chembl/api/data"
    chembl_release: str = "unknown"
    
    # Extraction parameters
    limit: int = 1000
    max_retries: int = 5
    timeout_sec: float = 60.0
    
    # Filter parameters (optional)
    assay_ids: list[str] = field(default_factory=list)
    molecule_ids: list[str] = field(default_factory=list)
    target_ids: list[str] = field(default_factory=list)
    
    # Quality profiles
    strict_quality: bool = True
    
    # Output settings
    output_dir: str = "data/output/activity"
    cache_dir: str = "data/cache/activity/raw"
    
    # Runtime settings
    dry_run: bool = False
    workers: int = 4
    
    # Logging
    log_level: str = "INFO"
    
    @classmethod
    def from_yaml(cls, config_path: str | Path) -> ActivityConfig:
        """Load configuration from YAML file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        return cls.from_dict(config_data)
    
    @classmethod
    def from_dict(cls, config_data: dict[str, Any]) -> ActivityConfig:
        """Create configuration from dictionary."""
        # Extract ChEMBL settings
        chembl_config = config_data.get('sources', {}).get('chembl', {})
        http_config = chembl_config.get('http', {})
        
        # Extract runtime settings
        runtime_config = config_data.get('runtime', {})
        
        # Extract IO settings
        io_config = config_data.get('io', {})
        output_config = io_config.get('output', {})
        cache_config = io_config.get('cache', {})
        
        # Extract logging settings
        logging_config = config_data.get('logging', {})
        
        return cls(
            chembl_base_url=http_config.get('base_url', 'https://www.ebi.ac.uk/chembl/api/data'),
            limit=runtime_config.get('limit', 1000),
            max_retries=config_data.get('http', {}).get('global', {}).get('retries', {}).get('total', 5),
            timeout_sec=http_config.get('timeout_sec', 60.0),
            output_dir=output_config.get('dir', 'data/output/activity'),
            cache_dir=cache_config.get('raw_dir', 'data/cache/activity/raw'),
            dry_run=runtime_config.get('dry_run', False),
            workers=runtime_config.get('workers', 4),
            log_level=logging_config.get('level', 'INFO'),
        )
    
    def to_api_client_config(self) -> APIClientConfig:
        """Convert to APIClientConfig for use with base client."""
        # Get API token from environment
        api_token = os.getenv('CHEMBL_API_TOKEN')
        
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'bioactivity-data-acquisition/0.1.0'
        }
        
        if api_token:
            headers['Authorization'] = f'Bearer {api_token}'
        
        return APIClientConfig(
            name='chembl_activity',
            base_url=self.chembl_base_url,
            timeout=self.timeout_sec,
            headers=headers,
            retries={
                'total': self.max_retries,
                'backoff_multiplier': 3.0
            }
        )
    
    def get_extraction_url(self) -> str:
        """Get the base URL for activity extraction."""
        return f"{self.chembl_base_url}/activity"
    
    def get_cache_path(self) -> Path:
        """Get the cache directory path."""
        return Path(self.cache_dir)
    
    def get_output_path(self) -> Path:
        """Get the output directory path."""
        return Path(self.output_dir)
    
    def ensure_directories(self) -> None:
        """Ensure output and cache directories exist."""
        self.get_output_path().mkdir(parents=True, exist_ok=True)
        self.get_cache_path().mkdir(parents=True, exist_ok=True)
