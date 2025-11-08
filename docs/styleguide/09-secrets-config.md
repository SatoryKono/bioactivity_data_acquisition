# Secrets and Configuration

This document defines the standards for handling secrets and configuration in the `bioetl` project. All secrets **MUST** be handled securely, and all configuration **MUST** be type-safe.

## Principles

- **No Hardcoded Secrets**: Secrets **SHALL NOT** be hardcoded in source code.
- **Environment Variables**: Secrets **MUST** be loaded from environment variables or secret managers.
- **Typed Configuration**: All configuration **MUST** use typed Pydantic models.
- **Configuration Profiles**: Shared settings **SHOULD** use configuration profiles.
- **Secret Rotation**: Secrets **SHOULD** support rotation and revocation.

## Secret Management

### Environment Variables

Secrets **MUST** be loaded from environment variables via `.env` files or environment:

```python
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file
load_dotenv(Path(".env"))

# Access secrets
api_key = os.getenv("CHEMBL_API_KEY")
if not api_key:
    raise ValueError("CHEMBL_API_KEY environment variable not set")
```

### .env File Structure

```bash
# .env (gitignored)
CHEMBL_API_KEY=your_api_key_here
PUBCHEM_API_KEY=your_api_key_here
DATABASE_URL=postgresql://user:password@localhost/db
```

### Secret Manager Integration

For production, secrets **SHOULD** be loaded from secret managers:

```python
from bioetl.core.secrets import SecretManager

# Load from secret manager
secret_manager = SecretManager(provider="aws_secrets_manager")
api_key = secret_manager.get_secret("chembl/api_key")
```

### Valid Examples

```python
import os
from typing import Optional

def get_api_key(service: str) -> str:
    """Get API key from environment variable."""
    key_name = f"{service.upper()}_API_KEY"
    api_key = os.getenv(key_name)
    if not api_key:
        raise ValueError(f"{key_name} environment variable not set")
    return api_key
```

### Invalid Examples

```python
# Invalid: hardcoded secret
API_KEY = "sk_live_1234567890abcdef"  # SHALL NOT hardcode secrets

# Invalid: secret in code comments
# API key: sk_live_1234567890abcdef  # SHALL NOT commit secrets
```

## Typed Configuration

### Pydantic Models

All configuration **MUST** use typed Pydantic models:

```python
from pydantic import BaseModel, Field
from pathlib import Path

class PipelineConfig(BaseModel):
    """Pipeline configuration model."""
    name: str
    source: str
    version: str = "1.0.0"
    output_dir: Path
    batch_size: int = Field(default=1000, ge=1, le=10000)

    class Config:
        env_prefix = "PIPELINE_"
        env_file = ".env"
```

### Configuration Loading

```python
from bioetl.config.models.base import PipelineConfig
import yaml
from pathlib import Path

def load_config(config_path: Path) -> PipelineConfig:
    """Load and validate configuration."""
    with config_path.open() as f:
        config_data = yaml.safe_load(f)

    # Merge with environment variables
    config = PipelineConfig(**config_data)
    return config
```

## Configuration Profiles

Shared settings **SHOULD** use configuration profiles:

### Profile Structure

```yaml
# configs/defaults/base.yaml
pipeline:
  version: "1.0.0"
  batch_size: 1000

output:
  format: "csv"
  encoding: "utf-8"
```

```yaml
# configs/defaults/network.yaml
api:
  timeout: 30.0
  retry_max_attempts: 3
  rate_limit_max_calls: 10
```

### Profile Inheritance

```yaml
# configs/pipelines/activity.yaml
extends:
  - "profiles/base.yaml"
  - "profiles/network.yaml"

pipeline:
  name: "activity"
  source: "chembl"
```

### Valid Examples

```python
from pathlib import Path
import yaml

def load_config_with_profiles(config_path: Path, profile_names: list[str]) -> dict:
    """Load config with profile inheritance."""
    config = {}

    # Load profiles first
    for profile_name in profile_names:
        profile_path = Path(f"configs/defaults/{profile_name}.yaml")
        with profile_path.open() as f:
            profile_data = yaml.safe_load(f)
            config = merge_config(config, profile_data)

    # Load main config
    with config_path.open() as f:
        main_config = yaml.safe_load(f)
        config = merge_config(config, main_config)

    return config
```

## Configuration Validation

All configuration **MUST** be validated on load:

### Valid Examples

```python
from pydantic import BaseModel, validator

class APIConfig(BaseModel):
    """API configuration with validation."""
    base_url: str
    timeout: float = Field(default=30.0, gt=0.0)
    api_key: str = Field(..., min_length=1)

    @validator("base_url")
    def validate_url(cls, v):
        if not v.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return v
```

## Secret Rotation

Secrets **SHOULD** support rotation:

### Valid Examples

```python
from datetime import datetime, timedelta

class RotatingSecret:
    """Secret with rotation support."""

    def __init__(self, secret_name: str, rotation_interval_days: int = 90):
        self.secret_name = secret_name
        self.rotation_interval = timedelta(days=rotation_interval_days)
        self.last_rotated = self.load_rotation_date()

    def needs_rotation(self) -> bool:
        """Check if secret needs rotation."""
        return datetime.now() - self.last_rotated > self.rotation_interval

    def rotate(self):
        """Rotate secret."""
        # Rotation logic
        self.last_rotated = datetime.now()
        self.save_rotation_date()
```

## CI Secret Scanning

CI **MUST** scan for secret leaks:

### Pre-commit Hook

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.4.0
    hooks:
      - id: detect-secrets
        args: ['--baseline', '.secrets.baseline']
```

### CI Configuration

```yaml
# .github/workflows/ci.yaml
- name: Scan for secrets
  uses: trufflesecurity/trufflehog@main
  with:
    path: ./
    base: ${{ github.event.repository.default_branch }}
```

## Configuration Precedence

Configuration **MUST** follow this precedence (highest to lowest):

1. Environment variables
2. CLI overrides (`--set KEY=VALUE`)
3. Pipeline-specific config
4. Configuration profiles

### Valid Examples

```python
def merge_config(base: dict, override: dict) -> dict:
    """Merge configuration with override taking precedence."""
    merged = base.copy()
    merged.update(override)
    return merged

# Load with precedence
config = load_profile("base.yaml")
config = merge_config(config, load_pipeline_config("activity.yaml"))
config = merge_config(config, parse_cli_overrides(cli_args))
config = merge_config(config, load_env_variables())
```

## References

- Configuration documentation: [`docs/configs/`](../configs/)
- Typed configs: [`docs/configs/00-typed-configs-and-profiles.md`](../configs/00-typed-configs-and-profiles.md)
