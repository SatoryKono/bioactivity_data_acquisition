# Python Code Style

This document defines the Python code style standards for the `bioetl` project. All code **MUST** conform to these standards, which are enforced by automated tooling in CI/CD.

## Principles

- **Determinism**: Code formatting and style checks **MUST** produce consistent results across all environments.
- **Type Safety**: Public APIs **MUST** be fully annotated and pass `mypy --strict` without `Any`.
- **Clarity**: Code **SHOULD** be readable, maintainable, and follow Python best practices.
- **Composition over Inheritance**: Prefer composition patterns over class inheritance where possible.

## Code Formatting

### Ruff and Black

All Python code **MUST** be formatted using `ruff format` (or `black` with identical settings):

- **Line length**: 100 characters maximum
- **Target version**: Python 3.10+ (as specified in `pyproject.toml`)
- **Indentation**: 4 spaces, no tabs

**Enforcement**: Pre-commit hooks and CI check formatting before allowing commits.

### Import Sorting (isort)

Imports **MUST** be sorted using `isort` with the following rules:

- Standard library imports first
- Third-party imports second
- First-party (`bioetl`) imports last
- No wildcard imports (`from x import *`) **SHALL NOT** be used
- Groups separated by blank lines

**Configuration**: Defined in `pyproject.toml` under `[tool.ruff.lint.isort]`.

#### Valid Examples

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import pandas as pd
import structlog

from bioetl.core.logger import UnifiedLogger
from bioetl.schemas import ActivitySchema
```

#### Invalid Examples

```python
# Invalid: wildcard import
from bioetl.core import *

# Invalid: incorrect ordering
from bioetl.core.logger import UnifiedLogger
import pandas as pd
from typing import Any
```

## Type Annotations

### Public API Requirements

All public functions, classes, and methods **MUST** have complete type annotations:

- Function parameters and return types
- Class attributes (using `dataclass` or explicit annotations)
- No use of `Any` without justification (documented in comments)

### Mypy Configuration

- **Mode**: `strict = true` for `src/bioetl` and `scripts`
- **Python version**: 3.10
- **Overrides**: Some third-party modules may ignore missing imports (see `pyproject.toml`)

### Valid Examples

```python
from typing import Sequence

def process_data(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """Process data and return filtered DataFrame."""
    return df[list(columns)]
```

```python
from dataclasses import dataclass
from pathlib import Path

@dataclass
class Config:
    """Pipeline configuration."""
    input_path: Path
    output_path: Path
    batch_size: int = 1000
```

### Invalid Examples

```python
# Invalid: missing return type
def process_data(df):
    return df

# Invalid: Any without justification
def process_data(df: pd.DataFrame) -> Any:
    return df
```

## Code Quality Rules

### Prohibited Patterns

The following patterns **SHALL NOT** be used:

1. **Wildcard imports**: `from x import *`
2. **Magic numbers**: Use named constants or configuration
3. **Global mutable state**: Prefer dependency injection or explicit state management
4. **Hidden side effects**: Functions should be pure where possible
5. **Function calls in argument defaults**: Use `None` and check inside function

### Valid Examples

```python
# Valid: named constant
DEFAULT_BATCH_SIZE = 1000

def process_batch(items: Sequence[str], batch_size: int = DEFAULT_BATCH_SIZE) -> list[str]:
    return items[:batch_size]
```

```python
# Valid: composition over inheritance
class DataProcessor:
    def __init__(self, validator: DataValidator, transformer: DataTransformer):
        self.validator = validator
        self.transformer = transformer
```

### Invalid Examples

```python
# Invalid: magic number
def process_batch(items: Sequence[str]) -> list[str]:
    return items[:1000]  # What is 1000?

# Invalid: function call in default
def process_data(items: list[str] = get_default_items()):  # B008 violation
    return items

# Invalid: global mutable state
_cache = {}  # Avoid global mutable state

def get_item(key: str) -> str | None:
    return _cache.get(key)  # Hidden side effect
```

## Ruff Linting Rules

The following rule categories are enabled (see `pyproject.toml`):

- **E, W**: pycodestyle errors and warnings
- **F**: pyflakes (unused imports, undefined names)
- **I**: isort (import sorting)
- **C**: flake8-comprehensions
- **B**: flake8-bugbear (common bugs)
- **UP**: pyupgrade (modernize Python syntax)

**Ignored rules**:
- `E501`: Line too long (handled by formatter)
- `B008`: Function calls in defaults (allowed with `None` pattern)
- `C901`: Too complex (review on case-by-case basis)

## Pre-commit Hooks

All code changes **MUST** pass pre-commit hooks:

- `ruff check`: Lint Python code
- `ruff format`: Format Python code
- `mypy`: Type checking (strict mode)

See `.pre-commit-config.yaml` for full configuration.

## CI Enforcement

The CI pipeline **MUST** block commits/PRs if any of the following fail:

- `ruff check src tests`
- `ruff format --check src tests`
- `mypy --strict src/bioetl`

See `.github/workflows/ci.yaml` for CI configuration.

## References

- Project configuration: `pyproject.toml`
- Pre-commit hooks: `.pre-commit-config.yaml`
- CI pipeline: `.github/workflows/ci.yaml`
- Detailed logging guidelines: [`02-logging-guidelines.md`](./02-logging-guidelines.md)
