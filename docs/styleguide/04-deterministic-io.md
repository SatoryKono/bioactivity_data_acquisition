# Deterministic I/O

This document defines the standards for deterministic input/output operations in the `bioetl` project. All data operations **MUST** ensure bit-for-bit reproducibility.

## Principles

- **Determinism**: Same input and configuration **MUST** produce identical output files.
- **Fixed Ordering**: Row and column order **MUST** be stable across runs.
- **UTC Time**: All timestamps **MUST** use UTC and ISO-8601 format.
- **Canonical Serialization**: JSON and other formats **MUST** use stable key ordering.
- **Atomic Writes**: File writes **MUST** be atomic (temp → fsync → rename).

## Row and Column Ordering

### Row Sorting

Before writing, data **MUST** be sorted by business keys in a stable order:

```python
def sort_dataframe(df: pd.DataFrame, sort_keys: list[str]) -> pd.DataFrame:
    """Sort DataFrame by business keys for deterministic output."""
    return df.sort_values(by=sort_keys, kind="stable").reset_index(drop=True)
```

**Configuration**: Sort keys defined in pipeline config under `write.sort_by`:

```yaml
write:
  sort_by: ["assay_id", "testitem_id", "activity_id"]
```

### Column Ordering

Column order **MUST** match the Pandera schema's `column_order`:

1. Schema defines order via `ordered=True`
2. Validation enforces column order
3. Write stage preserves this order

## Time Handling

### UTC Timestamps

All timestamps **MUST** use UTC and ISO-8601 format:

```python
from datetime import datetime, timezone

# Valid: UTC timestamp
timestamp = datetime.now(timezone.utc).isoformat()
# Result: "2024-01-01T12:00:00.123456+00:00"

# Invalid: local time
timestamp = datetime.now().isoformat()  # SHALL NOT use local time
```

### Valid Examples

```python
from datetime import datetime, timezone

def generate_metadata() -> dict[str, str]:
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "pipeline_version": "1.0.0",
    }
```

## Canonical Serialization

### JSON Serialization

JSON output **MUST** use stable key ordering:

```python
import json

def serialize_json(data: dict) -> str:
    """Serialize dict to JSON with sorted keys."""
    return json.dumps(data, sort_keys=True, ensure_ascii=False)
```

### CSV Serialization

CSV output **MUST** preserve column order from schema:

```python
def write_csv_deterministic(df: pd.DataFrame, path: Path):
    """Write CSV with fixed column order."""
    # Column order already enforced by schema validation
    df.to_csv(path, index=False, encoding="utf-8")
```

## Atomic File Writes

### Write Pattern

All file writes **MUST** use the atomic write pattern:

```python
import os
from pathlib import Path

def write_atomic(content: str, path: Path):
    """Write file atomically using temp → fsync → rename."""
    # 1. Write to temporary file
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    
    # 2. Flush to disk (fsync)
    tmp_path_file = tmp_path.open("wb")
    tmp_path_file.flush()
    os.fsync(tmp_path_file.fileno())
    tmp_path_file.close()
    
    # 3. Atomic rename
    os.replace(tmp_path, path)
```

### Valid Examples

```python
from pathlib import Path
import os

def write_dataframe_atomic(df: pd.DataFrame, path: Path):
    """Write DataFrame atomically."""
    tmp_path = path.with_suffix(".tmp")
    
    # Write to temp file
    df.to_csv(tmp_path, index=False, encoding="utf-8")
    
    # Flush and sync
    with tmp_path.open("rb+") as f:
        f.flush()
        os.fsync(f.fileno())
    
    # Atomic rename
    os.replace(tmp_path, path)
```

### Invalid Examples

```python
# Invalid: direct write (not atomic)
def write_dataframe_direct(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False)  # SHALL NOT write directly

# Invalid: missing fsync
def write_without_fsync(content: str, path: Path):
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(content)
    os.replace(tmp_path, path)  # Missing fsync
```

## Business Keys and Hashing

### Business Keys

All tables **MUST** have explicit business keys defined:

- Primary keys for uniqueness
- Sort keys for row ordering
- Foreign keys for relationships

### BLAKE2 Hashing

Derived keys **MUST** use BLAKE2 hash when generating composite keys:

```python
import hashlib

def hash_business_key(components: list[str]) -> str:
    """Generate BLAKE2 hash of business key components."""
    key_string = "|".join(components)
    return hashlib.blake2b(key_string.encode(), digest_size=16).hexdigest()
```

## QC Sidecar Files

### meta.yaml Structure

Every output file **MUST** have a corresponding `meta.yaml` sidecar file:

```yaml
pipeline_version: "1.0.0"
git_commit: "abc123def456"
config_hash: "sha256:..."
row_count: 50000
blake2_checksum: "a1b2c3d4..."
business_key_hash: "e5f6g7h8..."
generated_at_utc: "2024-01-01T12:00:00.123456+00:00"
```

### Valid Examples

```python
from pathlib import Path
import yaml
from datetime import datetime, timezone
import hashlib

def generate_meta_yaml(
    row_count: int,
    file_path: Path,
    pipeline_version: str,
    git_commit: str
) -> dict:
    """Generate meta.yaml content."""
    # Calculate checksum
    file_hash = hashlib.blake2b(file_path.read_bytes()).hexdigest()
    
    return {
        "pipeline_version": pipeline_version,
        "git_commit": git_commit,
        "row_count": row_count,
        "blake2_checksum": file_hash,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

def write_meta_yaml(meta: dict, data_path: Path):
    """Write meta.yaml atomically."""
    meta_path = data_path.with_suffix(".meta.yaml")
    content = yaml.dump(meta, sort_keys=True, default_flow_style=False)
    write_atomic(content, meta_path)
```

## File Naming Conventions

Output files **SHOULD** include:

- Entity name (e.g., `activity`)
- Schema version (e.g., `v1`)
- Business key hash (optional, for large datasets)

Example: `activity_v1_abc123def.csv`

## References

- Determinism policy: [`docs/determinism/`](../determinism/)
- ETL contract: [`docs/etl_contract/06-determinism-output.md`](../etl_contract/06-determinism-output.md)
- Schema guidelines: [`03-data-schemas.md`](./03-data-schemas.md)
