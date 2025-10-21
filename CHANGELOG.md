# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- CLI commands `analyze-results` and `check-fields` for pipeline analysis
- Deprecation utilities in `src/library/utils/deprecation.py`
- Quality tools integration: Vulture (dead code), jscpd (duplicates), Deptry (dependencies)
- pytest markers for slow/integration tests
- Comprehensive Makefile targets for quality checks

### Changed
- Migrated debug scripts to CLI commands for better integration
- Reorganized test structure with slow/integration markers
- Enhanced pre-commit hooks with additional quality checks

### Removed
- Empty test files from repository root (7 files)
- Duplicate IUPHAR debug tests (13 files)
- Outdated development scripts and PowerShell files
- Empty `scripts/dev/` directory

## [0.2.0] - 2025-10-21

### Deprecated
- `scripts/analyze_fixed_results.py` → CLI command `analyze-results` (will be removed in 0.3.0)
- `scripts/check_field_fill.py` → CLI command `check-fields` (will be removed in 0.3.0)
- `scripts/check_specific_limits.py` → CLI command `health` (will be removed in 0.3.0)

### Removed
- None (first deprecation warning)

## [0.3.0] - 2025-12-01

### Removed
- `scripts/analyze_fixed_results.py` (deprecated since 0.2.0)
- `scripts/check_field_fill.py` (deprecated since 0.2.0)
- `scripts/check_specific_limits.py` (deprecated since 0.2.0)

## Deprecation Policy

This project follows a **N+1 release deprecation policy**:

1. **Version N (current)**: Feature works without warnings
2. **Version N+1**: Feature marked as `@deprecated`, works with warnings
3. **Version N+2**: Feature removed, usage causes errors

### Using Deprecation Utilities

```python
from library.utils.deprecation import deprecated

@deprecated(
    reason="Function renamed for clarity",
    version="0.2.0",
    removal_version="0.3.0",
    replacement="new_function"
)
def old_function(x: int) -> int:
    return new_function(x)
```

### Migration Examples

#### Scripts → CLI Commands

**Before (0.1.x)**:
```bash
python scripts/analyze_fixed_results.py data/output/documents.csv
python scripts/check_field_fill.py data/output/target.csv
```

**After (0.2.0+)**:
```bash
bioactivity-data-acquisition analyze-results data/output/documents.csv
bioactivity-data-acquisition check-fields data/output/target.csv
```

#### Test Organization

**Before (0.1.x)**:
```bash
pytest tests/iuphar/  # All tests, including slow debug tests
```

**After (0.2.0+)**:
```bash
pytest                              # Fast tests only
pytest -m "slow"                    # Slow tests only
pytest -m "integration and not slow" # Integration without slow
```

## Quality Tools Integration

### Dead Code Detection (Vulture)
```bash
vulture src/ --min-confidence 80
make check-dead-code
```

### Duplicate Code Detection (jscpd)
```bash
npx jscpd src/ --config .jscpd.json --yes
make check-duplicates
```

### Dependency Analysis (Deptry)
```bash
deptry src/
make check-deps
```

### Full Quality Check
```bash
make qa-full  # Includes fmt, lint, type-check, duplicates, dead-code, deps
```

## Breaking Changes

- **0.2.0**: Removed empty test files and duplicate debug scripts
- **0.3.0**: Will remove deprecated scripts (migrated to CLI commands)

## Migration Guide

### For Developers

1. **Update scripts**: Replace direct script calls with CLI commands
2. **Test markers**: Use appropriate pytest markers for test categorization
3. **Quality checks**: Run `make qa-full` before committing
4. **Deprecation warnings**: Address deprecation warnings promptly

### For CI/CD

1. **Update workflows**: Use new CLI commands instead of scripts
2. **Test execution**: Use `pytest -m "not slow"` for fast CI runs
3. **Quality gates**: Add quality tool checks to CI pipeline
4. **Dependency updates**: Monitor and update dependencies regularly

## Support

- **Documentation**: See `docs/how-to/` for detailed guides
- **Issues**: Report issues via GitHub Issues
- **Migration help**: Check `docs/how-to/contribute.md` for contribution guidelines
