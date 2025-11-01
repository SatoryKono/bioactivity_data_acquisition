# Pipeline Metrics Report

Generated on 2025-10-31T15:46:36+00:00

## Code Footprint

| Category | Files (baseline) | Files (current) | Δ | LOC (baseline) | LOC (current) | Δ | Public symbols (baseline) | Public symbols (current) | Δ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Monolithic pipelines | 9 | 9 | +0 | 8 036 | 1 441 | -6 595 | 31 | 27 | -4 |
| ChEMBL proxies | 0 | 79 | +79 | 0 | 6 874 | +6 874 | 0 | 109 | +109 |
| Combined | 9 | 88 | +79 | 8 036 | 8 315 | +279 | 31 | 136 | +105 |

## Pandera Validation Coverage

| Scope | Baseline | Current | Δ |
| --- | ---: | ---: | ---: |
| Pipeline family | 79.0% | 40.3% | -38.6% |

## Test Execution Time

| Test suite | Baseline | Current | Δ |
| --- | ---: | ---: | ---: |
| pytest (tests) | 9.09s | 3.69s | -5.40s |

### Methodology

- Inventory metrics are derived from `PIPELINES.inventory.csv` snapshots (baseline vs. current). Public symbols count exported names captured during inventory collection.
- Pandera coverage estimates weight the share of lines of code importing Pandera schemas within monolithic pipelines and ChEMBL proxies.
- Test duration measures `pytest --maxfail=1 --disable-warnings -q tests` wall-clock time.
