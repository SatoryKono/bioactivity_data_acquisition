# Deprecations Register

| Symbol | Module | Date Announced | Removal Plan |
| --- | --- | --- | --- |
| _No deprecations registered._ | – | – | – |

## Update Policy

All deprecations MUST be recorded in this table when the `DeprecationWarning` is introduced. Each entry MUST specify the earliest
release where the removal is planned (SemVer `MAJOR.MINOR`), and the date the warning was announced. When the removal is executed,
update the table to reflect the outcome and link to the corresponding changelog entry.

Changes to public APIs MUST follow Semantic Versioning 2.0.0. Any incompatible change is deferred to the next MAJOR release; while
a warning is active, the MINOR version MUST increment on releases that introduce or update the deprecation plan.
