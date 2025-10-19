"""Data validation schemas for the ETL pipeline."""

from __future__ import annotations

# Preferred names for activity
from .activity_schema import NormalizedActivitySchema, RawActivitySchema

# Backward-compatible exported aliases keeping old import paths working
# Old names map to the new implementations to unify behaviour
RawBioactivitySchema = RawActivitySchema
NormalizedBioactivitySchema = NormalizedActivitySchema

# Public API (both new and legacy names)
__all__ = [
    "RawActivitySchema",
    "NormalizedActivitySchema",
    "RawBioactivitySchema",
    "NormalizedBioactivitySchema",
]
