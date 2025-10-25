"""Data transformation layer for parsing raw API responses into structured data."""

from .chembl import extract_target_payload, parse_target_record

__all__ = ["parse_target_record", "extract_target_payload"]
