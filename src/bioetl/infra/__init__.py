"""Инфраструктурные компоненты BioETL."""

from bioetl.infra.pagination_registry import (
    PaginationRegistry,
    default_pagination_registry,
)

__all__ = ["PaginationRegistry", "default_pagination_registry"]
