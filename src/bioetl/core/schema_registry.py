"""Registry for Pandera schemas.

This module provides a centralized registry for loading and caching Pandera
schemas, with validation and error handling.
"""

from __future__ import annotations

import importlib
from typing import Any

import pandera as pa
from pandera.typing import DataFrame

from bioetl.core.logger import get_logger


class SchemaRegistry:
    """Centralized registry for Pandera schemas."""

    def __init__(self) -> None:
        """Initialize schema registry."""
        self.logger = get_logger(__name__)
        self._schemas: dict[str, Any] = {}
        self._schema_paths: dict[str, str] = {}

    def register_schema(self, name: str, schema_path: str) -> None:
        """Register a schema by name and dotted path.

        Args:
            name: Name to register the schema under.
            schema_path: Dotted path to the schema (e.g., 'bioetl.schemas.chembl.activity_out.ActivitySchema').

        Raises:
            ValueError: If schema path is invalid or schema cannot be loaded.
        """
        if name in self._schemas:
            self.logger.warning(
                "schema_already_registered",
                name=name,
                message=f"Schema '{name}' is already registered, overwriting",
            )

        # Try to load schema to validate path
        schema = self.load_schema(schema_path)

        # Validate schema structure
        if not self.validate_schema_structure(schema):
            msg = f"Schema '{name}' does not have valid Pandera structure"
            raise ValueError(msg)

        self._schemas[name] = schema
        self._schema_paths[name] = schema_path

        self.logger.info("schema_registered", name=name, schema_path=schema_path)

    def get_schema(self, name: str) -> Any:
        """Get a registered schema by name.

        Args:
            name: Name of the registered schema.

        Returns:
            Pandera schema object.

        Raises:
            KeyError: If schema is not registered.
        """
        if name not in self._schemas:
            msg = f"Schema '{name}' is not registered"
            raise KeyError(msg)

        return self._schemas[name]

    def load_schema(self, dotted_path: str) -> Any:
        """Load a Pandera schema from a dotted module path.

        Args:
            dotted_path: Dotted path to the schema (e.g., 'bioetl.schemas.chembl.activity_out.ActivitySchema').

        Returns:
            Pandera DataFrameSchema instance or SchemaModel class.

        Raises:
            ImportError: If module cannot be imported.
            AttributeError: If schema class cannot be found in module.
            ValueError: If schema path format is invalid.
        """
        # Check cache first
        if dotted_path in self._schemas:
            self.logger.debug("schema_cache_hit", schema_path=dotted_path)
            return self._schemas[dotted_path]

        # Parse dotted path
        parts = dotted_path.split(".")
        if len(parts) < 2:
            msg = f"Invalid schema path format: '{dotted_path}' (expected 'module.ClassName' or 'package.module.ClassName')"
            raise ValueError(msg)

        # Extract module path and class name
        class_name = parts[-1]
        module_path = ".".join(parts[:-1])

        try:
            # Import module
            module = importlib.import_module(module_path)
        except ImportError as e:
            msg = f"Failed to import module '{module_path}' for schema '{dotted_path}': {e}"
            raise ImportError(msg) from e

        # Get schema class
        if not hasattr(module, class_name):
            msg = f"Schema class '{class_name}' not found in module '{module_path}'"
            raise AttributeError(msg)

        schema_class = getattr(module, class_name)

        # Cache schema
        self._schemas[dotted_path] = schema_class

        self.logger.info("schema_loaded", schema_path=dotted_path, schema_class=class_name)

        return schema_class

    def validate_schema_structure(self, schema: Any) -> bool:
        """Validate that an object has a valid Pandera schema structure.

        Args:
            schema: Object to validate.

        Returns:
            True if object appears to be a valid Pandera schema, False otherwise.
        """
        # Check if it's a SchemaModel class
        if isinstance(schema, type) and issubclass(schema, pa.SchemaModel):
            return True

        # Check if it's a DataFrameSchema instance
        if isinstance(schema, pa.DataFrameSchema):
            return True

        # Check if it has a validate method (other schema types)
        if hasattr(schema, "validate") and callable(getattr(schema, "validate")):
            return True

        return False

    def clear(self) -> None:
        """Clear all registered schemas."""
        self._schemas.clear()
        self._schema_paths.clear()
        self.logger.debug("schema_registry_cleared")

    def list_schemas(self) -> list[str]:
        """List all registered schema names.

        Returns:
            List of registered schema names.
        """
        return list(self._schemas.keys())

    def is_registered(self, name: str) -> bool:
        """Check if a schema is registered.

        Args:
            name: Name of the schema.

        Returns:
            True if schema is registered, False otherwise.
        """
        return name in self._schemas


# Global registry instance
_registry: SchemaRegistry | None = None


def get_registry() -> SchemaRegistry:
    """Get the global schema registry instance.

    Returns:
        Global SchemaRegistry instance.
    """
    global _registry
    if _registry is None:
        _registry = SchemaRegistry()
    return _registry


def reset_registry() -> None:
    """Reset the global schema registry (useful for testing)."""
    global _registry
    _registry = None

