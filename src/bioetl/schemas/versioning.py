"""Schema versioning primitives and migration registry."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Callable, MutableMapping, Sequence

import pandas as pd


class SchemaMigrationError(ValueError):
    """Base error for schema migration issues."""


class SchemaVersionMismatchError(SchemaMigrationError):
    """Raised when the actual schema version does not match the expected one."""

    def __init__(
        self,
        *,
        schema_identifier: str,
        actual_version: str,
        expected_version: str,
        message: str | None = None,
    ) -> None:
        details = message or (
            f"Schema '{schema_identifier}' version '{actual_version}' does not match "
            f"expected version '{expected_version}'."
        )
        super().__init__(details)
        self.schema_identifier = schema_identifier
        self.actual_version = actual_version
        self.expected_version = expected_version


class SchemaMigrationPathError(SchemaMigrationError):
    """Raised when a migration path cannot be resolved."""


SchemaMigrationTransform = Callable[[pd.DataFrame], pd.DataFrame]


@dataclass(frozen=True, slots=True)
class SchemaMigration:
    """Single migration step between two schema versions."""

    schema_identifier: str
    from_version: str
    to_version: str
    transform_fn: SchemaMigrationTransform
    description: str | None = None

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the migration transform and ensure a DataFrame is returned."""

        result = self.transform_fn(df.copy())
        if not isinstance(result, pd.DataFrame):
            msg = (
                "Schema migration transform_fn must return a pandas.DataFrame "
                f"(got {type(result)!r})"
            )
            raise TypeError(msg)
        return result


class SchemaMigrationRegistry:
    """Registry storing schema migrations and resolving version paths."""

    def __init__(self) -> None:
        self._graph: MutableMapping[
            str, MutableMapping[str, MutableMapping[str, SchemaMigration]]
        ] = {}

    def register(self, migration: SchemaMigration) -> None:
        """Register a schema migration ensuring uniqueness."""

        schema_id = migration.schema_identifier
        from_version = migration.from_version
        to_version = migration.to_version

        if not schema_id:
            msg = "schema_identifier must be a non-empty string"
            raise ValueError(msg)
        if not from_version or not to_version:
            msg = "from_version and to_version must be non-empty strings"
            raise ValueError(msg)
        if from_version == to_version:
            msg = "Schema migration must target a different version"
            raise ValueError(msg)

        graph = self._graph.setdefault(schema_id, {})
        adjacency = graph.setdefault(from_version, {})
        if to_version in adjacency:
            msg = (
                f"Migration for '{schema_id}' from '{from_version}' to "
                f"'{to_version}' is already registered."
            )
            raise ValueError(msg)

        if self._creates_cycle(schema_id, from_version, to_version):
            msg = (
                f"Registering migration {from_version}->{to_version} for '{schema_id}' "
                "introduces a cycle, which is not allowed."
            )
            raise ValueError(msg)
        adjacency[to_version] = migration

    def resolve_path(
        self,
        schema_identifier: str,
        current_version: str,
        target_version: str,
        *,
        max_hops: int | None = None,
    ) -> list[SchemaMigration]:
        """Return the shortest migration path between versions."""

        if current_version == target_version:
            return []
        graph = self._graph.get(schema_identifier)
        if not graph:
            msg = (
                f"No migrations registered for schema '{schema_identifier}' "
                "and versions differ."
            )
            raise SchemaMigrationPathError(msg)

        queue = deque([(current_version, [])])
        visited = {current_version}

        while queue:
            version, path = queue.popleft()
            adjacency = graph.get(version, {})
            for destination, migration in adjacency.items():
                if destination in visited:
                    continue
                next_path = [*path, migration]
                if max_hops is not None and len(next_path) > max_hops:
                    continue
                if destination == target_version:
                    return next_path
                visited.add(destination)
                queue.append((destination, next_path))

        msg = (
            f"Cannot resolve migration path for schema '{schema_identifier}' "
            f"from '{current_version}' to '{target_version}'."
        )
        raise SchemaMigrationPathError(msg)

    def apply_migrations(
        self,
        df: pd.DataFrame,
        migrations: Sequence[SchemaMigration],
    ) -> pd.DataFrame:
        """Apply migrations sequentially in the provided order."""

        result = df.copy()
        for migration in migrations:
            result = migration.apply(result)
        return result

    def list_migrations(self, schema_identifier: str) -> list[SchemaMigration]:
        """Return all registered migrations for the schema."""

        graph = self._graph.get(schema_identifier)
        if not graph:
            return []
        payload: list[SchemaMigration] = []
        for adjacency in graph.values():
            payload.extend(adjacency.values())
        return payload

    def has_version(self, schema_identifier: str, version: str) -> bool:
        """Return true if schema version participates in migrations."""

        graph = self._graph.get(schema_identifier)
        if not graph:
            return False
        if version in graph:
            return True
        return any(version in adjacency for adjacency in graph.values())

    def _creates_cycle(self, schema_id: str, from_version: str, to_version: str) -> bool:
        """Return True if adding the migration would introduce a cycle."""

        graph = self._graph.get(schema_id)
        if not graph:
            return False

        stack = [to_version]
        visited: set[str] = set()
        while stack:
            current = stack.pop()
            if current == from_version:
                return True
            if current in visited:
                continue
            visited.add(current)
            stack.extend(graph.get(current, {}).keys())
        return False


SCHEMA_MIGRATION_REGISTRY = SchemaMigrationRegistry()


