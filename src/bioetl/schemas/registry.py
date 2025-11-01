"""Schema Registry with versioning and descriptive metadata."""

import re
from dataclasses import dataclass

from packaging import version

from bioetl.core.logger import UnifiedLogger
from bioetl.pandera_pandas import DataFrameModel

logger = UnifiedLogger.get(__name__)


def _resolve_schema_column_order(schema: DataFrameModel | None) -> list[str]:
    """Return the canonical column order for a schema.

    This is a local copy to avoid circular imports with bioetl.utils.dataframe.

    Pandera's :class:`~pandera.api.pandas.model.DataFrameModel` exposes a
    ``get_column_order`` helper in our ``BaseSchema`` subclasses, but in
    practice some schemas may not populate ``_column_order`` (for example when
    the contract was imported from an external source).  Historically this led
    to callers receiving an empty list, skipping reordering logic and letting
    Pandera enforce its own field order.  On certain environments this surfaced
    as ``column '...' out-of-order`` validation failures.

    This helper normalises the behaviour by preferring the explicit
    ``get_column_order`` value and falling back to the concrete DataFrameSchema
    definition so that callers can deterministically align their dataframe
    columns with the schema contract.
    """

    if schema is None:
        return []

    try:
        explicit_order = schema.get_column_order()  # type: ignore[attr-defined]
    except AttributeError:
        explicit_order = []

    if explicit_order:
        return list(explicit_order)

    fallback_order = getattr(schema, "_column_order", None)
    if fallback_order:
        return list(fallback_order)

    try:
        materialised = schema.to_schema()
    except Exception:  # pragma: no cover - defensive fallback
        materialised = None

    if materialised is not None:
        try:
            columns = list(materialised.columns.keys())
        except AttributeError:  # pragma: no cover - legacy Pandera versions
            columns = list(materialised.columns)
        if columns:
            return columns

    # Final fallback: rely on Pydantic's field order if available.
    model_fields = getattr(schema, "model_fields", None)
    if isinstance(model_fields, dict) and model_fields:
        return list(model_fields.keys())

    return []


@dataclass(frozen=True)
class SchemaRegistration:
    """Container describing a schema registration entry."""

    entity: str
    version: str
    schema: DataFrameModel
    schema_id: str
    column_order_source: str
    na_policy: str
    precision_policy: str


class SchemaRegistry:
    """Реестр Pandera схем с версионированием."""

    _registry: dict[str, dict[str, DataFrameModel]] = {}
    _metadata: dict[tuple[str, str], SchemaRegistration] = {}

    @classmethod
    def register(
        cls,
        entity: str,
        schema_version: str,
        schema: DataFrameModel,
        column_order: list[str] | None = None,
        *,
        schema_id: str | None = None,
        column_order_source: str | None = None,
        na_policy: str | None = None,
        precision_policy: str | None = None,
    ) -> None:
        """
        Регистрирует схему в реестре.

        Args:
            entity: Имя сущности (например, 'document', 'target')
            schema_version: Семантическая версия (например, '1.0.0')
            schema: Pandera schema
            column_order: Порядок колонок (опционально)
        """
        if entity not in cls._registry:
            cls._registry[entity] = {}

        # Validate version format (semver)
        if not re.match(r"^\d+\.\d+\.\d+$", schema_version):
            raise ValueError(f"Invalid version format: {schema_version}. Expected semver (e.g., 1.0.0)")

        cls._registry[entity][schema_version] = schema

        column_count = 0

        try:
            column_order = _resolve_schema_column_order(schema)
        except Exception:  # pragma: no cover - defensive fallback
            column_order = []

        if column_order:
            column_count = len(column_order)
        else:
            model_fields = getattr(schema, "model_fields", {})
            if model_fields:
                column_count = len(model_fields)

        if column_count == 0:
            try:
                materialised = schema.to_schema()
            except (AttributeError, TypeError):
                materialised = None

            if materialised is not None:
                try:
                    column_count = len(materialised.columns)
                except AttributeError:
                    column_count = len(list(materialised.columns or []))

        if column_count == 0:
            try:
                column_count = len(_resolve_schema_column_order(schema))
            except Exception:  # pragma: no cover - defensive fallback
                column_count = 0

        registration = SchemaRegistration(
            entity=entity,
            version=schema_version,
            schema=schema,
            schema_id=schema_id or f"{entity}.output",
            column_order_source=column_order_source or "schema_registry",
            na_policy=na_policy or "schema_defined",
            precision_policy=precision_policy or "%.6f",
        )
        cls._metadata[(entity, schema_version)] = registration

        logger.info(
            "schema_registered",
            entity=entity,
            version=schema_version,
            columns=column_count,
        )

    @classmethod
    def get(cls, entity: str, schema_version: str = "latest") -> DataFrameModel:
        """
        Получает схему из реестра.

        Args:
            entity: Имя сущности
            schema_version: Версия схемы или 'latest'

        Returns:
            Pandera schema
        """
        if entity not in cls._registry:
            raise ValueError(f"Entity {entity} not found in registry")

        if schema_version == "latest":
            # Get latest version
            versions = cls._registry[entity].keys()
            if not versions:
                raise ValueError(f"No schemas found for entity {entity}")

            latest_version = max(versions, key=lambda v: version.parse(v))
            schema = cls._registry[entity][latest_version]
            logger.debug(
                "schema_retrieved_latest",
                entity=entity,
                version=latest_version,
            )
            return schema

        if schema_version not in cls._registry[entity]:
            raise ValueError(f"Schema version {schema_version} not found for entity {entity}")

        return cls._registry[entity][schema_version]

    @classmethod
    def get_metadata(
        cls,
        entity: str,
        schema_version: str = "latest",
    ) -> SchemaRegistration | None:
        """Return descriptive metadata for a registered schema."""

        if entity not in cls._registry:
            return None

        resolved_version = schema_version
        if schema_version == "latest":
            versions = cls._registry[entity].keys()
            if not versions:
                return None
            resolved_version = max(versions, key=lambda v: version.parse(v))

        return cls._metadata.get((entity, resolved_version))

    @classmethod
    def find_registration(
        cls,
        schema: DataFrameModel,
    ) -> SchemaRegistration | None:
        """Return the registration metadata matching ``schema`` if available."""

        for (entity, schema_version), registration in cls._metadata.items():
            registered_schema = cls._registry.get(entity, {}).get(schema_version)
            if registered_schema is schema:
                return registration
        return None

    @classmethod
    def find_registration_by_schema_id(
        cls,
        schema_id: str,
        *,
        version: str | None = None,
    ) -> SchemaRegistration | None:
        """Return the registration matching ``schema_id`` and optional ``version``."""

        for (_entity, schema_version), registration in cls._metadata.items():
            if registration.schema_id != schema_id:
                continue
            if version is not None and schema_version != version:
                continue
            return registration

        if version is not None:
            return None

        for registration in cls._metadata.values():
            if registration.schema_id == schema_id:
                return registration
        return None

    @classmethod
    def validate_compatibility(cls, entity: str, old_version: str, new_version: str) -> bool:
        """
        Проверяет совместимость версий схем.

        Returns:
            True if compatible (minor or patch change), False if major change
        """
        old = version.parse(old_version)
        new = version.parse(new_version)

        if new.major > old.major:
            logger.error(
                "schema_major_version_change",
                entity=entity,
                old_version=old_version,
                new_version=new_version,
            )
            return False

        logger.info(
            "schema_compatible",
            entity=entity,
            old_version=old_version,
            new_version=new_version,
        )
        return True


# Global registry instance
schema_registry = SchemaRegistry()

