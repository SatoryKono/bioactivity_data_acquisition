"""Schema Registry with versioning."""

import re
from typing import Any

import pandera as pa
from packaging import version

from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


class SchemaRegistry:
    """Реестр Pandera схем с версионированием."""

    _registry: dict[str, dict[str, pa.DataFrameModel]] = {}

    @classmethod
    def register(
        cls,
        entity: str,
        schema_version: str,
        schema: pa.DataFrameModel,
        column_order: list[str] | None = None,
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

        logger.info(
            "schema_registered",
            entity=entity,
            version=schema_version,
            columns=len(schema.__fields__),
        )

    @classmethod
    def get(cls, entity: str, schema_version: str = "latest") -> pa.DataFrameModel:
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

