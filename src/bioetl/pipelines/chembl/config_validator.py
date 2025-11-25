from __future__ import annotations

"""Проверка конфигурации ChEMBL пайплайнов."""

from typing import Any, Mapping, Sequence

from bioetl.pipelines.chembl.common.descriptor import ConfigValidationError


class ChemblConfigValidator:
    """Валидатор пользовательской конфигурации для ChEMBL."""

    def __init__(self, *, entity_name: str, required_sort_fields: Sequence[str]) -> None:
        self.entity_name = entity_name
        self.required_sort_fields = required_sort_fields

    def validate(self, config: Mapping[str, Any]) -> None:
        batch_size = self._get_config_value(config, "sources.chembl.batch_size")
        if not isinstance(batch_size, int) or batch_size <= 0 or batch_size > 25:
            raise ConfigValidationError("sources.chembl.batch_size must be integer within (0,25]")

        max_url_length = self._get_config_value(config, "sources.chembl.max_url_length")
        if not isinstance(max_url_length, int) or max_url_length <= 0 or max_url_length > 2000:
            raise ConfigValidationError("sources.chembl.max_url_length must be integer within (0,2000]")

        namespace = self._get_config_value(config, "cache.namespace")
        if not isinstance(namespace, str) or not namespace.strip():
            raise ConfigValidationError("cache.namespace must be non-empty string")

        sort_by = self._get_config_value(config, "determinism.sort.by")
        if not isinstance(sort_by, list) or not all(isinstance(x, str) for x in sort_by):
            raise ConfigValidationError("determinism.sort.by must be a list of strings")
        missing = [field for field in self.required_sort_fields if field not in sort_by]
        if missing:
            raise ConfigValidationError(
                f"determinism.sort.by is missing required fields for {self.entity_name}: {missing}"
            )

    def _get_config_value(self, config: Mapping[str, Any], dotted_path: str) -> Any:
        current: Any = config
        for part in dotted_path.split("."):
            if not isinstance(current, Mapping) or part not in current:
                raise ConfigValidationError(f"Missing configuration key: {dotted_path}")
            current = current[part]
        return current


__all__ = ["ChemblConfigValidator"]
